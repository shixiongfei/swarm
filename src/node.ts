/*
 * node.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { eventlite } from "eventlite.js";
import { randomId } from "./random.ts";
import { SwarmError } from "./errors.ts";
import { isAdvertisable, isLoopback } from "./address.ts";
import { Balancer } from "./balancer.ts";
import { NodeLocal } from "./nodelocal.ts";
import { LinkPort } from "./linkport.ts";
import { LinkTcp } from "./linktcp.ts";
import { LinkWs } from "./linkws.ts";
import { NodeLink } from "./nodelink.ts";
import { NodeHub } from "./nodehub.ts";
import { Watcher } from "./watcher.ts";
import { isMetaData } from "./registry.ts";
import type { Client, ClientOptions } from "./client.ts";
import type { WsClientOptions, WsTlsOptions } from "./wsclient.ts";
import type { Server } from "./server.ts";
import type { Link, Transport } from "./link.ts";
import type { Methods, TopicHandler } from "./local.ts";
import type { Provider } from "./provider.ts";
import type { PeerEntry } from "./envelope.ts";
import type { Multicast, MulticastOptions } from "./multicast.ts";
import type {
  Instance,
  MetaData,
  WatchEvent,
  WatchHandler,
} from "./registry.ts";
import type { Server as HttpServer } from "node:http";
import type { ServerOptions as HttpsServerOptions } from "node:https";

export type ServiceProxy<M extends Methods<M> = Methods> = {
  [K in keyof M]: (
    ...args: Parameters<M[K]>
  ) => Promise<Awaited<ReturnType<M[K]>>>;
};

export type ServiceResult =
  | { nodeId: string; ok: true; value: unknown }
  | { nodeId: string; ok: false; error: unknown };

export type NodeConfig = {
  cluster: string;
  secret?: string;
  callTimeout?: number;
  memberTtl?: number;
  dialTtl?: number;
  maxHops?: number;
  heartbeatInterval?: number;
  heartbeatTimeout?: number;
  handshakeTimeout?: number;
  maxInflight?: number;
  seeds?: Array<string | URL>;
  multicast?: MulticastOptions;
};

export type WsListenOptions = {
  path?: string;
  server?: HttpServer;
  tls?: HttpsServerOptions;
};

export type ListenOptions = {
  backlog?: number;
  advertise?: string | URL;
  ws?: WsListenOptions;
};

export type NodeMember = {
  id: string;
  addr: string;
};

export type NodeLinkInfo = {
  peerId: string | undefined;
  transport: Transport;
  latency: number | undefined;
  local: boolean;
  services: Instance[];
  setup: number | undefined;
  queue: number;
};

export type NodeHealth = "healthy" | "isolated" | "down";

export type NodeSnapshot = {
  nodeId: string;
  cluster: string;
  closed: boolean;
  services: Instance[];
  members: NodeMember[];
  links: NodeLinkInfo[];
  seeds: string[];
  multicast: boolean;
  health: NodeHealth;
};

export type NodeEvents = {
  "member-up": (member: NodeMember) => void;
  "member-down": (member: NodeMember) => void;
  "link-up": (link: NodeLinkInfo) => void;
  "link-down": (link: NodeLinkInfo) => void;
  error: (error: Error) => void;
};

const nodes = new Map<string, { node: Node; options: NodeOptions }>();
const defaultMaxHops = 8;
const maxSeenPubs = 1024;
const closeFlushTimeout = 1000;

export class Node implements Provider {
  readonly nodeId = randomId();
  readonly started = Date.now();
  readonly cluster: string;
  readonly secret?: string;
  readonly events = eventlite<NodeEvents>({
    allowDuplicate: true,
    isolate: true,
  });

  private readonly nodeLocal = new NodeLocal(this.nodeId);
  private readonly nodeLinks: NodeLink[] = [];
  private readonly nodeHub: NodeHub;
  private readonly balancer = new Balancer();
  private readonly watcher = new Watcher();
  private readonly servers: Server[] = [];
  private readonly clients: Client[] = [];
  private readonly members: PeerEntry[] = [];
  private readonly seen = new Map<string, number>();
  private readonly dials = new Map<string, Client | undefined>();
  private readonly expiries = new Map<string, number>();
  private readonly seenPubs = new Set<string>();
  private readonly callTimeout: number;
  private readonly memberTtl: number;
  private readonly dialTtl: number;
  private readonly maxHops: number;
  private readonly maxInflight: number | undefined;
  private readonly multicastConfig: MulticastOptions | undefined;
  private seeds: Array<string | URL>;
  private multicast: Multicast | undefined;
  private closed = false;
  private pruneTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(config: NodeConfig) {
    this.cluster = config.cluster;
    this.secret = config.secret;

    this.watcher.addSource(this.nodeLocal);

    this.callTimeout =
      config.callTimeout !== undefined ? config.callTimeout : 5000;

    this.memberTtl = config.memberTtl !== undefined ? config.memberTtl : 60000;
    this.dialTtl =
      config.dialTtl !== undefined ? config.dialTtl : 24 * 60 * 60 * 1000;
    this.maxHops =
      config.maxHops !== undefined ? config.maxHops : defaultMaxHops;
    this.maxInflight = config.maxInflight;

    this.seeds = config.seeds !== undefined ? config.seeds : [];
    this.multicastConfig = config.multicast;

    this.nodeHub = new NodeHub({
      secret: this.secret,
      provider: this,
      heartbeatInterval: config.heartbeatInterval,
      heartbeatTimeout: config.heartbeatTimeout,
      handshakeTimeout: config.handshakeTimeout,
      maxInflight: this.maxInflight,
    });

    this.nodeHub.events.on("open", (nodeLink) => {
      this.events.emit("link-up", this.linkInfo(nodeLink));
      nodeLink.events.once("close", () =>
        this.events.emit("link-down", this.linkInfo(nodeLink)),
      );
      nodeLink.peers(this.shared(nodeLink));
    });
    this.nodeHub.events.on("peers", (nodeLink, members) =>
      this.learn(members, nodeLink),
    );
    this.nodeHub.events.on("bye", (nodeLink) => this.forgetByLink(nodeLink));

    this.watcher.addSource(this.nodeHub);
    this.watcher.watch((event) => this.relay(event));
  }

  registrations() {
    return this.watcher.instances();
  }

  private linkInfo(nodeLink: NodeLink): NodeLinkInfo {
    return {
      peerId: nodeLink.peerId,
      transport: nodeLink.transport,
      latency: nodeLink.latency,
      local: nodeLink.local,
      services: nodeLink.instances(),
      setup: nodeLink.setup,
      queue: nodeLink.queue,
    };
  }

  snapshot(): NodeSnapshot {
    const links = [...this.nodeLinks, ...this.nodeHub.links].map((nodeLink) =>
      this.linkInfo(nodeLink),
    );

    const health: NodeHealth = this.closed
      ? "down"
      : links.length > 0
        ? "healthy"
        : "isolated";

    return {
      nodeId: this.nodeId,
      cluster: this.cluster,
      closed: this.closed,
      services: this.watcher.instances(),
      members: this.members.map((member) => ({
        id: member.id,
        addr: member.addr,
      })),
      links,
      seeds: this.seeds.map((seed) => String(seed)),
      multicast: this.multicast !== undefined && this.multicast.active,
      health,
    };
  }

  register(service: string, methods: Methods, meta?: MetaData) {
    this.checkOpen();

    let ownedMeta: MetaData | undefined;

    if (meta !== undefined) {
      try {
        ownedMeta = structuredClone(meta);
      } catch (err) {
        throw new SwarmError("rejected", "meta is not cloneable");
      }

      if (!isMetaData(ownedMeta)) {
        throw new SwarmError("rejected", "meta is not a plain object");
      }
    }

    this.nodeLocal.register(service, methods, ownedMeta);

    return this;
  }

  unregister(service: string) {
    this.checkOpen();
    this.nodeLocal.unregister(service);

    return this;
  }

  attach(port: MessagePort, secret?: string) {
    this.checkOpen();
    this.attachLink(new LinkPort(port, this.nodeId), secret);

    return this;
  }

  join(port: MessagePort, secret?: string) {
    this.checkOpen();
    this.nodeHub.join(new LinkPort(port, this.nodeId), secret);

    return this;
  }

  async listen(url: string | URL, options?: ListenOptions) {
    this.checkOpen();

    const { path } = parseUrl(url);
    const ws = options !== undefined ? options.ws : undefined;
    const listenPath =
      ws !== undefined && ws.path !== undefined ? normalizePath(ws.path) : path;

    const advertised =
      options !== undefined && options.advertise !== undefined
        ? parseUrl(options.advertise)
        : undefined;

    if (advertised !== undefined && !isAdvertisable(advertised.host)) {
      throw new SwarmError(
        "rejected",
        `invalid advertise host: ${advertised.host}`,
      );
    }

    const server = await this.createServer(url, options);

    this.servers.push(server);

    try {
      await server.listen();
    } catch (err) {
      const index = this.servers.indexOf(server);

      if (index >= 0) {
        this.servers.splice(index, 1);
      }

      server.close();

      throw err;
    }

    if (advertised !== undefined) {
      if (advertised.portOmitted || advertised.port === 0) {
        advertised.port = server.port;
      }

      const scheme = advertised.secure ? "wss:" : advertised.protocol;
      const name = advertised.host.includes(":")
        ? `[${advertised.host}]`
        : advertised.host;
      const advertisedPath =
        advertised.path !== "/" ? advertised.path : listenPath;
      const suffix = advertisedPath === "/" ? "" : advertisedPath;
      const self = {
        id: this.nodeId,
        addr: `${scheme}//${name}:${advertised.port}${suffix}`,
      };
      const index = this.members.findIndex(
        (member) => member.id === this.nodeId,
      );

      if (index >= 0) {
        this.members[index] = self;
      } else {
        this.members.push(self);
      }

      this.broadcastPeers();
    }

    this.discover();

    await this.startMulticast();

    return { host: server.host, port: server.port };
  }

  async connect(url: string | URL, options?: WsClientOptions) {
    this.checkOpen();

    const client = await this.createClient(url, options);

    this.clients.push(client);

    try {
      await client.connect();
    } catch (err) {
      if (options !== undefined && options.reconnect === false) {
        const index = this.clients.indexOf(client);

        if (index >= 0) {
          this.clients.splice(index, 1);
        }

        client.close();
      }

      throw err;
    }

    return this;
  }

  addSeed(seed: string | URL) {
    this.checkOpen();

    if (this.seeds.some((existing) => String(existing) === String(seed))) {
      return this;
    }

    this.seeds.push(seed);
    void this.dial(String(seed));

    return this;
  }

  removeSeed(seed: string | URL) {
    const target = String(seed);
    const index = this.seeds.findIndex(
      (existing) => String(existing) === target,
    );

    if (index >= 0) {
      this.seeds.splice(index, 1);
    }

    return this;
  }

  call(
    service: string,
    method: string,
    args: unknown[] = [],
    options: { timeout?: number; signal?: AbortSignal } = {},
  ) {
    const timeout =
      options.timeout === undefined ? this.callTimeout : options.timeout;

    try {
      this.checkOpen();

      return Promise.resolve(
        this.invoke(
          service,
          method,
          args,
          timeout,
          this.maxHops,
          undefined,
          undefined,
          options.signal,
        ),
      );
    } catch (err) {
      return Promise.reject(err);
    }
  }

  callTo(
    nodeId: string,
    service: string,
    method: string,
    args: unknown[] = [],
    options: { timeout?: number; signal?: AbortSignal } = {},
  ) {
    const timeout =
      options.timeout === undefined ? this.callTimeout : options.timeout;

    try {
      this.checkOpen();

      return Promise.resolve(
        this.invoke(
          service,
          method,
          args,
          timeout,
          this.maxHops,
          undefined,
          nodeId,
          options.signal,
        ),
      );
    } catch (err) {
      return Promise.reject(err);
    }
  }

  invoke(
    service: string,
    method: string,
    args: unknown[],
    timeout?: number,
    ttl?: number,
    source?: NodeLink,
    target?: string,
    signal?: AbortSignal,
  ) {
    if (target !== undefined && target === this.nodeId) {
      return this.withCancel(
        this.nodeLocal.invoke(service, method, args, timeout),
        signal,
      );
    }

    if (target === undefined && this.nodeLocal.has(service)) {
      try {
        return this.withCancel(
          this.nodeLocal.invoke(service, method, args, timeout),
          signal,
        );
      } catch (err) {
        if (!(err instanceof SwarmError) || err.kind !== "not-found") {
          throw err;
        }
      }
    }

    const remaining = ttl === undefined ? this.maxHops : ttl;

    if (remaining <= 0) {
      throw new SwarmError("not-found", `service not found: ${service}`);
    }

    const links =
      source === undefined
        ? this.nodeLinks
        : this.nodeLinks.filter((link) => link !== source);

    const hubLinks =
      source === undefined
        ? this.nodeHub.links
        : this.nodeHub.links.filter((link) => link !== source);

    const routed =
      target === undefined
        ? [links, hubLinks]
        : [
            links.filter((link) => link.has(service, target)),
            hubLinks.filter((link) => link.has(service, target)),
          ];

    return this.balancer.invoke(
      service,
      method,
      args,
      timeout !== undefined ? timeout : this.callTimeout,
      remaining - 1,
      target,
      signal,
      ...routed,
    );
  }

  private withCancel<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
    if (signal === undefined) {
      return promise;
    }

    if (signal.aborted) {
      return Promise.reject(new SwarmError("cancelled", "call was cancelled"));
    }

    return new Promise((resolve, reject) => {
      const onAbort = () =>
        reject(new SwarmError("cancelled", "call was cancelled"));

      signal.addEventListener("abort", onAbort, { once: true });

      promise.then(
        (value) => {
          signal.removeEventListener("abort", onAbort);
          resolve(value);
        },
        (error) => {
          signal.removeEventListener("abort", onAbort);
          reject(error);
        },
      );
    });
  }

  get<M extends Methods<M> = Methods>(service: string) {
    this.checkOpen();

    return new Proxy(
      {},
      {
        get: (_target, property) => {
          if (property === "then" || typeof property !== "string") {
            return undefined;
          }
          return (...args: unknown[]) => this.call(service, property, args);
        },
      },
    ) as ServiceProxy<M>;
  }

  watch(service: string, handler: WatchHandler) {
    this.checkOpen();
    return this.watcher.watch(service, handler);
  }

  subscribe(topic: string, handler: TopicHandler) {
    this.checkOpen();
    return this.nodeLocal.subscribe(topic, handler);
  }

  publish(
    topic: string,
    data: unknown,
    origin?: string,
    from?: string,
    id?: string,
    ttl?: number,
  ) {
    this.checkOpen();

    const source = origin !== undefined ? origin : this.nodeId;
    const pubId = id !== undefined ? id : randomId();
    const remaining = ttl !== undefined ? ttl : this.maxHops;

    if (this.seenPubs.has(pubId)) {
      return;
    }

    this.seenPubs.add(pubId);

    if (this.seenPubs.size > maxSeenPubs) {
      const oldest = this.seenPubs.values().next().value;

      if (oldest !== undefined) {
        this.seenPubs.delete(oldest);
      }
    }

    if (remaining > 0) {
      try {
        for (const nodeLink of this.nodeLinks) {
          if (from !== undefined && nodeLink.peerId === from) {
            continue;
          }

          nodeLink.publish(topic, data, source, pubId, remaining - 1);
        }

        for (const nodeLink of this.nodeHub.links) {
          if (from !== undefined && nodeLink.peerId === from) {
            continue;
          }

          nodeLink.publish(topic, data, source, pubId, remaining - 1);
        }
      } catch (err) {
        if (err instanceof Error && err.name === "DataCloneError") {
          throw new SwarmError("failed", "data is not cloneable");
        }

        throw err;
      }
    }

    this.nodeLocal.publish(topic, data, source);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.members.length = 0;

    if (this.pruneTimer !== undefined) {
      clearTimeout(this.pruneTimer);
      this.pruneTimer = undefined;
    }

    if (this.multicast !== undefined) {
      this.multicast.close();
    }

    const links = [...this.nodeLinks, ...this.nodeHub.links];

    for (const nodeLink of links) {
      nodeLink.bye();
    }

    this.nodeLinks.length = 0;
    this.nodeHub.close();

    this.dials.clear();
    this.expiries.clear();
    this.seenPubs.clear();

    const entry = nodes.get(this.cluster);

    if (entry !== undefined && entry.node === this) {
      nodes.delete(this.cluster);
    }

    this.nodeLocal.close();
    this.watcher.close();

    let disposed = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const dispose = () => {
      if (disposed) {
        return;
      }

      disposed = true;

      if (timer !== undefined) {
        clearTimeout(timer);
        timer = undefined;
      }

      for (const nodeLink of links) {
        nodeLink.close();
      }

      for (const client of this.clients) {
        client.close();
      }

      this.clients.length = 0;

      for (const server of this.servers) {
        server.close();
      }

      this.servers.length = 0;
    };

    if (links.length === 0) {
      dispose();
      return;
    }

    timer = setTimeout(dispose, closeFlushTimeout);

    Promise.all(links.map((nodeLink) => nodeLink.flush())).then(
      dispose,
      dispose,
    );
  }

  async collect(
    service: string,
    method: string,
    args: unknown[] = [],
    options: { timeout?: number } = {},
  ) {
    this.checkOpen();

    const timeout =
      options.timeout === undefined ? this.callTimeout : options.timeout;
    const instances = this.watcher
      .instances()
      .filter((instance) => instance.service === service);

    if (instances.length === 0) {
      throw new SwarmError("not-found", `service ${service} is not available`);
    }

    return Promise.all(
      instances.map((instance) => {
        const promise =
          instance.nodeId === this.nodeId
            ? Promise.resolve().then(() =>
                this.nodeLocal.invoke(service, method, args, timeout),
              )
            : Promise.resolve().then(() =>
                this.invoke(
                  service,
                  method,
                  args,
                  timeout,
                  this.maxHops,
                  undefined,
                  instance.nodeId,
                ),
              );

        return promise.then(
          (value) => ({ nodeId: instance.nodeId, ok: true as const, value }),
          (error) => ({ nodeId: instance.nodeId, ok: false as const, error }),
        );
      }),
    );
  }

  private relay(event: WatchEvent) {
    for (const nodeLink of this.nodeLinks) {
      if (nodeLink.has(event.instance.service, event.instance.nodeId)) {
        continue;
      }

      if (event.type === "up") {
        nodeLink.register(event.instance);
      } else {
        nodeLink.unregister(event.instance);
      }
    }

    for (const nodeLink of this.nodeHub.links) {
      if (nodeLink.has(event.instance.service, event.instance.nodeId)) {
        continue;
      }

      if (event.type === "up") {
        nodeLink.register(event.instance);
      } else {
        nodeLink.unregister(event.instance);
      }
    }
  }

  private checkOpen() {
    if (this.closed) {
      throw new SwarmError("rejected", "node is closed");
    }
  }

  private closeServer(server: Server) {
    const index = this.servers.indexOf(server);

    if (index >= 0) {
      this.servers.splice(index, 1);
    }

    server.close();
  }

  private async createServer(
    url: string | URL,
    options: ListenOptions | undefined,
  ): Promise<Server> {
    const { protocol, secure, host, port, secret, path } = parseUrl(url);
    const ws = options !== undefined ? options.ws : undefined;
    const backlog = options !== undefined ? options.backlog : undefined;
    const tls = ws !== undefined ? ws.tls : undefined;
    const server = ws !== undefined ? ws.server : undefined;
    const listenPath =
      ws !== undefined && ws.path !== undefined ? normalizePath(ws.path) : path;

    if (secure && server === undefined && tls === undefined) {
      throw new SwarmError("rejected", "wss listen requires tls options");
    }

    if (protocol === "tcp:") {
      if (ws !== undefined) {
        throw new SwarmError("rejected", "tcp listen does not support http");
      }

      const { TcpServer } = await import("./tcpserver.ts");
      const tcpServer = new TcpServer(host, port, backlog);

      tcpServer.events.on("online", (socket) => {
        this.nodeHub.join(new LinkTcp(socket, this.nodeId), secret);
      });
      tcpServer.events.on("error", (error) => {
        this.events.emit("error", error);
        this.closeServer(tcpServer);
      });

      return tcpServer;
    }

    const { WsServer } = await import("./wsserver.ts");
    const wsServer = new WsServer(host, port, {
      backlog,
      path: listenPath,
      server,
      tls,
    });

    wsServer.events.on("online", (socket, remoteAddress) => {
      this.nodeHub.join(
        new LinkWs(
          socket as unknown as WebSocket,
          this.nodeId,
          isLoopback(remoteAddress),
        ),
        secret,
      );
    });
    wsServer.events.on("error", (error) => {
      this.events.emit("error", error);
      this.closeServer(wsServer);
    });

    return wsServer;
  }

  private async createClient(
    url: string | URL,
    options: WsClientOptions | undefined,
    expected?: string,
  ): Promise<Client> {
    const { protocol, host, port, secret: urlSecret } = parseUrl(url);
    const secret =
      options !== undefined && options.secret !== undefined
        ? options.secret
        : urlSecret;

    if (protocol === "tcp:") {
      const { TcpClient } = await import("./tcpclient.ts");
      const client = new TcpClient(host, port, options);

      client.events.on("online", (socket) => {
        this.attachLink(
          new LinkTcp(socket, this.nodeId),
          secret,
          expected,
          String(url),
        );
      });

      return client;
    }

    const { WsClient } = await import("./wsclient.ts");
    const client = new WsClient(url, options);

    client.events.on("online", (socket) => {
      this.attachLink(
        new LinkWs(socket, this.nodeId, isLoopback(host)),
        secret,
        expected,
        String(url),
      );
    });

    return client;
  }

  private attachLink(
    link: Link,
    secret?: string,
    expected?: string,
    addr?: string,
  ) {
    const nodeLink = new NodeLink(link, {
      provider: this,
      secret: secret !== undefined ? secret : this.secret,
      maxInflight: this.maxInflight,
    });

    this.nodeLinks.push(nodeLink);
    this.watcher.addSource(nodeLink);

    nodeLink.events.on("open", (peer) => {
      if (expected !== undefined && peer !== expected) {
        nodeLink.close();

        if (addr !== undefined) {
          this.forget(expected, addr);
        }

        return;
      }

      if (expected !== undefined) {
        const duplicate =
          this.nodeLinks.some(
            (existing) => existing !== nodeLink && existing.peerId === peer,
          ) || this.nodeHub.links.some((existing) => existing.peerId === peer);

        if (duplicate) {
          nodeLink.close();
          return;
        }
      }

      this.events.emit("link-up", this.linkInfo(nodeLink));

      if (addr !== undefined) {
        this.expiries.delete(addr);
      }

      if (expected !== undefined && addr !== undefined) {
        this.seen.set(expected, Date.now());

        if (!this.members.some((member) => member.id === expected)) {
          const member = { id: expected, addr };
          this.members.push(member);
          this.broadcastPeers();
          this.events.emit("member-up", member);
          return;
        }
      }

      nodeLink.peers(this.shared(nodeLink));
    });
    nodeLink.events.on("peers", (members) => this.learn(members, nodeLink));
    nodeLink.events.on("bye", () => this.forgetByLink(nodeLink));
    nodeLink.events.on("close", () => this.detach(nodeLink));

    try {
      nodeLink.hello();
    } catch (err) {
      this.detach(nodeLink);
      nodeLink.close();
      throw err;
    }
  }

  leave(nodeId: string) {
    this.checkOpen();

    for (const member of this.members.filter(
      (member) => member.id === nodeId,
    )) {
      this.forget(member.id, member.addr);
    }

    for (const nodeLink of this.nodeLinks) {
      if (nodeLink.peerId === nodeId) {
        nodeLink.close();
      }
    }

    for (const nodeLink of this.nodeHub.links) {
      if (nodeLink.peerId === nodeId) {
        nodeLink.close();
      }
    }

    return this;
  }

  private forgetByLink(nodeLink: NodeLink) {
    const peer = nodeLink.peerId;

    if (peer === undefined) {
      return;
    }

    const member = this.members.find((member) => member.id === peer);

    if (member !== undefined) {
      this.forget(member.id, member.addr);
    }
  }

  private detach(nodeLink: NodeLink) {
    const index = this.nodeLinks.indexOf(nodeLink);

    if (index >= 0) {
      this.nodeLinks.splice(index, 1);
      this.events.emit("link-down", this.linkInfo(nodeLink));
    }

    this.watcher.removeSource(nodeLink);

    const peer = nodeLink.peerId;

    if (
      peer !== undefined &&
      this.members.some((member) => member.id === peer)
    ) {
      this.seen.set(peer, Date.now());
    }

    this.schedulePrune(Date.now(), this.linkedPeers());
  }

  private learn(entries: PeerEntry[], nodeLink?: NodeLink) {
    const now = Date.now();
    const local = nodeLink === undefined ? true : nodeLink.local;
    const advertised = this.members.some((member) => member.id === this.nodeId);
    let changed = false;

    for (const entry of entries) {
      if (entry.id === this.nodeId) {
        continue;
      }

      let host: string;

      try {
        host = parseUrl(entry.addr).host;
      } catch {
        continue;
      }

      if (!isAdvertisable(host) || (!local && isLoopback(host))) {
        continue;
      }

      this.seen.set(entry.id, now);
      this.expiries.delete(entry.addr);

      const existing = this.members.find((member) => member.id === entry.id);

      if (existing !== undefined) {
        if (existing.addr !== entry.addr) {
          const linked =
            this.nodeLinks.some((nodeLink) => nodeLink.peerId === entry.id) ||
            this.nodeHub.links.some((nodeLink) => nodeLink.peerId === entry.id);

          if (!linked && !this.dials.has(entry.addr)) {
            void this.dial(entry.addr, entry.id);
          }
        }

        continue;
      }

      this.members.push(entry);
      changed = true;
      this.events.emit("member-up", { id: entry.id, addr: entry.addr });

      const linked =
        this.nodeLinks.some((nodeLink) => nodeLink.peerId === entry.id) ||
        this.nodeHub.links.some((nodeLink) => nodeLink.peerId === entry.id);

      if ((!advertised || this.nodeId < entry.id) && !linked) {
        void this.dial(entry.addr, entry.id);
      }
    }

    this.schedulePrune(now, this.linkedPeers());

    if (changed) {
      this.broadcastPeers();
    }
  }

  private prune(now: number) {
    const linked = this.linkedPeers();

    let changed = false;

    for (let index = this.members.length - 1; index >= 0; index -= 1) {
      const member = this.members[index];

      if (member.id === this.nodeId || linked.has(member.id)) {
        continue;
      }

      const seen = this.seen.get(member.id);

      if (seen === undefined || seen + this.memberTtl > now) {
        continue;
      }

      this.members.splice(index, 1);
      this.seen.delete(member.id);
      this.events.emit("member-down", { id: member.id, addr: member.addr });

      if (this.dials.has(member.addr)) {
        this.expiries.set(member.addr, now + this.dialTtl);
      }

      changed = true;
    }

    for (const [addr, expires] of this.expiries) {
      if (expires <= now) {
        this.dropDial(addr);
      }
    }

    this.schedulePrune(now, linked);

    return changed;
  }

  private linkedPeers() {
    const linked = new Set<string>();

    for (const nodeLink of this.nodeLinks) {
      if (nodeLink.peerId !== undefined) {
        linked.add(nodeLink.peerId);
      }
    }

    for (const nodeLink of this.nodeHub.links) {
      if (nodeLink.peerId !== undefined) {
        linked.add(nodeLink.peerId);
      }
    }

    return linked;
  }

  private schedulePrune(now: number, linked: Set<string>) {
    if (this.closed) {
      return;
    }

    if (this.pruneTimer !== undefined) {
      clearTimeout(this.pruneTimer);
      this.pruneTimer = undefined;
    }

    let next: number | undefined;

    for (const member of this.members) {
      if (member.id === this.nodeId || linked.has(member.id)) {
        continue;
      }

      const seen = this.seen.get(member.id);

      if (seen === undefined) {
        continue;
      }

      const deadline = seen + this.memberTtl;

      if (next === undefined || deadline < next) {
        next = deadline;
      }
    }

    for (const deadline of this.expiries.values()) {
      if (next === undefined || deadline < next) {
        next = deadline;
      }
    }

    if (next === undefined) {
      return;
    }

    this.pruneTimer = setTimeout(
      () => {
        this.pruneTimer = undefined;

        if (this.prune(Date.now())) {
          this.broadcastPeers();
        }
      },
      Math.max(0, next - now),
    );
  }

  private forget(id: string, addr: string) {
    const index = this.members.findIndex((member) => member.id === id);

    if (index >= 0) {
      const [member] = this.members.splice(index, 1);
      this.seen.delete(id);
      this.broadcastPeers();
      this.events.emit("member-down", { id: member.id, addr: member.addr });
    }

    this.dropDial(addr);
  }

  private dropDial(addr: string) {
    const client = this.dials.get(addr);

    this.dials.delete(addr);
    this.expiries.delete(addr);

    if (client === undefined) {
      return;
    }

    const index = this.clients.indexOf(client);

    if (index >= 0) {
      this.clients.splice(index, 1);
    }

    client.close();
  }

  private shared(nodeLink: NodeLink) {
    if (nodeLink.local) {
      return this.members;
    }

    return this.members.filter(
      (member) => !isLoopback(parseUrl(member.addr).host),
    );
  }

  private broadcastPeers() {
    for (const nodeLink of this.nodeLinks) {
      if (nodeLink.peerId !== undefined) {
        nodeLink.peers(this.shared(nodeLink));
      }
    }

    for (const nodeLink of this.nodeHub.links) {
      if (nodeLink.peerId !== undefined) {
        nodeLink.peers(this.shared(nodeLink));
      }
    }
  }

  private discover() {
    for (const seed of this.seeds) {
      void this.dial(String(seed));
    }
  }

  private async startMulticast() {
    if (this.multicast !== undefined || this.multicastConfig === undefined) {
      return;
    }

    const options = this.multicastConfig;

    try {
      const { Multicast } = await import("./multicast.ts");

      if (this.multicast !== undefined) {
        return;
      }

      const multicast = new Multicast({
        cluster: this.cluster,
        nodeId: this.nodeId,
        secret: this.secret,
        started: this.started,
        family: options.family,
        group: options.group,
        port: options.port,
        interval: options.interval,
        interface: options.interface,
        ttl: options.ttl,
      });

      multicast.events.on("peers", (entries) => this.learn(entries));

      this.multicast = multicast;

      await multicast.open(() =>
        this.members.filter((member) => member.id === this.nodeId),
      );
    } catch {
      this.multicast = undefined;
      // multicast is best-effort; seeds and explicit links still work
    }
  }

  private async dial(addr: string, id?: string) {
    if (this.dials.has(addr)) {
      return;
    }

    this.dials.set(addr, undefined);

    let client: Client;

    try {
      client = await this.createClient(addr, undefined, id);
    } catch {
      this.dials.delete(addr);
      return;
    }

    if (this.closed || !this.dials.has(addr)) {
      client.close();
      return;
    }

    this.dials.set(addr, client);
    this.clients.push(client);

    try {
      await client.connect();
    } catch {
      // the client retries on its own
    }
  }
}

function parseUrl(url: string | URL): {
  protocol: "tcp:" | "ws:";
  secure: boolean;
  host: string;
  port: number;
  portOmitted: boolean;
  path: string;
  secret: string | undefined;
} {
  let parsed: URL;

  try {
    parsed = new URL(url);
  } catch {
    throw new SwarmError("rejected", "invalid url");
  }

  if (
    (parsed.protocol !== "tcp:" &&
      parsed.protocol !== "ws:" &&
      parsed.protocol !== "wss:") ||
    parsed.hostname === "" ||
    (parsed.protocol === "tcp:" && parsed.port === "")
  ) {
    throw new SwarmError(
      "rejected",
      `invalid url: ${parsed.protocol}//${parsed.host}`,
    );
  }

  const portOmitted = parsed.port === "";
  const port = portOmitted
    ? parsed.protocol === "wss:"
      ? 443
      : 80
    : Number(parsed.port);

  if (!Number.isInteger(port) || port > 65535) {
    throw new SwarmError("rejected", `invalid port: ${parsed.hostname}`);
  }

  const path = normalizePath(parsed.pathname);

  if (parsed.protocol === "tcp:" && path !== "/") {
    throw new SwarmError("rejected", "tcp url does not support a path");
  }

  let secret: string | undefined;

  if (parsed.password !== "") {
    try {
      secret = decodeURIComponent(parsed.password);
    } catch {
      throw new SwarmError("rejected", "invalid secret");
    }
  }

  return {
    host: parsed.hostname.replace(/^\[(.*)\]$/, "$1"),
    port,
    portOmitted,
    path,
    secret,
    protocol: parsed.protocol === "tcp:" ? "tcp:" : "ws:",
    secure: parsed.protocol === "wss:",
  };
}

function normalizePath(path: string | undefined) {
  if (path === undefined || path === "") {
    return "/";
  }

  const value = path.startsWith("/") ? path : `/${path}`;

  return value.length > 1 && value.endsWith("/") ? value.slice(0, -1) : value;
}

export type NodeOptions = Partial<NodeConfig>;

export function createNode(options: NodeOptions = {}) {
  const cluster = options.cluster === undefined ? "default" : options.cluster;
  const existing = nodes.get(cluster);

  if (existing !== undefined) {
    for (const key of Object.keys(options) as Array<keyof NodeOptions>) {
      const value = options[key];

      if (
        value !== undefined &&
        JSON.stringify(value) !== JSON.stringify(existing.options[key])
      ) {
        throw new SwarmError(
          "rejected",
          `cluster ${cluster} was created with a different ${key}`,
        );
      }
    }

    return existing.node;
  }

  const copied = { ...options, cluster };
  const node = new Node(copied);

  nodes.set(cluster, { node, options: copied });

  return node;
}

export type { ClientOptions, MulticastOptions, WsClientOptions, WsTlsOptions };
export type { Methods, TopicHandler };
export type { Instance, MetaData, WatchHandler, WatchEvent };
