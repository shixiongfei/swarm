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
import type { Server } from "./server.ts";
import type { Link } from "./link.ts";
import type { Methods, TopicHandler } from "./local.ts";
import type { Provider } from "./provider.ts";
import type { PeerEntry } from "./envelope.ts";
import type { Multicast, MulticastOptions } from "./multicast.ts";
import type { MetaData, WatchEvent, WatchHandler } from "./registry.ts";
import type { Server as HttpServer } from "node:http";

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
  seeds?: Array<string | URL>;
  multicast?: MulticastOptions;
};

export type WsListenOptions = {
  path?: string;
  server?: HttpServer;
};

export type ListenOptions = {
  backlog?: number;
  advertise?: string | URL;
  ws?: WsListenOptions;
};

const nodes = new Map<string, Node>();
const maxHops = 8;

export class Node implements Provider {
  readonly nodeId = randomId();
  readonly started = Date.now();
  readonly cluster: string;
  readonly secret?: string;

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
  private readonly seeds: Array<string | URL>;
  private readonly callTimeout: number;
  private readonly memberTtl: number;
  private readonly dialTtl: number;
  private readonly multicastConfig: MulticastOptions | undefined;
  private multicast: Multicast | undefined;
  private closed = false;

  constructor(config: NodeConfig) {
    this.cluster = config.cluster;
    this.secret = config.secret;

    this.watcher.addSource(this.nodeLocal);

    this.callTimeout =
      config.callTimeout !== undefined ? config.callTimeout : 5000;

    this.memberTtl = config.memberTtl !== undefined ? config.memberTtl : 60000;
    this.dialTtl =
      config.dialTtl !== undefined ? config.dialTtl : 24 * 60 * 60 * 1000;

    this.seeds = config.seeds !== undefined ? config.seeds : [];
    this.multicastConfig = config.multicast;

    this.nodeHub = new NodeHub({
      secret: this.secret,
      provider: this,
    });

    this.nodeHub.events.on("open", (nodeLink) =>
      nodeLink.peers(this.shared(nodeLink)),
    );
    this.nodeHub.events.on("peers", (nodeLink, members) =>
      this.learn(members, nodeLink),
    );

    this.watcher.addSource(this.nodeHub);
    this.watcher.watch((event) => this.relay(event));
  }

  registrations() {
    return this.watcher.instances();
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

      throw err;
    }

    if (advertised !== undefined) {
      if (advertised.port === 0) {
        advertised.port = server.port;
      }

      const scheme = advertised.secure ? "wss:" : advertised.protocol;
      const name = advertised.host.includes(":")
        ? `[${advertised.host}]`
        : advertised.host;
      const advertisedPath =
        advertised.path !== "/" ? advertised.path : listenPath;
      const suffix = advertisedPath === "/" ? "" : advertisedPath;

      this.members.push({
        id: this.nodeId,
        addr: `${scheme}//${name}:${advertised.port}${suffix}`,
      });
      this.broadcastPeers();
    }

    this.discover();

    await this.startMulticast();

    return { host: server.host, port: server.port };
  }

  async connect(url: string | URL, options?: ClientOptions) {
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

  call(
    service: string,
    method: string,
    args: unknown[] = [],
    options: { timeout?: number } = {},
  ) {
    this.checkOpen();

    const timeout =
      options.timeout === undefined ? this.callTimeout : options.timeout;

    try {
      return this.invoke(service, method, args, timeout, maxHops);
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
  ) {
    if (this.nodeLocal.has(service)) {
      try {
        return this.nodeLocal.invoke(service, method, args, timeout);
      } catch (err) {
        if (!(err instanceof SwarmError) || err.kind !== "not-found") {
          throw err;
        }
      }
    }

    const remaining = ttl === undefined ? maxHops : ttl;

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

    return this.balancer.invoke(
      service,
      method,
      args,
      timeout !== undefined ? timeout : this.callTimeout,
      remaining - 1,
      links,
      hubLinks,
    );
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

  publish(topic: string, data: unknown, origin?: string, from?: string) {
    this.checkOpen();

    const source = origin !== undefined ? origin : this.nodeId;

    try {
      for (const nodeLink of this.nodeLinks) {
        if (from !== undefined && nodeLink.peerId === from) {
          continue;
        }

        nodeLink.publish(topic, data, source);
      }

      for (const nodeLink of this.nodeHub.links) {
        if (from !== undefined && nodeLink.peerId === from) {
          continue;
        }

        nodeLink.publish(topic, data, source);
      }
    } catch (err) {
      if (err instanceof Error && err.name === "DataCloneError") {
        throw new SwarmError("failed", "data is not cloneable");
      }

      throw err;
    }

    this.nodeLocal.publish(topic, data, source);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    if (this.multicast !== undefined) {
      this.multicast.close();
    }

    for (const client of this.clients) {
      client.close();
    }

    this.clients.length = 0;

    for (const server of this.servers) {
      server.close();
    }

    this.servers.length = 0;

    for (const nodeLink of this.nodeLinks.slice()) {
      nodeLink.close();
    }

    this.nodeLinks.length = 0;

    this.nodeHub.close();
    this.nodeLocal.close();
    this.watcher.close();

    if (nodes.get(this.cluster) === this) {
      nodes.delete(this.cluster);
    }
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
    const calls: Array<Promise<ServiceResult>> = [];

    if (this.nodeLocal.has(service)) {
      calls.push(
        Promise.resolve()
          .then(() => this.nodeLocal.invoke(service, method, args, timeout))
          .then(
            (value) => ({ nodeId: this.nodeId, ok: true as const, value }),
            (error) => ({ nodeId: this.nodeId, ok: false as const, error }),
          ),
      );
    }

    const targets = new Map<string, NodeLink>();

    for (const nodeLink of [...this.nodeLinks, ...this.nodeHub.links]) {
      const peer = nodeLink.peerId;

      if (peer !== undefined && nodeLink.has(service) && !targets.has(peer)) {
        targets.set(peer, nodeLink);
      }
    }

    for (const [peer, nodeLink] of targets) {
      calls.push(
        nodeLink.call(peer, service, method, args, timeout, maxHops).then(
          (value) => ({ nodeId: peer, ok: true as const, value }),
          (error) => ({ nodeId: peer, ok: false as const, error }),
        ),
      );
    }

    if (calls.length === 0) {
      throw new SwarmError("not-found", `service ${service} is not available`);
    }

    return Promise.all(calls);
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

  private async createServer(
    url: string | URL,
    options: ListenOptions | undefined,
  ): Promise<Server> {
    const { protocol, secure, host, port, secret, path } = parseUrl(url);
    const ws = options !== undefined ? options.ws : undefined;
    const backlog = options !== undefined ? options.backlog : undefined;
    const server = ws !== undefined ? ws.server : undefined;
    const listenPath =
      ws !== undefined && ws.path !== undefined ? normalizePath(ws.path) : path;

    if (secure) {
      throw new SwarmError("rejected", "wss listen is not supported");
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

      return tcpServer;
    }

    const { WsServer } = await import("./wsserver.ts");
    const wsServer = new WsServer(host, port, {
      backlog,
      path: listenPath,
      server,
    });

    wsServer.events.on("online", (socket) => {
      this.nodeHub.join(
        new LinkWs(socket as unknown as WebSocket, this.nodeId),
        secret,
      );
    });

    return wsServer;
  }

  private async createClient(
    url: string | URL,
    options: ClientOptions | undefined,
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

      if (addr !== undefined) {
        this.expiries.delete(addr);
      }

      if (expected !== undefined && addr !== undefined) {
        this.seen.set(expected, Date.now());

        if (!this.members.some((member) => member.id === expected)) {
          this.members.push({ id: expected, addr });
          this.broadcastPeers();
          return;
        }
      }

      nodeLink.peers(this.shared(nodeLink));
    });
    nodeLink.events.on("peers", (members) => this.learn(members, nodeLink));
    nodeLink.events.on("close", () => this.detach(nodeLink));

    nodeLink.hello();
  }

  private detach(nodeLink: NodeLink) {
    const index = this.nodeLinks.indexOf(nodeLink);

    if (index >= 0) {
      this.nodeLinks.splice(index, 1);
    }

    this.watcher.removeSource(nodeLink);

    const peer = nodeLink.peerId;

    if (
      peer !== undefined &&
      this.members.some((member) => member.id === peer)
    ) {
      this.seen.set(peer, Date.now());
    }
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

      if (this.members.some((member) => member.id === entry.id)) {
        continue;
      }

      this.members.push(entry);
      this.expiries.delete(entry.addr);
      changed = true;

      const linked =
        this.nodeLinks.some((nodeLink) => nodeLink.peerId === entry.id) ||
        this.nodeHub.links.some((nodeLink) => nodeLink.peerId === entry.id);

      if ((!advertised || this.nodeId < entry.id) && !linked) {
        void this.dial(entry.addr, entry.id);
      }
    }

    if (this.prune(now)) {
      changed = true;
    }

    if (changed) {
      this.broadcastPeers();
    }
  }

  private prune(now: number) {
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

    return changed;
  }

  private forget(id: string, addr: string) {
    const index = this.members.findIndex((member) => member.id === id);

    if (index >= 0) {
      this.members.splice(index, 1);
      this.seen.delete(id);
      this.broadcastPeers();
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

    this.dials.set(addr, client);
    this.clients.push(client);

    client.events.on("retry", () => {
      queueMicrotask(() => {
        if (this.prune(Date.now())) {
          this.broadcastPeers();
        }
      });
    });

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

  const port =
    parsed.port === ""
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
    return existing;
  }

  const node = new Node({ ...options, cluster });
  nodes.set(cluster, node);

  return node;
}

export type { ClientOptions, MulticastOptions };
export type { Methods, TopicHandler };
export type { MetaData, WatchHandler };
export type { Instance, WatchEvent } from "./registry.ts";
