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
import type { PeerEntry } from "./envelope.ts";
import type { MetaData, WatchHandler } from "./registry.ts";

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
  seeds?: Array<string | URL>;
};

export type ListenOptions = {
  backlog?: number;
  advertise?: string | URL;
};

const nodes = new Map<string, Node>();

export class Node {
  readonly nodeId = randomId();
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
  private readonly dials = new Set<string>();
  private readonly seeds: Array<string | URL>;
  private readonly callTimeout: number;
  private closed = false;

  constructor(config: NodeConfig) {
    this.cluster = config.cluster;
    this.secret = config.secret;

    this.watcher.addSource(this.nodeLocal);

    this.callTimeout =
      config.callTimeout !== undefined ? config.callTimeout : 5000;

    this.seeds = config.seeds !== undefined ? config.seeds : [];

    this.nodeHub = new NodeHub({
      nodeId: this.nodeId,
      callTimeout: this.callTimeout,
      secret: this.secret,
      local: this.nodeLocal,
    });

    this.nodeHub.events.on("publish", (topic, data, origin) => {
      this.nodeLocal.publish(topic, data, origin);
    });

    this.nodeHub.events.on("open", (nodeLink) => nodeLink.peers(this.members));
    this.nodeHub.events.on("peers", (members) => this.learn(members));

    this.watcher.addSource(this.nodeHub);
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

    const instance = this.nodeLocal.register(service, methods, ownedMeta);

    for (const nodeLink of this.nodeLinks) {
      nodeLink.register(instance);
    }

    return this;
  }

  unregister(service: string) {
    this.checkOpen();

    const instance = this.nodeLocal.unregister(service);

    if (instance !== undefined) {
      for (const nodeLink of this.nodeLinks) {
        nodeLink.unregister(instance);
      }
    }

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

    if (options !== undefined && options.advertise !== undefined) {
      const parsed = parseUrl(options.advertise);
      const scheme = parsed.secure ? "wss:" : parsed.protocol;
      const name = parsed.host.includes(":") ? `[${parsed.host}]` : parsed.host;

      this.members.push({
        id: this.nodeId,
        addr: `${scheme}//${name}:${parsed.port}`,
      });
      this.broadcastPeers();
    }

    this.discover();

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

    if (this.nodeLocal.has(service)) {
      try {
        return this.nodeLocal.invoke(service, method, args, timeout);
      } catch (err) {
        if (!(err instanceof SwarmError) || err.kind !== "not-found") {
          return Promise.reject(err);
        }
      }
    }

    try {
      return this.balancer.invoke(
        service,
        method,
        args,
        timeout,
        this.nodeLinks,
        this.nodeHub.links,
      );
    } catch (err) {
      return Promise.reject(err);
    }
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

  publish(topic: string, data: unknown) {
    this.checkOpen();

    try {
      for (const nodeLink of this.nodeLinks) {
        nodeLink.publish(topic, data, this.nodeId);
      }

      this.nodeHub.publish(topic, data, this.nodeId);
    } catch (err) {
      if (err instanceof Error && err.name === "DataCloneError") {
        throw new SwarmError("failed", "data is not cloneable");
      }

      throw err;
    }

    this.nodeLocal.publish(topic, data, this.nodeId);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

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
        nodeLink.call(peer, service, method, args, timeout).then(
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

  private checkOpen() {
    if (this.closed) {
      throw new SwarmError("rejected", "node is closed");
    }
  }

  private async createServer(
    url: string | URL,
    options: ListenOptions | undefined,
  ): Promise<Server> {
    const { protocol, secure, host, port, secret } = parseUrl(url);
    const backlog = options !== undefined ? options.backlog : undefined;

    if (secure) {
      throw new SwarmError("rejected", "wss listen is not supported");
    }

    if (protocol === "tcp:") {
      const { TcpServer } = await import("./tcpserver.ts");
      const server = new TcpServer(host, port, backlog);

      server.events.on("online", (socket) => {
        this.nodeHub.join(new LinkTcp(socket, this.nodeId), secret);
      });

      return server;
    }

    const { WsServer } = await import("./wsserver.ts");
    const server = new WsServer(host, port, backlog);

    server.events.on("online", (socket) => {
      this.nodeHub.join(
        new LinkWs(socket as unknown as WebSocket, this.nodeId),
        secret,
      );
    });

    return server;
  }

  private async createClient(
    url: string | URL,
    options: ClientOptions | undefined,
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
        this.attachLink(new LinkTcp(socket, this.nodeId), secret);
      });

      return client;
    }

    const { WsClient } = await import("./wsclient.ts");
    const client = new WsClient(url, options);

    client.events.on("online", (socket) => {
      this.attachLink(new LinkWs(socket, this.nodeId), secret);
    });

    return client;
  }

  private attachLink(link: Link, secret?: string) {
    const nodeLink = new NodeLink(link, {
      provider: this.nodeLocal,
      secret: secret !== undefined ? secret : this.secret,
    });

    this.nodeLinks.push(nodeLink);
    this.watcher.addSource(nodeLink);

    nodeLink.events.on("open", () => nodeLink.peers(this.members));
    nodeLink.events.on("peers", (members) => this.learn(members));
    nodeLink.events.on("close", () => this.detach(nodeLink));

    nodeLink.hello();
  }

  private detach(nodeLink: NodeLink) {
    const index = this.nodeLinks.indexOf(nodeLink);

    if (index >= 0) {
      this.nodeLinks.splice(index, 1);
    }

    this.watcher.removeSource(nodeLink);
  }

  private learn(entries: PeerEntry[]) {
    let changed = false;

    for (const entry of entries) {
      if (
        entry.id === this.nodeId ||
        this.members.some((member) => member.id === entry.id)
      ) {
        continue;
      }

      try {
        parseUrl(entry.addr);
      } catch {
        continue;
      }

      this.members.push(entry);
      changed = true;

      const linked =
        this.nodeLinks.some((nodeLink) => nodeLink.peerId === entry.id) ||
        this.nodeHub.links.some((nodeLink) => nodeLink.peerId === entry.id);

      if (this.nodeId < entry.id && !linked) {
        void this.dial(entry.addr);
      }
    }

    if (changed) {
      this.broadcastPeers();
    }
  }

  private broadcastPeers() {
    for (const nodeLink of this.nodeLinks) {
      if (nodeLink.peerId !== undefined) {
        nodeLink.peers(this.members);
      }
    }

    for (const nodeLink of this.nodeHub.links) {
      if (nodeLink.peerId !== undefined) {
        nodeLink.peers(this.members);
      }
    }
  }

  private discover() {
    for (const seed of this.seeds) {
      void this.dial(String(seed));
    }
  }

  private async dial(addr: string) {
    if (this.dials.has(addr)) {
      return;
    }

    this.dials.add(addr);

    try {
      await this.connect(addr);
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
    secret,
    protocol: parsed.protocol === "tcp:" ? "tcp:" : "ws:",
    secure: parsed.protocol === "wss:",
  };
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

export type { ClientOptions };
export type { Methods, TopicHandler };
export type { MetaData, WatchHandler };
export type { Instance, WatchEvent } from "./registry.ts";
