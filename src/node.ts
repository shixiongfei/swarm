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
import { isAdvertisable, normalizePath, parseUrl } from "./address.ts";
import { NodeLocal } from "./nodelocal.ts";
import { LinkPort } from "./linkport.ts";
import { NodeLink } from "./nodelink.ts";
import { NodeHub } from "./nodehub.ts";
import { Dialer } from "./dialer.ts";
import { Discovery } from "./discovery.ts";
import { Membership } from "./membership.ts";
import { PubSub } from "./pubsub.ts";
import { Router } from "./router.ts";
import { Transport } from "./transport.ts";
import { Watcher } from "./watcher.ts";
import { isMetaData } from "./registry.ts";
import type { LinkCloseReason } from "./nodelink.ts";
import type { NodeMember } from "./membership.ts";
import type { ListenOptions } from "./transport.ts";
import type { WsClientOptions } from "./wsclient.ts";
import type { Link, LinkTransport } from "./link.ts";
import type { Methods, TopicHandler } from "./local.ts";
import type { Provider } from "./provider.ts";
import type { MulticastOptions } from "./multicast.ts";
import type { DiscoveryOptions } from "./discovery.ts";
import type {
  Instance,
  MetaData,
  WatchEvent,
  WatchHandler,
} from "./registry.ts";

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
  discovery?: boolean | DiscoveryOptions;
};

export type NodeLinkInfo = {
  peerId: string | undefined;
  transport: LinkTransport;
  latency: number | undefined;
  local: boolean;
  services: Instance[];
};

export type NodeSnapshot = {
  nodeId: string;
  startup: number;
  cluster: string;
  services: Instance[];
  members: NodeMember[];
  links: NodeLinkInfo[];
  seeds: string[];
  multicast: boolean;
  unix: boolean;
};

export type NodeLinkDown = NodeLinkInfo & {
  reason: LinkCloseReason;
};

export type NodeEvents = {
  "member-up": (member: NodeMember) => void;
  "member-down": (member: NodeMember) => void;
  "link-up": (link: NodeLinkInfo) => void;
  "link-down": (link: NodeLinkDown) => void;
  discovery: (directory: string) => void;
  error: (error: Error) => void;
};

const nodes = new Map<string, Node>();
const defaultMaxHops = 8;

export class Node implements Provider {
  readonly nodeId = randomId();
  readonly startup = Date.now();
  readonly cluster: string;
  readonly secret: string;
  readonly events = eventlite<NodeEvents>({
    allowDuplicate: true,
    isolate: true,
  });

  private readonly nodeLocal = new NodeLocal(this.nodeId);
  private readonly nodeLinks: NodeLink[] = [];
  private readonly nodeHub: NodeHub;
  private readonly watcher = new Watcher();
  private readonly transport: Transport;
  private readonly membership: Membership;
  private readonly dialer: Dialer;
  private readonly pubsub: PubSub;
  private readonly router: Router;
  private readonly discovery: Discovery;
  private readonly dialed = new Map<NodeLink, string>();
  private readonly adopted = new Set<NodeLink>();
  private readonly callTimeout: number;
  private readonly maxHops: number;
  private readonly maxInflight: number | undefined;
  private closed = false;

  constructor(config: NodeConfig) {
    this.cluster = config.cluster;
    this.secret = config.secret !== undefined ? config.secret : "";

    this.watcher.addSource(this.nodeLocal);

    this.callTimeout =
      config.callTimeout !== undefined ? config.callTimeout : 5000;

    const memberTtl = config.memberTtl !== undefined ? config.memberTtl : 60000;
    const dialTtl =
      config.dialTtl !== undefined ? config.dialTtl : 24 * 60 * 60 * 1000;

    this.maxHops =
      config.maxHops !== undefined ? config.maxHops : defaultMaxHops;
    this.maxInflight = config.maxInflight;

    this.transport = new Transport({
      nodeId: this.nodeId,
      join: (nodeLink, secret) => this.nodeHub.join(nodeLink, secret),
      attach: (nodeLink, secret, expected, addr, auto) =>
        this.attachLink(nodeLink, secret, expected, addr, auto),
      onError: (error) => this.events.emit("error", error),
    });

    this.dialer = new Dialer({
      seeds: config.seeds !== undefined ? config.seeds : [],
      create: (addr, id) => this.transport.createClient(addr, undefined, id),
      track: (client) => this.transport.track(client),
      untrack: (client) => this.transport.untrack(client),
      isClosed: () => this.closed,
    });

    this.membership = new Membership({
      id: this.nodeId,
      memberTtl,
      dialTtl,
      linked: () => this.linkedPeers(),
      dialing: (addr) => this.dialer.has(addr),
      dial: (addr, id) => void this.dialer.dial(addr, id),
      drop: (addr) => this.dialer.drop(addr),
      broadcast: () => this.broadcastPeers(),
      onUp: (member) => this.events.emit("member-up", member),
      onDown: (member) => this.events.emit("member-down", member),
    });

    this.pubsub = new PubSub({
      nodeId: this.nodeId,
      maxHops: this.maxHops,
      links: () => [...this.nodeLinks, ...this.nodeHub.links],
      local: (topic, data, origin) =>
        this.nodeLocal.publish(topic, data, origin),
    });

    this.router = new Router({
      nodeId: this.nodeId,
      local: this.nodeLocal,
      maxHops: this.maxHops,
      callTimeout: this.callTimeout,
      links: () => this.nodeLinks,
      hubLinks: () => this.nodeHub.links,
      instances: () => this.watcher.instances(),
    });

    this.discovery = new Discovery({
      nodeId: this.nodeId,
      cluster: this.cluster,
      secret: this.secret,
      multicast: config.multicast,
      discovery: config.discovery,
      learn: (entries, local) => this.membership.learn(entries, local),
      self: () => this.membership.self(),
      join: (link) => this.nodeHub.join(link, this.secret),
      attach: (link, auto) =>
        this.attachLink(link, this.secret, undefined, undefined, auto),
      onError: (error) => this.events.emit("error", error),
      onReady: (directory) => this.events.emit("discovery", directory),
    });

    this.nodeHub = new NodeHub({
      secret: this.secret,
      provider: this,
      startup: this.startup,
      heartbeatInterval: config.heartbeatInterval,
      heartbeatTimeout: config.heartbeatTimeout,
      handshakeTimeout: config.handshakeTimeout,
      maxInflight: this.maxInflight,
    });

    this.nodeHub.events.on("open", (nodeLink) => {
      const peer = nodeLink.peerId;

      if (peer !== undefined && this.nodeId > peer) {
        for (const outbound of this.nodeLinks.filter(
          (existing) => existing.peerId === peer,
        )) {
          this.dropDialed(outbound);
        }
      }

      this.events.emit("link-up", this.linkInfo(nodeLink));
      nodeLink.events.once("close", () =>
        this.events.emit("link-down", this.linkDownInfo(nodeLink)),
      );
      nodeLink.peers(this.membership.shared(nodeLink.local));
    });
    this.nodeHub.events.on("peers", (nodeLink, members) =>
      this.membership.learn(members, nodeLink.local),
    );
    this.nodeHub.events.on("bye", (nodeLink) =>
      this.membership.forgetByLink(nodeLink.peerId),
    );

    this.watcher.addSource(this.nodeHub);
    this.watcher.watch((event) => this.relay(event));

    void this.discovery.startUnix();
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
    };
  }

  private linkDownInfo(nodeLink: NodeLink): NodeLinkDown {
    return Object.assign(this.linkInfo(nodeLink), {
      reason: nodeLink.closeReason,
    });
  }

  snapshot(): NodeSnapshot {
    return {
      nodeId: this.nodeId,
      startup: this.startup,
      cluster: this.cluster,
      services: this.watcher.instances(),
      members: this.membership.snapshot(),
      links: [...this.nodeLinks, ...this.nodeHub.links].map((nodeLink) =>
        this.linkInfo(nodeLink),
      ),
      seeds: this.dialer.list(),
      multicast: this.discovery.multicastActive,
      unix: this.discovery.unixActive,
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

    const server = await this.transport.open(url, options);

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
        startup: this.startup,
      };
      this.membership.upsertSelf(self);
    }

    this.dialer.discover();

    await this.discovery.startMulticast();

    return { host: server.host, port: server.port };
  }

  async connect(url: string | URL, options?: WsClientOptions) {
    this.checkOpen();
    await this.transport.connect(url, options);

    return this;
  }

  addSeed(seed: string | URL) {
    this.checkOpen();
    this.dialer.add(seed);

    return this;
  }

  removeSeed(seed: string | URL) {
    this.dialer.remove(seed);

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
    return this.router.invoke(
      service,
      method,
      args,
      timeout,
      ttl,
      source,
      target,
      signal,
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

  publish(
    topic: string,
    data: unknown,
    origin?: string,
    from?: string,
    id?: string,
    ttl?: number,
  ) {
    this.checkOpen();
    this.pubsub.publish(topic, data, origin, from, id, ttl);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.membership.close();
    this.discovery.close();

    const links = [...this.nodeLinks, ...this.nodeHub.links];

    for (const nodeLink of links) {
      nodeLink.bye();
    }

    this.nodeLinks.length = 0;
    this.nodeHub.close();

    this.pubsub.close();

    if (nodes.get(this.cluster) === this) {
      nodes.delete(this.cluster);
    }

    this.nodeLocal.close();
    this.watcher.close();

    for (const nodeLink of links) {
      nodeLink.close();
    }

    this.dialer.close();
    this.transport.dispose();
    this.dialed.clear();
    this.adopted.clear();
  }

  async callAll(
    service: string,
    method: string,
    args: unknown[] = [],
    options: { timeout?: number } = {},
  ) {
    this.checkOpen();

    return this.router.callAll(service, method, args, options);
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

  private attachLink(
    link: Link,
    secret?: string,
    expected?: string,
    addr?: string,
    auto?: boolean,
  ) {
    const nodeLink = new NodeLink(link, {
      provider: this,
      startup: this.startup,
      secret: secret !== undefined ? secret : this.secret,
      maxInflight: this.maxInflight,
    });

    if (addr !== undefined) {
      this.dialed.set(nodeLink, addr);
    }

    this.nodeLinks.push(nodeLink);
    this.watcher.addSource(nodeLink);

    nodeLink.events.on("open", (peer) => {
      if (expected !== undefined && peer !== expected) {
        nodeLink.close("error");

        if (addr !== undefined) {
          this.membership.forget(expected, addr);
        }

        return;
      }

      if (this.closed) {
        nodeLink.close();
        return;
      }

      if (!this.keepOutbound(nodeLink, peer, addr, auto)) {
        return;
      }

      this.adopted.add(nodeLink);
      this.events.emit("link-up", this.linkInfo(nodeLink));

      if (this.membership.attach(expected, addr, nodeLink.peerStartup)) {
        return;
      }

      nodeLink.peers(this.membership.shared(nodeLink.local));
    });
    nodeLink.events.on("peers", (members) =>
      this.membership.learn(members, nodeLink.local),
    );
    nodeLink.events.on("bye", () =>
      this.membership.forgetByLink(nodeLink.peerId),
    );
    nodeLink.events.on("close", () => this.detach(nodeLink));

    try {
      nodeLink.hello();
    } catch (err) {
      this.detach(nodeLink);
      nodeLink.close();
      throw err;
    }
  }

  private dropDialed(nodeLink: NodeLink) {
    const addr = this.dialed.get(nodeLink);

    if (addr === undefined) {
      return;
    }

    nodeLink.close("dedupe");
    this.dialer.drop(addr);
  }

  private dropOutbound(nodeLink: NodeLink) {
    const addr = this.dialed.get(nodeLink);

    nodeLink.close("dedupe");

    if (addr !== undefined) {
      this.dialer.drop(addr);
    }
  }

  private keepOutbound(
    nodeLink: NodeLink,
    peer: string,
    addr?: string,
    auto?: boolean,
  ) {
    if (addr === undefined && auto !== true) {
      return true;
    }

    const outbound = this.nodeLinks.find(
      (existing) => existing !== nodeLink && existing.peerId === peer,
    );

    if (outbound === undefined) {
      const inbound = this.nodeHub.links.some(
        (existing) => existing.peerId === peer,
      );

      if (addr === undefined || this.nodeId <= peer || !inbound) {
        return true;
      }

      this.dropOutbound(nodeLink);

      return false;
    }

    if (auto === true) {
      this.dropDialed(outbound);

      return true;
    }

    this.dropOutbound(nodeLink);

    return false;
  }

  leave(nodeId: string) {
    this.checkOpen();

    this.membership.leave(nodeId);

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

  private detach(nodeLink: NodeLink) {
    this.dialed.delete(nodeLink);

    const adopted = this.adopted.delete(nodeLink);
    const index = this.nodeLinks.indexOf(nodeLink);

    if (index >= 0) {
      this.nodeLinks.splice(index, 1);

      if (adopted) {
        this.events.emit("link-down", this.linkDownInfo(nodeLink));
      }
    }

    this.watcher.removeSource(nodeLink);
    this.membership.linkDown(nodeLink.peerId);
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

  private broadcastPeers() {
    for (const nodeLink of this.nodeLinks) {
      if (nodeLink.peerId !== undefined) {
        nodeLink.peers(this.membership.shared(nodeLink.local));
      }
    }

    for (const nodeLink of this.nodeHub.links) {
      if (nodeLink.peerId !== undefined) {
        nodeLink.peers(this.membership.shared(nodeLink.local));
      }
    }
  }
}

export type NodeOptions = Partial<NodeConfig>;

export function createNode(options: NodeOptions = {}) {
  const cluster = options.cluster === undefined ? "default" : options.cluster;
  const existing = nodes.get(cluster);

  if (existing !== undefined) {
    return existing;
  }

  const node = new Node(Object.assign({}, options, { cluster }));

  nodes.set(cluster, node);

  return node;
}
