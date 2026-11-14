/*
 * discovery.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { LinkStream } from "./linkstream.ts";
import type { Link } from "./link.ts";
import type { PeerEntry } from "./envelope.ts";
import type { Multicast, MulticastOptions } from "./multicast.ts";
import type { UnixDiscovery } from "./unixdiscovery.ts";

export type DiscoveryOptions = {
  dir?: string;
  interval?: number;
};

export type DiscoveryHostOptions = {
  nodeId: string;
  cluster: string;
  secret: string;
  multicast?: MulticastOptions;
  discovery?: boolean | DiscoveryOptions;
  learn: (entries: PeerEntry[], local: boolean) => void;
  self: () => PeerEntry[];
  join: (link: Link) => void;
  attach: (link: Link, auto: boolean) => void;
  onError: (error: Error) => void;
  onReady: (directory: string) => void;
};

export class Discovery {
  private readonly nodeId: string;
  private readonly cluster: string;
  private readonly secret: string;
  private readonly multicastConfig: MulticastOptions | undefined;
  private readonly config: DiscoveryOptions | undefined;
  private readonly learn: DiscoveryHostOptions["learn"];
  private readonly self: DiscoveryHostOptions["self"];
  private readonly join: DiscoveryHostOptions["join"];
  private readonly attach: DiscoveryHostOptions["attach"];
  private readonly onError: DiscoveryHostOptions["onError"];
  private readonly onReady: DiscoveryHostOptions["onReady"];
  private multicast: Multicast | undefined;
  private unixDiscovery: UnixDiscovery | undefined;
  private closed = false;

  constructor(options: DiscoveryHostOptions) {
    this.nodeId = options.nodeId;
    this.cluster = options.cluster;
    this.secret = options.secret;
    this.multicastConfig = options.multicast;
    this.config =
      options.discovery === undefined || options.discovery === false
        ? undefined
        : options.discovery === true
          ? {}
          : options.discovery;
    this.learn = options.learn;
    this.self = options.self;
    this.join = options.join;
    this.attach = options.attach;
    this.onError = options.onError;
    this.onReady = options.onReady;
  }

  get multicastActive() {
    return this.multicast !== undefined && this.multicast.active;
  }

  get unixActive() {
    return this.unixDiscovery !== undefined && this.unixDiscovery.active;
  }

  close() {
    this.closed = true;

    if (this.multicast !== undefined) {
      this.multicast.close();
    }

    if (this.unixDiscovery !== undefined) {
      this.unixDiscovery.close();
      this.unixDiscovery = undefined;
    }
  }

  async startMulticast() {
    if (this.multicast !== undefined || this.multicastConfig === undefined) {
      return;
    }

    const options = this.multicastConfig;

    try {
      const { Multicast } = await import("./multicast.ts");

      if (this.multicast !== undefined || this.closed) {
        return;
      }

      const multicast = new Multicast({
        cluster: this.cluster,
        nodeId: this.nodeId,
        secret: this.secret,
        family: options.family,
        group: options.group,
        port: options.port,
        interval: options.interval,
        interface: options.interface,
        ttl: options.ttl,
      });

      multicast.events.on("peers", (entries) => this.learn(entries, true));

      this.multicast = multicast;

      await multicast.open(this.self);
    } catch {
      this.multicast = undefined;
      // multicast is best-effort; seeds and explicit links still work
    }
  }

  async startUnix() {
    const options = this.config;

    if (
      this.unixDiscovery !== undefined ||
      options === undefined ||
      this.closed
    ) {
      return;
    }

    let discovery: UnixDiscovery | undefined;

    try {
      const { UnixDiscovery } = await import("./unixdiscovery.ts");

      if (this.unixDiscovery !== undefined || this.closed) {
        return;
      }

      discovery = new UnixDiscovery({
        nodeId: this.nodeId,
        cluster: this.cluster,
        secret: this.secret,
        dir: options.dir,
        interval: options.interval,
      });

      discovery.events.on("accept", (socket) => {
        this.join(new LinkStream(socket, this.nodeId, true));
      });
      discovery.events.on("connect", (socket) => {
        this.attach(new LinkStream(socket, this.nodeId, true), true);
      });
      discovery.events.on("error", (error) => this.onError(error));

      this.unixDiscovery = discovery;

      await discovery.open();

      if (this.unixDiscovery === discovery && !this.closed) {
        this.onReady(discovery.dir);
      }
    } catch (err) {
      if (discovery !== undefined) {
        discovery.close();
      }

      if (this.unixDiscovery === discovery) {
        this.unixDiscovery = undefined;
      }

      if (!this.closed) {
        this.onError(
          err instanceof Error ? err : new Error("unix discovery failed"),
        );
      }
    }
  }
}
