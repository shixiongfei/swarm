/*
 * nodehub.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { eventlite } from "eventlite.js";
import { SwarmError } from "./errors.ts";
import { NodeLink } from "./nodelink.ts";
import { Watcher } from "./watcher.ts";
import type { Link } from "./link.ts";
import type { NodeLinkConfig } from "./nodelink.ts";
import type { Provider } from "./provider.ts";
import type { PeerEntry } from "./envelope.ts";
import type { Observable, WatchHandler } from "./registry.ts";

export type NodeHubEvents = {
  open: (nodeLink: NodeLink) => void;
  peers: (nodeLink: NodeLink, members: PeerEntry[]) => void;
  bye: (nodeLink: NodeLink) => void;
  drop: (error: SwarmError) => void;
};

export type NodeHubConfig = NodeLinkConfig;

export class NodeHub implements Observable {
  readonly events = eventlite<NodeHubEvents>({
    allowDuplicate: true,
    isolate: true,
  });

  private readonly secret: string;
  private readonly cluster: string;
  private readonly provider: Provider;
  private readonly startup: number;
  private readonly heartbeatInterval: number;
  private readonly heartbeatTimeout: number;
  private readonly handshakeTimeout?: number;
  private readonly maxInflight?: number;
  private readonly maxInboundTimeout?: number;
  private readonly nodeLinks: NodeLink[] = [];
  private readonly watcher = new Watcher();
  private closed = false;

  constructor(config: NodeHubConfig) {
    this.provider = config.provider;
    this.startup = config.startup;
    this.secret = config.secret;
    this.cluster = config.cluster;
    this.heartbeatInterval = config.heartbeatInterval;
    this.heartbeatTimeout = config.heartbeatTimeout;
    this.handshakeTimeout = config.handshakeTimeout;
    this.maxInflight = config.maxInflight;
    this.maxInboundTimeout = config.maxInboundTimeout;
  }

  join(link: Link, secret?: string) {
    const nodeLink = new NodeLink(link, {
      provider: this.provider,
      startup: this.startup,
      secret: secret !== undefined ? secret : this.secret,
      cluster: this.cluster,
      heartbeatInterval: this.heartbeatInterval,
      heartbeatTimeout: this.heartbeatTimeout,
      handshakeTimeout: this.handshakeTimeout,
      maxInflight: this.maxInflight,
      maxInboundTimeout: this.maxInboundTimeout,
    });

    this.nodeLinks.push(nodeLink);
    this.watcher.addSource(nodeLink);

    nodeLink.events.on("open", () => this.events.emit("open", nodeLink));
    nodeLink.events.on("peers", (members) =>
      this.events.emit("peers", nodeLink, members),
    );
    nodeLink.events.on("bye", () => this.events.emit("bye", nodeLink));
    nodeLink.events.on("drop", (error) => this.events.emit("drop", error));
    nodeLink.events.on("close", () => this.detach(nodeLink));
  }

  get links(): readonly NodeLink[] {
    return this.nodeLinks;
  }

  watch(handler: WatchHandler): () => void;
  watch(service: string, handler: WatchHandler): () => void;
  watch(service: string | WatchHandler, handler?: WatchHandler) {
    if (typeof service === "function") {
      return this.watcher.watch(service);
    }

    if (handler === undefined) {
      throw new SwarmError("rejected", "REJECTED", "watch handler is required");
    }

    return this.watcher.watch(service, handler);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    const links = [...this.nodeLinks];

    this.nodeLinks.length = 0;
    this.watcher.close();

    for (const link of links) {
      link.close();
    }
  }

  private detach(link: NodeLink) {
    const index = this.nodeLinks.indexOf(link);

    if (index < 0) {
      return;
    }

    this.nodeLinks.splice(index, 1);
    this.watcher.removeSource(link);
  }
}
