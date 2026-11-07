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
import { Balancer } from "./balancer.ts";
import { NodeLink } from "./nodelink.ts";
import { Watcher } from "./watcher.ts";
import type { Link } from "./link.ts";
import type { NodeLocal } from "./nodelocal.ts";
import type { Provider } from "./provider.ts";
import type { Instance, Observable, WatchHandler } from "./registry.ts";

export type NodeHubEvents = {
  publish: (topic: string, data: unknown, origin: string) => void;
};

export type NodeHubConfig = {
  nodeId: string;
  callTimeout: number;
  secret?: string;
  local?: NodeLocal;
};

export class NodeHub implements Provider, Observable {
  readonly events = eventlite<NodeHubEvents>({
    allowDuplicate: true,
    isolate: true,
  });

  private readonly nodeId: string;
  private readonly callTimeout: number;
  private readonly secret?: string;
  private readonly local?: NodeLocal;
  private readonly nodeLinks: NodeLink[] = [];
  private readonly balancer = new Balancer();
  private readonly watcher = new Watcher();
  private closed = false;

  constructor(config: NodeHubConfig) {
    this.nodeId = config.nodeId;
    this.callTimeout = config.callTimeout;
    this.secret = config.secret;
    this.local = config.local;

    this.watcher.watch((event) => {
      if (event.type === "up") {
        this.register(event.instance);
      } else {
        this.unregister(event.instance);
      }
    });

    if (this.local !== undefined) {
      this.watcher.addSource(this.local);
    }
  }

  join(link: Link, secret?: string) {
    const nodeLink = new NodeLink(link, {
      provider: this,
      secret: secret !== undefined ? secret : this.secret,
    });

    this.nodeLinks.push(nodeLink);
    this.watcher.addSource(nodeLink);

    nodeLink.events.on("close", () => this.detach(nodeLink));
  }

  get links(): readonly NodeLink[] {
    return this.nodeLinks;
  }

  registrations() {
    return this.watcher.instances();
  }

  invoke(service: string, method: string, args: unknown[], timeout?: number) {
    if (timeout === undefined) {
      timeout = this.callTimeout;
    }

    if (this.local !== undefined && this.local.has(service)) {
      return this.local.invoke(service, method, args, timeout);
    }

    return this.balancer.invoke(service, method, args, timeout, this.nodeLinks);
  }

  publish(topic: string, data: unknown, origin: string, from?: string) {
    for (const nodeLink of this.nodeLinks) {
      if (from !== undefined && nodeLink.peerId === from) {
        continue;
      }

      nodeLink.publish(topic, data, origin);
    }

    if (from !== undefined) {
      this.events.emit("publish", topic, data, origin);
    }
  }

  watch(handler: WatchHandler): () => void;
  watch(service: string, handler: WatchHandler): () => void;
  watch(service: string | WatchHandler, handler?: WatchHandler) {
    if (typeof service === "function") {
      return this.watcher.watch(service);
    }

    if (handler === undefined) {
      throw new SwarmError("rejected", "watch handler is required");
    }

    return this.watcher.watch(service, handler);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    for (const nodeLink of this.nodeLinks.slice()) {
      nodeLink.close();
    }

    this.nodeLinks.length = 0;
    this.watcher.close();
  }

  private register(instance: Instance) {
    if (this.closed) {
      return;
    }

    for (const nodeLink of this.nodeLinks) {
      if (nodeLink.has(instance.service, instance.nodeId)) {
        continue;
      }

      nodeLink.register({
        service: instance.service,
        nodeId: this.nodeId,
        meta: instance.meta,
      });
    }
  }

  private unregister(instance: Instance) {
    if (
      this.closed ||
      this.nodeLinks.some((link) => link.has(instance.service))
    ) {
      return;
    }

    for (const nodeLink of this.nodeLinks) {
      nodeLink.unregister({ service: instance.service, nodeId: this.nodeId });
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
