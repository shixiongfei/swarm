/*
 * nodehub.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { eventlite } from "eventlite.js";
import { LinkPort } from "./linkport.ts";
import { MusterError } from "./errors.ts";
import { NodeLink } from "./nodelink.ts";
import { Watcher } from "./watcher.ts";
import type { Provider } from "./provider.ts";
import type { Instance, Observable, WatchHandler } from "./registry.ts";

export type NodeHubEvents = {
  publish: (topic: string, data: unknown, from: string) => void;
};

export type NodeHubConfig = {
  nodeId: string;
  callTimeout: number;
  secret?: string;
};

export class NodeHub implements Provider, Observable {
  readonly events = eventlite<NodeHubEvents>({
    allowDuplicate: true,
    isolate: true,
  });

  private readonly nodeId: string;
  private readonly callTimeout: number;
  private readonly secret?: string;
  private readonly nodeLinks: NodeLink[] = [];
  private readonly watcher = new Watcher();
  private closed = false;

  constructor(config: NodeHubConfig) {
    this.nodeId = config.nodeId;
    this.callTimeout = config.callTimeout;
    this.secret = config.secret;

    this.watcher.watch((event) => {
      if (event.type === "up") {
        this.register(event.instance);
      } else {
        this.unregister(event.instance);
      }
    });
  }

  join(port: MessagePort, secret?: string) {
    const nodeLink = new NodeLink(new LinkPort(port, this.nodeId), {
      provider: this,
      secret: secret !== undefined ? secret : this.secret,
    });

    this.nodeLinks.push(nodeLink);
    this.watcher.addSource(nodeLink);

    nodeLink.events.on("close", () => this.detach(nodeLink));
  }

  registrations() {
    return this.watcher.instances();
  }

  invoke(service: string, method: string, args: unknown[], timeout?: number) {
    for (const link of this.nodeLinks) {
      const [instance] = link.lookup(service);

      if (instance === undefined) {
        continue;
      }

      return link.call(
        instance.nodeId,
        service,
        method,
        args,
        timeout !== undefined ? timeout : this.callTimeout,
      );
    }

    throw new MusterError("not-found", `service not found: ${service}`);
  }

  publish(topic: string, data: unknown, from?: string) {
    for (const nodeLink of this.nodeLinks) {
      if (from !== undefined && nodeLink.peerId === from) {
        continue;
      }

      nodeLink.publish(topic, data);
    }

    if (from !== undefined) {
      this.events.emit("publish", topic, data, from);
    }
  }

  call(service: string, method: string, args: unknown[], timeout: number) {
    for (const link of this.nodeLinks) {
      const [instance] = link.lookup(service);

      if (instance === undefined) {
        continue;
      }

      return link.call(instance.nodeId, service, method, args, timeout);
    }

    return undefined;
  }

  watch(handler: WatchHandler): () => void;
  watch(service: string, handler: WatchHandler): () => void;
  watch(service: string | WatchHandler, handler?: WatchHandler) {
    if (typeof service === "function") {
      return this.watcher.watch(service);
    }

    if (handler === undefined) {
      throw new MusterError("rejected", "watch handler is required");
    }

    return this.watcher.watch(service, handler);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    for (const link of this.nodeLinks.slice()) {
      link.close();
    }

    this.nodeLinks.length = 0;
    this.watcher.close();
  }

  private register(instance: Instance) {
    if (this.closed) {
      return;
    }

    for (const link of this.nodeLinks) {
      const known = link
        .lookup(instance.service)
        .some((item) => item.nodeId === instance.nodeId);

      if (known) {
        continue;
      }

      link.register({
        service: instance.service,
        nodeId: this.nodeId,
        meta: instance.meta,
      });
    }
  }

  private unregister(instance: Instance) {
    if (
      this.closed ||
      this.nodeLinks.some((link) => link.lookup(instance.service).length > 0)
    ) {
      return;
    }

    for (const link of this.nodeLinks) {
      link.unregister({ service: instance.service, nodeId: this.nodeId });
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
