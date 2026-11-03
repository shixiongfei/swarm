/*
 * node.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { randomId } from "./random.ts";
import { MusterError } from "./errors.ts";
import { NodeLocal } from "./nodelocal.ts";
import { LinkPort } from "./linkport.ts";
import { NodeLink } from "./nodelink.ts";
import { Watcher } from "./watcher.ts";
import { isMetaData } from "./registry.ts";
import type { Methods, TopicHandler } from "./local.ts";
import type { MetaData, WatchHandler } from "./registry.ts";

export type ServiceProxy<M extends Methods<M> = Methods> = {
  [K in keyof M]: (
    ...args: Parameters<M[K]>
  ) => Promise<Awaited<ReturnType<M[K]>>>;
};

export type NodeConfig = {
  cluster: string;
  secret?: string;
  callTimeout?: number;
};

export type NodeOptions = Partial<NodeConfig>;

const nodes = new Map<string, Node>();

export class Node {
  readonly nodeId = randomId();
  readonly cluster: string;
  readonly secret?: string;

  private readonly nodeLocal = new NodeLocal(this.nodeId);
  private readonly nodeLinks: NodeLink[] = [];
  private readonly watcher = new Watcher();
  private readonly callTimeout: number;
  private closed = false;

  constructor(config: NodeConfig) {
    this.cluster = config.cluster;
    this.secret = config.secret;

    this.watcher.addSource(this.nodeLocal);

    this.callTimeout =
      config.callTimeout !== undefined ? config.callTimeout : 5000;
  }

  register(service: string, methods: Methods, meta?: MetaData) {
    this.checkOpen();

    let ownedMeta: MetaData | undefined;

    if (meta !== undefined) {
      try {
        ownedMeta = structuredClone(meta);
      } catch (err) {
        throw new MusterError("rejected", "meta is not cloneable");
      }

      if (!isMetaData(ownedMeta)) {
        throw new MusterError("rejected", "meta is not a plain object");
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

    const nodeLink = new NodeLink(new LinkPort(port, this.nodeId), {
      provider: this.nodeLocal,
      secret: secret !== undefined ? secret : this.secret,
    });

    this.nodeLinks.push(nodeLink);
    this.watcher.addSource(nodeLink);

    nodeLink.events.on("close", () => this.detach(nodeLink));
    nodeLink.hello();

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

    return new Promise<unknown>((resolve, reject) => {
      if (this.nodeLocal.has(service)) {
        try {
          resolve(this.nodeLocal.invoke(service, method, args, timeout));
        } catch (err) {
          reject(err);
        }
        return;
      }

      for (const nodeLink of this.nodeLinks) {
        const [instance] = nodeLink.lookup(service);

        if (instance === undefined) {
          continue;
        }

        nodeLink
          .call(instance.nodeId, service, method, args, timeout)
          .then(resolve, reject);

        return;
      }

      reject(new MusterError("not-found", `service not found: ${service}`));
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

  publish(topic: string, data: unknown) {
    this.checkOpen();

    try {
      structuredClone(data);
    } catch (err) {
      throw new MusterError("failed", "data is not cloneable");
    }

    for (const nodeLink of this.nodeLinks) {
      nodeLink.publish(topic, data);
    }

    this.nodeLocal.publish(topic, data, this.nodeId);
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

    this.nodeLocal.close();
    this.watcher.close();

    if (nodes.get(this.cluster) === this) {
      nodes.delete(this.cluster);
    }
  }

  private checkOpen() {
    if (this.closed) {
      throw new MusterError("rejected", "node is closed");
    }
  }

  private detach(nodeLink: NodeLink) {
    const index = this.nodeLinks.indexOf(nodeLink);

    if (index >= 0) {
      this.nodeLinks.splice(index, 1);
    }

    this.watcher.removeSource(nodeLink);
  }
}

export function createNode(options: NodeOptions = {}) {
  const cluster = options.cluster === undefined ? "default" : options.cluster;
  const existing = nodes.get(cluster);

  if (existing !== undefined) {
    return existing;
  }

  const node = new Node({
    cluster,
    secret: options.secret,
    callTimeout: options.callTimeout,
  });
  nodes.set(cluster, node);
  return node;
}

export type { Methods, TopicHandler } from "./local.ts";

export type {
  Instance,
  MetaData,
  WatchEvent,
  WatchHandler,
} from "./registry.ts";
