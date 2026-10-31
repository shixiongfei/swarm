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
import { Service } from "./service.ts";
import type { Channel } from "./link.ts";
import type { Methods } from "./local.ts";
import type { WatchHandler } from "./registry.ts";

export type TopicHandler = (data: unknown, from: string) => void;

export type ServiceProxy<M extends Methods<M> = Methods> = {
  [K in keyof M]: (
    ...args: Parameters<M[K]>
  ) => Promise<Awaited<ReturnType<M[K]>>>;
};

export type NodeConfig = {
  cluster: string;
  callTimeout?: number;
};

export type NodeOptions = Partial<NodeConfig>;

const nodes = new Map<string, Node>();

export class Node {
  readonly nodeId = randomId();
  readonly cluster: string;

  private readonly service: Service;
  private readonly callTimeout: number;
  private closed = false;

  constructor(config: NodeConfig) {
    this.cluster = config.cluster;
    this.service = new Service({ nodeId: this.nodeId });
    this.callTimeout =
      config.callTimeout === undefined ? 5000 : config.callTimeout;
  }

  register(service: string, methods: Methods, meta?: Record<string, unknown>) {
    this.checkOpen();
    this.service.register(service, methods, meta);
    return this;
  }

  unregister(service: string) {
    this.checkOpen();
    this.service.unregister(service);
    return this;
  }

  attach(target: Channel) {
    this.checkOpen();
    return this.service.attach(target);
  }

  async call(
    service: string,
    method: string,
    args: unknown[] = [],
    options: { timeout?: number } = {},
  ) {
    this.checkOpen();

    const timeout =
      options.timeout === undefined ? this.callTimeout : options.timeout;

    return this.service.invoke(service, method, args, timeout);
  }

  get<M extends Methods<M> = Methods>(service: string) {
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
    return this.service.watch(service, handler);
  }

  subscribe(topic: string, handler: TopicHandler) {
    this.checkOpen();
    return this.service.subscribe(topic, handler);
  }

  publish(topic: string, data: unknown) {
    this.checkOpen();
    this.service.publish(topic, data);
  }

  instances(service: string) {
    return this.service.lookup(service);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.service.close();

    if (nodes.get(this.cluster) === this) {
      nodes.delete(this.cluster);
    }
  }

  private checkOpen() {
    if (this.closed) {
      throw new MusterError("rejected", "node is closed");
    }
  }
}

export function createNode(options: NodeOptions = {}) {
  const cluster = options.cluster === undefined ? "default" : options.cluster;
  const existing = nodes.get(cluster);

  if (existing !== undefined) {
    return existing;
  }

  const node = new Node({ cluster, callTimeout: options.callTimeout });
  nodes.set(cluster, node);
  return node;
}

export type { Methods } from "./local.ts";
