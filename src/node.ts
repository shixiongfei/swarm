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

import { eventlite } from "eventlite.js";
import { randomId } from "./random.ts";
import { MusterError } from "./errors.ts";
import { Registry } from "./registry.ts";
import { Link } from "./link.ts";
import type { Channel } from "./link.ts";
import type { WatchHandler } from "./registry.ts";

export type TopicHandler = (data: unknown, from: string) => void;

export type Methods<M = Record<string, any>> = Record<
  keyof M,
  (...args: any[]) => any
>;

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

const hasOwn =
  Object.hasOwn ||
  ((obj: object, key: PropertyKey) =>
    Object.prototype.hasOwnProperty.call(obj, key));

const nodes = new Map<string, Node>();

function withTimeout<T>(work: Promise<T>, timeout: number, label: string) {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new MusterError("timeout", `call timed out: ${label}`));
    }, timeout);

    work.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err: unknown) => {
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}

export class Node {
  readonly nodeId = randomId();
  readonly cluster: string;

  private readonly registry = new Registry();
  private readonly services = new Map<string, Methods>();
  private readonly topics = eventlite<string>({
    allowDuplicate: true,
    isolate: true,
  });
  private readonly links = new Set<Link>();
  private readonly callTimeout: number;
  private closed = false;

  constructor(config: NodeConfig) {
    this.cluster = config.cluster;
    this.callTimeout =
      config.callTimeout === undefined ? 5000 : config.callTimeout;
  }

  register(service: string, methods: Methods, meta?: Record<string, unknown>) {
    this.checkOpen();

    if (this.services.has(service)) {
      throw new MusterError(
        "rejected",
        `service already registered: ${service}`,
      );
    }

    this.services.set(service, methods);
    this.registry.add(service, this.nodeId, meta);

    return this;
  }

  unregister(service: string) {
    this.checkOpen();

    this.services.delete(service);
    this.registry.remove(service, this.nodeId);

    return this;
  }

  attach(target: Channel) {
    this.checkOpen();

    const link = new Link(target, {
      receive: () => {},
      close: () => this.links.delete(link),
    });

    this.links.add(link);

    return () => link.close();
  }

  async call(
    service: string,
    method: string,
    args: unknown[] = [],
    options: { timeout?: number } = {},
  ) {
    this.checkOpen();

    const methods = this.services.get(service);

    if (methods === undefined) {
      throw new MusterError("not-found", `service not found: ${service}`);
    }

    const handler = hasOwn(methods, method) ? methods[method] : undefined;

    if (typeof handler !== "function") {
      throw new MusterError(
        "not-found",
        `method not found: ${service}.${method}`,
      );
    }

    const timeout =
      options.timeout === undefined ? this.callTimeout : options.timeout;

    return withTimeout(
      Promise.resolve(handler.apply(methods, args)),
      timeout,
      `${service}.${method}`,
    );
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
    return this.registry.watch(service, handler);
  }

  subscribe(topic: string, handler: TopicHandler) {
    this.checkOpen();
    return this.topics.on(topic, handler);
  }

  publish(topic: string, data: unknown) {
    this.checkOpen();
    this.topics.emit(topic, data, this.nodeId);
  }

  instances(service: string) {
    return this.registry.lookup(service);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    for (const link of this.links) {
      link.close();
    }

    this.links.clear();
    this.services.clear();
    this.topics.removeAllListeners();
    this.registry.close();

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
