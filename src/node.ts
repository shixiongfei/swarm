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
import { Registry } from "./registry.ts";
import { Local } from "./local.ts";
import { LinkPort } from "./linkport.ts";
import type { Link } from "./link.ts";
import type { Envelope, Service } from "./envelope.ts";
import type { Methods, TopicHandler } from "./local.ts";
import type { Instance, MetaData, WatchHandler } from "./registry.ts";

export type ServiceProxy<M extends Methods<M> = Methods> = {
  [K in keyof M]: (
    ...args: Parameters<M[K]>
  ) => Promise<Awaited<ReturnType<M[K]>>>;
};

export type NodeConfig = {
  cluster: string;
  secret?: string;
  connectTimeout?: number;
  callTimeout?: number;
};

export type NodeOptions = Partial<NodeConfig>;

type Pending = {
  link: Link;
  resolve(value: unknown): void;
  reject(error: MusterError): void;
  timer: ReturnType<typeof setTimeout>;
};

const nodes = new Map<string, Node>();

export class Node {
  readonly nodeId = randomId();
  readonly cluster: string;
  readonly secret?: string;

  private readonly registry = new Registry();
  private readonly local = new Local();
  private readonly handshakes = new Map<Link, ReturnType<typeof setTimeout>>();
  private readonly links = new Map<string, Link>();
  private readonly pending = new Map<string, Pending>();
  private readonly connectTimeout: number;
  private readonly callTimeout: number;
  private closed = false;

  private readonly onEnvelope = (link: Link, message: Envelope) => {
    switch (message.t) {
      case "hello":
        if (this.secret !== undefined && message.secret !== this.secret) {
          this.endHandshake(link);
          link.close();
          return;
        }

        if (this.connect(link, message.nodeId, message.services)) {
          link.accept(this.nodeId, this.registry.registrations(this.nodeId));
        }
        return;

      case "accept":
        this.connect(link, message.nodeId, message.services);
        return;

      case "up":
        if (message.nodeId !== this.nodeId) {
          this.registry.add(message.service, message.nodeId, message.meta);
        }
        return;

      case "down":
        if (message.nodeId !== this.nodeId) {
          this.registry.remove(message.service, message.nodeId);
        }
        return;

      case "call":
        Promise.resolve()
          .then(() =>
            this.local.invoke(message.service, message.method, message.args),
          )
          .then(
            (value) => {
              try {
                link.reply(message.id, value);
              } catch {
                link.fail(
                  message.id,
                  new MusterError("failed", "reply is not cloneable"),
                );
              }
            },
            (error: unknown) => link.fail(message.id, error),
          );
        return;

      case "reply":
        this.settle(message.id, message.value);
        return;

      case "error":
        this.reject(message.id, new MusterError(message.kind, message.message));
        return;

      case "pub":
        this.local.publish(message.topic, message.data, message.nodeId);
        return;
    }
  };

  constructor(config: NodeConfig) {
    this.cluster = config.cluster;
    this.secret = config.secret;

    this.connectTimeout =
      config.connectTimeout !== undefined ? config.connectTimeout : 3000;

    this.callTimeout =
      config.callTimeout !== undefined ? config.callTimeout : 5000;
  }

  register(service: string, methods: Methods, meta?: MetaData) {
    this.checkOpen();

    if (meta !== undefined) {
      try {
        structuredClone(meta);
      } catch {
        throw new MusterError("rejected", "meta is not cloneable");
      }
    }

    if (this.local.register(service, methods)) {
      const instance = this.registry.add(service, this.nodeId, meta);

      for (const link of this.links.values()) {
        link.register(instance);
      }
    }

    return this;
  }

  unregister(service: string) {
    this.checkOpen();

    if (this.local.unregister(service)) {
      const instance = this.registry.remove(service, this.nodeId);

      if (instance) {
        for (const link of this.links.values()) {
          link.unregister(instance);
        }
      }
    }

    return this;
  }

  attach(port: MessagePort) {
    this.checkOpen();

    const link = new LinkPort(port);

    link.events.on("envelope", (message) => {
      this.onEnvelope(link, message);
    });

    link.events.on("close", () => {
      this.disconnect(link);
    });

    const timer = setTimeout(() => {
      if (!this.handshakes.has(link)) {
        return;
      }

      this.endHandshake(link);
      link.close();
    }, this.connectTimeout);

    this.handshakes.set(link, timer);

    link.hello(
      this.nodeId,
      this.registry.registrations(this.nodeId),
      this.secret,
    );
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
      if (this.local.has(service)) {
        resolve(this.local.invoke(service, method, args));
        return;
      }

      const instances = this.registry.lookup(service);
      const link = this.selectLink(instances);

      if (!link) {
        reject(
          new MusterError(
            "not-found",
            `method not found: ${service}.${method}`,
          ),
        );
        return;
      }

      const id = randomId();

      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(
          new MusterError("timeout", `call timed out: ${service}.${method}`),
        );
      }, timeout);

      this.pending.set(id, { link, resolve, reject, timer });

      try {
        link.call(id, service, method, args);
      } catch {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(new MusterError("failed", "arguments are not cloneable"));
      }
    });
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
    return this.local.subscribe(topic, handler);
  }

  publish(topic: string, data: unknown) {
    this.checkOpen();

    for (const link of this.links.values()) {
      try {
        link.publish(topic, data, this.nodeId);
      } catch {
        throw new MusterError("failed", "data is not cloneable");
      }
    }

    this.local.publish(topic, data, this.nodeId);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    for (const [link, timer] of this.handshakes) {
      clearTimeout(timer);
      link.close();
    }
    this.handshakes.clear();

    for (const link of this.links.values()) {
      link.close();
    }
    this.links.clear();

    for (const entry of this.pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(new MusterError("failed", "node is closed"));
    }
    this.pending.clear();

    this.local.close();
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

  private endHandshake(link: Link) {
    const timer = this.handshakes.get(link);

    if (timer === undefined) {
      return;
    }

    clearTimeout(timer);
    this.handshakes.delete(link);
  }

  private connect(link: Link, nodeId: string, services: Service[]) {
    this.endHandshake(link);

    if (link.nodeId !== undefined && link.nodeId !== nodeId) {
      this.disconnect(link);
      link.close();
      return false;
    }

    const existing = this.links.get(nodeId);

    if (existing !== undefined) {
      if (existing !== link) {
        link.close();
      }
      return false;
    }

    link.setNodeId(nodeId);
    this.links.set(nodeId, link);

    for (const service of services) {
      this.registry.add(service.name, nodeId, service.meta);
    }

    return true;
  }

  private disconnect(link: Link) {
    this.endHandshake(link);
    this.failLink(link);

    if (link.nodeId && this.links.get(link.nodeId) === link) {
      this.links.delete(link.nodeId);
      this.registry.drop(link.nodeId);
    }
  }

  private failLink(link: Link) {
    const error = new MusterError("failed", "link is closed");

    for (const [id, entry] of this.pending) {
      if (entry.link !== link) {
        continue;
      }

      clearTimeout(entry.timer);
      this.pending.delete(id);
      entry.reject(error);
    }
  }

  private settle(id: string, value: unknown) {
    const entry = this.pending.get(id);

    if (entry === undefined) {
      return;
    }

    clearTimeout(entry.timer);
    this.pending.delete(id);
    entry.resolve(value);
  }

  private reject(id: string, error: MusterError) {
    const entry = this.pending.get(id);

    if (entry === undefined) {
      return;
    }

    clearTimeout(entry.timer);
    this.pending.delete(id);
    entry.reject(error);
  }

  private selectLink(instances: Instance[]) {
    for (const instance of instances) {
      const link = this.links.get(instance.nodeId);

      if (link) {
        return link;
      }
    }
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
    connectTimeout: options.connectTimeout,
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
