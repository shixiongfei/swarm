/*
 * local.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { eventlite } from "eventlite.js";
import { MusterError } from "./errors.ts";
import { Registry } from "./registry.ts";
import type { WatchHandler } from "./registry.ts";

export type Methods<M = Record<string, any>> = Record<
  keyof M,
  (...args: any[]) => any
>;

export type LocalConfig = {
  nodeId: string;
};

const hasOwn =
  Object.hasOwn ||
  ((obj: object, key: PropertyKey) =>
    Object.prototype.hasOwnProperty.call(obj, key));

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

export class Local {
  private readonly nodeId: string;
  private readonly registry = new Registry();
  private readonly services = new Map<string, Methods>();
  private readonly topics = eventlite<string>({
    allowDuplicate: true,
    isolate: true,
  });

  constructor(config: LocalConfig) {
    this.nodeId = config.nodeId;
  }

  register(service: string, methods: Methods, meta?: Record<string, unknown>) {
    if (this.services.has(service)) {
      throw new MusterError(
        "rejected",
        `service already registered: ${service}`,
      );
    }

    this.services.set(service, methods);
    this.registry.add(service, this.nodeId, meta);
  }

  unregister(service: string) {
    this.services.delete(service);
    this.registry.remove(service, this.nodeId);
  }

  registrations() {
    return this.registry
      .snapshot()
      .filter((instance) => instance.owner === this.nodeId);
  }

  has(service: string) {
    return this.services.has(service);
  }

  invoke(
    service: string,
    method: string,
    args: unknown[],
    timeout: number,
  ) {
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

    return withTimeout(
      Promise.resolve(handler.apply(methods, args)),
      timeout,
      `${service}.${method}`,
    );
  }

  lookup(service: string) {
    return this.registry.lookup(service);
  }

  watch(service: string, handler: WatchHandler) {
    return this.registry.watch(service, handler);
  }

  subscribe(topic: string, handler: (data: unknown, from: string) => void) {
    return this.topics.on(topic, handler);
  }

  publish(topic: string, data: unknown, from: string) {
    this.topics.emit(topic, data, from);
  }

  mirror(service: string, owner: string, meta?: Record<string, unknown>) {
    this.registry.add(service, owner, meta);
  }

  drop(service: string, owner: string) {
    this.registry.remove(service, owner);
  }

  dropOwner(owner: string) {
    for (const instance of this.registry.snapshot()) {
      if (instance.owner === owner) {
        this.registry.remove(instance.service, owner);
      }
    }
  }

  close() {
    this.services.clear();
    this.registry.close();
    this.topics.removeAllListeners();
  }
}
