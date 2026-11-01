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

export type Methods<M = Record<string, any>> = Record<
  keyof M,
  (...args: any[]) => any
>;

export type TopicHandler = (data: unknown, from: string) => void;

const hasOwn =
  Object.hasOwn ||
  ((obj: object, key: PropertyKey) =>
    Object.prototype.hasOwnProperty.call(obj, key));

export class Local {
  private readonly services = new Map<string, Methods>();
  private readonly topics = eventlite<string>({
    allowDuplicate: true,
    isolate: true,
  });

  register(service: string, methods: Methods) {
    if (this.services.has(service)) {
      throw new MusterError(
        "rejected",
        `service already registered: ${service}`,
      );
    }

    this.services.set(service, methods);
    return true;
  }

  unregister(service: string) {
    return this.services.delete(service);
  }

  has(service: string) {
    return this.services.has(service);
  }

  invoke(service: string, method: string, args: unknown[]) {
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

    return handler.apply(methods, args);
  }

  subscribe(topic: string, handler: TopicHandler) {
    return this.topics.on(topic, handler);
  }

  publish(topic: string, data: unknown, nodeId: string) {
    this.topics.emit(topic, data, nodeId);
  }

  close() {
    this.services.clear();
    this.topics.removeAllListeners();
  }
}
