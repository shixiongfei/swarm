/*
 * local.ts
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
import { withTimeout } from "./timeout.ts";
import { rethrowAsync } from "./events.ts";
import type { EventLite } from "eventlite.js";

export type Methods<M = Record<string, any>> = Record<
  keyof M,
  (...args: any[]) => any
>;

export type TopicHandler = (data: unknown, origin: string) => void;

export type LocalOptions = {
  onError?: (error: unknown) => void;
};

const hasOwn =
  Object.hasOwn ||
  ((obj: object, key: PropertyKey) =>
    Object.prototype.hasOwnProperty.call(obj, key));

export class Local {
  private readonly services = new Map<string, Methods>();
  private readonly topics: EventLite<string>;
  private closed = false;

  constructor(options: LocalOptions = {}) {
    this.topics = eventlite<string>({
      allowDuplicate: true,
      isolate: {
        onError: options.onError !== undefined ? options.onError : rethrowAsync,
      },
    });
  }

  register(service: string, methods: Methods) {
    this.checkOpen();
    this.services.set(service, methods);
  }

  unregister(service: string) {
    this.checkOpen();
    return this.services.delete(service);
  }

  has(service: string) {
    if (this.closed) {
      return false;
    }

    return this.services.has(service);
  }

  invoke(service: string, method: string, args: unknown[], timeout?: number) {
    this.checkOpen();

    const methods = this.services.get(service);

    if (methods === undefined) {
      throw new SwarmError(
        "not-found",
        "NOT_FOUND",
        `service not found: ${service}`,
        true,
      );
    }

    const handler = hasOwn(methods, method) ? methods[method] : undefined;

    if (typeof handler !== "function") {
      throw new SwarmError(
        "not-found",
        "NOT_FOUND",
        `method not found: ${service}.${method}`,
        true,
      );
    }

    const value = handler.apply(methods, args);

    if (timeout === undefined) {
      return value;
    }

    return withTimeout(
      value,
      timeout,
      () =>
        new SwarmError(
          "timeout",
          "TIMEOUT",
          `call timed out: ${service}.${method}`,
        ),
    );
  }

  subscribe(topic: string, handler: TopicHandler) {
    this.checkOpen();
    return this.topics.on(topic, handler);
  }

  publish(topic: string, data: unknown, origin: string) {
    this.checkOpen();
    this.topics.emit(topic, data, origin);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.services.clear();
    this.topics.removeAllListeners();
  }

  private checkOpen() {
    if (this.closed) {
      throw new SwarmError("rejected", "CLOSED", "local is closed", false);
    }
  }
}
