/*
 * nodelocal.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { SwarmError } from "./errors.ts";
import { Local } from "./local.ts";
import { Registry } from "./registry.ts";
import { neverAborted } from "./timeout.ts";
import type { Provider } from "./provider.ts";
import type { NodeLink } from "./nodelink.ts";
import type { CallContext, Methods, TopicHandler } from "./local.ts";
import type { MetaData, Observable, WatchHandler } from "./registry.ts";

export class NodeLocal implements Observable, Provider {
  private readonly nodeId: string;
  private readonly registry = new Registry();
  private readonly local: Local;

  constructor(nodeId: string, onError?: (error: unknown) => void) {
    this.nodeId = nodeId;
    this.local = new Local({ onError });
  }

  register(service: string, methods: Methods, meta?: MetaData) {
    this.local.register(service, methods);
    return this.registry.add(service, this.nodeId, meta);
  }

  unregister(service: string) {
    if (!this.local.unregister(service)) {
      return undefined;
    }
    return this.registry.remove(service, this.nodeId);
  }

  has(service: string) {
    return this.local.has(service);
  }

  hasMethod(service: string, method: string) {
    return this.local.hasMethod(service, method);
  }

  invoke(
    service: string,
    method: string,
    args: unknown[],
    timeout?: number,
    _ttl?: number,
    source?: NodeLink,
    _target?: string,
    signal?: AbortSignal,
  ) {
    return this.call(
      service,
      method,
      this.context(source, signal),
      args,
      timeout,
    );
  }

  call(
    service: string,
    method: string,
    ctx: CallContext,
    args: unknown[],
    timeout?: number,
  ) {
    return this.local.invoke(service, method, ctx, args, timeout);
  }

  context(source?: NodeLink, signal?: AbortSignal): CallContext {
    return {
      from:
        source !== undefined && source.peerId !== undefined
          ? source.peerId
          : this.nodeId,
      signal: signal !== undefined ? signal : neverAborted,
    };
  }

  publish(topic: string, data: unknown, origin: string) {
    this.local.publish(topic, data, origin);
  }

  registrations() {
    return this.registry.registrations(this.nodeId);
  }

  watch(handler: WatchHandler): () => void;
  watch(service: string, handler: WatchHandler): () => void;
  watch(service: string | WatchHandler, handler?: WatchHandler) {
    if (typeof service === "function") {
      return this.registry.watch(service);
    }

    if (handler === undefined) {
      throw new SwarmError("rejected", "REJECTED", "watch handler is required");
    }

    return this.registry.watch(service, handler);
  }

  subscribe(topic: string, handler: TopicHandler) {
    return this.local.subscribe(topic, handler);
  }

  close() {
    this.local.close();
    this.registry.close();
  }
}
