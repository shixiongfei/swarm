/*
 * nodelocal.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { Local } from "./local.ts";
import { Registry } from "./registry.ts";
import type { Methods, TopicHandler } from "./local.ts";
import type { MetaData, Observable, WatchHandler } from "./registry.ts";

export class NodeLocal implements Observable {
  private readonly nodeId: string;
  private readonly registry = new Registry();
  private readonly local = new Local();

  constructor(nodeId: string) {
    this.nodeId = nodeId;
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

  invoke(service: string, method: string, args: unknown[], timeout?: number) {
    return this.local.invoke(service, method, args, timeout);
  }

  publish(topic: string, data: unknown, from: string) {
    this.local.publish(topic, data, from);
  }

  registrations() {
    return this.registry.registrations(this.nodeId);
  }

  watch(service: string, handler: WatchHandler) {
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
