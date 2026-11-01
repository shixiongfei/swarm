/*
 * registry.ts
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

export type MetaData = Record<string, unknown>;

export function isMetaData(value: unknown): value is MetaData | undefined {
  if (value === undefined) {
    return true;
  }

  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const proto = Object.getPrototypeOf(value);

  return proto === Object.prototype || proto === null;
}

export type Instance = {
  readonly service: string;
  readonly nodeId: string;
  readonly meta?: MetaData;
};

export interface WatchEvent {
  type: "up" | "down";
  instance: Instance;
}

export type WatchHandler = (event: WatchEvent) => void;

export interface Observable {
  watch(service: string, handler: WatchHandler): () => void;
}

export class Registry implements Observable {
  private readonly table = new Map<string, Instance[]>();
  private readonly events = eventlite<string>({
    allowDuplicate: true,
    isolate: true,
  });
  private closed = false;

  add(service: string, nodeId: string, meta?: MetaData) {
    this.checkOpen();

    const instances = this.table.get(service);

    if (instances !== undefined) {
      const existing = instances.find((item) => item.nodeId === nodeId);

      if (existing !== undefined) {
        return existing;
      }
    }

    const instance: Instance =
      meta === undefined ? { service, nodeId } : { service, nodeId, meta };

    if (instances !== undefined) {
      instances.push(instance);
    } else {
      this.table.set(service, [instance]);
    }

    this.events.emit(service, { type: "up", instance });
    return instance;
  }

  remove(service: string, nodeId: string) {
    this.checkOpen();
    return this.evict(service, nodeId);
  }

  lookup(service: string) {
    const instances = this.table.get(service);
    return instances === undefined ? [] : instances.slice();
  }

  snapshot() {
    const instances: Instance[] = [];

    for (const bucket of this.table.values()) {
      instances.push(...bucket);
    }

    return instances;
  }

  registrations(nodeId: string) {
    return this.snapshot().filter((instance) => instance.nodeId === nodeId);
  }

  watch(service: string, handler: WatchHandler) {
    this.checkOpen();

    const unsubscribe = this.events.on(service, handler);

    for (const instance of this.lookup(service)) {
      try {
        handler({ type: "up", instance });
      } catch (err) {
        // An observer must never break the caller.
      }
    }

    return unsubscribe;
  }

  drop(nodeId: string) {
    this.checkOpen();

    for (const instance of this.snapshot()) {
      if (instance.nodeId === nodeId) {
        this.remove(instance.service, nodeId);
      }
    }
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    for (const instance of this.snapshot()) {
      this.evict(instance.service, instance.nodeId);
    }

    this.events.removeAllListeners();
  }

  private evict(service: string, nodeId: string) {
    const instances = this.table.get(service);

    if (instances === undefined) {
      return;
    }

    const index = instances.findIndex((item) => item.nodeId === nodeId);

    if (index < 0) {
      return;
    }

    const [instance] = instances.splice(index, 1);

    if (instances.length === 0) {
      this.table.delete(service);
    }

    this.events.emit(service, { type: "down", instance });
    return instance;
  }

  private checkOpen() {
    if (this.closed) {
      throw new MusterError("rejected", "registry is closed");
    }
  }
}
