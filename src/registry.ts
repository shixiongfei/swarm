/*
 * registry.ts
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

export type MetaData = Record<string, unknown>;

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

const up = Symbol("up");
const down = Symbol("down");

export type RegistryEvents = {
  [up]: (instance: Instance) => void;
  [down]: (instance: Instance) => void;
  [service: string]: (event: WatchEvent) => void;
};

export interface Observable {
  watch(handler: WatchHandler): () => void;
  watch(service: string, handler: WatchHandler): () => void;
}

export class Registry implements Observable {
  private readonly table = new Map<string, Instance[]>();
  private readonly events = eventlite<RegistryEvents>({
    allowDuplicate: true,
    isolate: true,
  });
  private closed = false;

  add(service: string, nodeId: string, meta?: MetaData) {
    this.checkOpen();

    const instances = this.table.get(service);

    if (instances !== undefined) {
      const index = instances.findIndex((item) => item.nodeId === nodeId);

      if (index >= 0) {
        const existing = instances[index];

        if (sameMeta(existing.meta, meta)) {
          return existing;
        }

        const instance: Instance =
          meta === undefined ? { service, nodeId } : { service, nodeId, meta };

        instances[index] = instance;

        this.events.emit(up, instance);
        this.events.emit(service, { type: "up", instance });
        return instance;
      }
    }

    const instance: Instance =
      meta === undefined ? { service, nodeId } : { service, nodeId, meta };

    if (instances !== undefined) {
      instances.push(instance);
    } else {
      this.table.set(service, [instance]);
    }

    this.events.emit(up, instance);
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

  has(service: string, nodeId?: string) {
    const instances = this.table.get(service);

    if (instances === undefined || instances.length === 0) {
      return false;
    }

    if (nodeId === undefined) {
      return true;
    }

    return instances.some((item) => item.nodeId === nodeId);
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

  watch(handler: WatchHandler): () => void;
  watch(service: string, handler: WatchHandler): () => void;
  watch(service: string | WatchHandler, handler?: WatchHandler) {
    this.checkOpen();

    if (typeof service === "function") {
      return this.watchAll(service);
    }

    if (handler === undefined) {
      throw new SwarmError("rejected", "watch handler is required");
    }

    return this.watchService(service, handler);
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

  private watchAll(handler: WatchHandler) {
    const unsubscribes = [
      this.events.on(up, (instance) => {
        handler({ type: "up", instance });
      }),
      this.events.on(down, (instance) => {
        handler({ type: "down", instance });
      }),
    ];

    for (const instance of this.snapshot()) {
      try {
        handler({ type: "up", instance });
      } catch (err) {
        // An observer must never break the caller.
      }
    }

    return () => {
      for (const unsubscribe of unsubscribes) {
        unsubscribe();
      }
    };
  }

  private watchService(service: string, handler: WatchHandler) {
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

    this.events.emit(down, instance);
    this.events.emit(service, { type: "down", instance });
    return instance;
  }

  private checkOpen() {
    if (this.closed) {
      throw new SwarmError("rejected", "registry is closed");
    }
  }
}

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

export function sameMeta(a: unknown, b: unknown): boolean {
  if (a === b) {
    return true;
  }

  if (
    typeof a !== "object" ||
    typeof b !== "object" ||
    a === null ||
    b === null
  ) {
    return false;
  }

  if (Array.isArray(a) || Array.isArray(b)) {
    return (
      Array.isArray(a) &&
      Array.isArray(b) &&
      a.length === b.length &&
      a.every((item, index) => sameMeta(item, b[index]))
    );
  }

  const left = a as Record<string, unknown>;
  const right = b as Record<string, unknown>;
  const keys = Object.keys(left);

  return (
    keys.length === Object.keys(right).length &&
    keys.every((key) => sameMeta(left[key], right[key]))
  );
}
