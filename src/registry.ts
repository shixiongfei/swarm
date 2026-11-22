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
import { deepFreeze, isObject, isPlainObject } from "./object.ts";

export type MetaData = Record<string, unknown>;

export type Instance = {
  readonly service: string;
  readonly nodeId: string;
  readonly meta?: Readonly<MetaData>;
};

function toInstance(
  service: string,
  nodeId: string,
  meta?: MetaData,
): Instance {
  return deepFreeze(
    meta === undefined
      ? { service, nodeId }
      : { service, nodeId, meta: Object.assign({}, meta) },
  );
}

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
  private readonly table = new Map<string, Map<string, Instance>>();
  private readonly events = eventlite<RegistryEvents>({
    allowDuplicate: true,
    isolate: true,
  });
  private closed = false;

  add(service: string, nodeId: string, meta?: MetaData) {
    this.checkOpen();

    const bucket = this.table.get(service);
    const existing = bucket?.get(nodeId);

    if (existing !== undefined && sameMeta(existing.meta, meta)) {
      return existing;
    }

    const instance = toInstance(service, nodeId, meta);

    if (bucket !== undefined) {
      bucket.set(nodeId, instance);
    } else {
      this.table.set(service, new Map([[nodeId, instance]]));
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
    const bucket = this.table.get(service);
    return bucket === undefined ? [] : [...bucket.values()];
  }

  has(service: string, nodeId?: string) {
    const bucket = this.table.get(service);

    if (bucket === undefined) {
      return false;
    }

    if (nodeId === undefined) {
      return true;
    }

    return bucket.has(nodeId);
  }

  snapshot() {
    const instances: Instance[] = [];

    for (const bucket of this.table.values()) {
      for (const instance of bucket.values()) {
        instances.push(instance);
      }
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
      throw new SwarmError("rejected", "REJECTED", "watch handler is required");
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
    const bucket = this.table.get(service);

    if (bucket === undefined) {
      return;
    }

    const instance = bucket.get(nodeId);

    if (instance === undefined) {
      return;
    }

    bucket.delete(nodeId);

    if (bucket.size === 0) {
      this.table.delete(service);
    }

    this.events.emit(down, instance);
    this.events.emit(service, { type: "down", instance });
    return instance;
  }

  private checkOpen() {
    if (this.closed) {
      throw new SwarmError("rejected", "CLOSED", "registry is closed", false);
    }
  }
}

export function isMetaData(value: unknown): value is MetaData | undefined {
  return value === undefined || (isObject(value) && isPlainObject(value));
}

export function sameMeta(a: unknown, b: unknown) {
  if (a === b) {
    return true;
  }

  return equalMeta(a, b, new WeakMap());
}

function equalMeta(
  a: unknown,
  b: unknown,
  seen: WeakMap<object, WeakSet<object>>,
): boolean {
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

  if (a instanceof Date || b instanceof Date) {
    return (
      a instanceof Date &&
      b instanceof Date &&
      Object.is(a.getTime(), b.getTime())
    );
  }

  if (a instanceof Map || b instanceof Map) {
    if (!(a instanceof Map) || !(b instanceof Map) || a.size !== b.size) {
      return false;
    }

    if (visited(a, b, seen)) {
      return true;
    }

    for (const [key, value] of a) {
      if (!b.has(key) || !equalMeta(value, b.get(key), seen)) {
        return false;
      }
    }

    return true;
  }

  if (a instanceof Set || b instanceof Set) {
    if (!(a instanceof Set) || !(b instanceof Set) || a.size !== b.size) {
      return false;
    }

    for (const value of a) {
      if (!b.has(value)) {
        return false;
      }
    }

    return true;
  }

  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) {
      return false;
    }

    if (visited(a, b, seen)) {
      return true;
    }

    return a.every((item, index) => equalMeta(item, b[index], seen));
  }

  if (visited(a, b, seen)) {
    return true;
  }

  const left = a as Record<string, unknown>;
  const right = b as Record<string, unknown>;
  const keys = Object.keys(left);

  return (
    keys.length === Object.keys(right).length &&
    keys.every((key) => equalMeta(left[key], right[key], seen))
  );
}

function visited(a: object, b: object, seen: WeakMap<object, WeakSet<object>>) {
  let pairs = seen.get(a);

  if (pairs === undefined) {
    pairs = new WeakSet();
    seen.set(a, pairs);
  }

  if (pairs.has(b)) {
    return true;
  }

  pairs.add(b);

  return false;
}
