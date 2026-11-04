/*
 * watcher.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { MusterError } from "./errors.ts";
import type {
  Instance,
  Observable,
  WatchEvent,
  WatchHandler,
} from "./registry.ts";

type Observed = {
  instance: Instance;
  sources: Set<Observable>;
};

export class Watcher implements Observable {
  private readonly sources = new Set<Observable>();
  private readonly unsubscribes = new Map<Observable, () => void>();
  private readonly observed = new Map<string, Map<string, Observed>>();
  private readonly watches = new Map<string, WatchHandler[]>();
  private readonly handlers: WatchHandler[] = [];

  addSource(source: Observable) {
    if (this.sources.has(source)) {
      return;
    }

    this.sources.add(source);

    const unsubscribe = source.watch((event) => this.observe(source, event));

    this.unsubscribes.set(source, unsubscribe);
  }

  removeSource(source: Observable) {
    if (!this.sources.delete(source)) {
      return;
    }

    const unsubscribe = this.unsubscribes.get(source);

    if (unsubscribe !== undefined) {
      unsubscribe();
      this.unsubscribes.delete(source);
    }

    this.drop(source);
  }

  watch(handler: WatchHandler): () => void;
  watch(service: string, handler: WatchHandler): () => void;
  watch(service: string | WatchHandler, handler?: WatchHandler) {
    if (typeof service === "function") {
      return this.watchAll(service);
    }

    if (handler === undefined) {
      throw new MusterError("rejected", "watch handler is required");
    }

    return this.watchService(service, handler);
  }

  instances() {
    const instances: Instance[] = [];

    for (const bucket of this.observed.values()) {
      for (const observed of bucket.values()) {
        instances.push(observed.instance);
      }
    }

    return instances;
  }

  close() {
    const instances = this.instances();

    for (const unsubscribe of this.unsubscribes.values()) {
      unsubscribe();
    }

    this.unsubscribes.clear();

    for (const instance of instances) {
      this.emit({ type: "down", instance });
    }

    this.watches.clear();
    this.handlers.length = 0;
    this.observed.clear();
    this.sources.clear();
  }

  private watchAll(handler: WatchHandler) {
    this.handlers.push(handler);

    for (const bucket of this.observed.values()) {
      for (const observed of bucket.values()) {
        notify([handler], { type: "up", instance: observed.instance });
      }
    }

    return () => {
      const index = this.handlers.indexOf(handler);

      if (index >= 0) {
        this.handlers.splice(index, 1);
      }
    };
  }

  private watchService(service: string, handler: WatchHandler) {
    let handlers = this.watches.get(service);

    if (handlers === undefined) {
      handlers = [];
      this.watches.set(service, handlers);
    }

    handlers.push(handler);

    const bucket = this.observed.get(service);

    if (bucket !== undefined) {
      for (const observed of bucket.values()) {
        notify([handler], { type: "up", instance: observed.instance });
      }
    }

    return () => {
      const index = handlers.indexOf(handler);

      if (index < 0) {
        return;
      }

      handlers.splice(index, 1);

      if (handlers.length === 0) {
        this.watches.delete(service);
      }
    };
  }

  private observe(source: Observable, event: WatchEvent) {
    const { instance } = event;

    if (event.type === "up") {
      let bucket = this.observed.get(instance.service);

      if (bucket === undefined) {
        bucket = new Map();
        this.observed.set(instance.service, bucket);
      }

      const observed = bucket.get(instance.nodeId);

      if (observed !== undefined) {
        observed.sources.add(source);
        return;
      }

      bucket.set(instance.nodeId, { instance, sources: new Set([source]) });
      this.emit(event);
      return;
    }

    const bucket = this.observed.get(instance.service);
    const observed = bucket?.get(instance.nodeId);

    if (bucket === undefined || observed === undefined) {
      return;
    }

    if (!observed.sources.delete(source)) {
      return;
    }

    if (observed.sources.size > 0) {
      return;
    }

    bucket.delete(instance.nodeId);

    if (bucket.size === 0) {
      this.observed.delete(instance.service);
    }

    this.emit({ type: "down", instance: observed.instance });
  }

  private emit(event: WatchEvent) {
    notify(this.handlers, event);

    const handlers = this.watches.get(event.instance.service);

    if (handlers !== undefined) {
      notify(handlers, event);
    }
  }

  private drop(source: Observable) {
    for (const [service, bucket] of this.observed) {
      for (const [nodeId, observed] of bucket) {
        if (!observed.sources.delete(source)) {
          continue;
        }

        if (observed.sources.size > 0) {
          continue;
        }

        bucket.delete(nodeId);
        this.emit({ type: "down", instance: observed.instance });
      }

      if (bucket.size === 0) {
        this.observed.delete(service);
      }
    }
  }
}

function notify(handlers: WatchHandler[], event: WatchEvent) {
  for (const handler of handlers) {
    try {
      handler(event);
    } catch (err) {
      // An observer must never break the caller.
    }
  }
}
