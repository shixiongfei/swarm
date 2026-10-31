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

export type Instance = {
  readonly service: string;
  readonly owner: string;
  readonly meta?: Record<string, unknown>;
};

export interface WatchEvent {
  type: "up" | "down";
  instance: Instance;
}

export type WatchHandler = (event: WatchEvent) => void;

export class Registry {
  private readonly table = new Map<string, Instance[]>();
  private readonly events = eventlite<string>({
    allowDuplicate: true,
    isolate: true,
  });

  add(service: string, owner: string, meta?: Record<string, unknown>) {
    const instance: Instance =
      meta === undefined ? { service, owner } : { service, owner, meta };

    const instances = this.table.get(service);

    if (instances !== undefined) {
      instances.push(instance);
    } else {
      this.table.set(service, [instance]);
    }

    this.events.emit(service, { type: "up", instance });
    return instance;
  }

  remove(service: string, owner: string) {
    const instances = this.table.get(service);

    if (instances === undefined) {
      return;
    }

    const index = instances.findIndex((item) => item.owner === owner);

    if (index < 0) {
      return;
    }

    const [instance] = instances.splice(index, 1);

    if (instances.length === 0) {
      this.table.delete(service);
    }

    this.events.emit(service, { type: "down", instance });
  }

  lookup(service: string) {
    const instances = this.table.get(service);
    return instances === undefined ? [] : instances.slice();
  }

  watch(service: string, handler: WatchHandler) {
    const off = this.events.on(service, handler);

    for (const instance of this.lookup(service)) {
      try {
        handler({ type: "up", instance });
      } catch (err) {
        // An observer must never break the caller.
      }
    }

    return off;
  }

  close() {
    this.table.clear();
    this.events.removeAllListeners();
  }
}
