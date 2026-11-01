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

type Watch = {
  handlers: WatchHandler[];
  observed: Map<string, Observed>;
  unsubscribes: Map<Observable, () => void>;
};

export class Watcher {
  private readonly sources = new Set<Observable>();
  private readonly watches = new Map<string, Watch>();

  addSource(source: Observable) {
    if (!this.sources.add(source)) {
      return;
    }

    for (const [service, watch] of this.watches) {
      this.subscribe(watch, service, source);
    }
  }

  removeSource(source: Observable) {
    if (!this.sources.delete(source)) {
      return;
    }

    for (const watch of this.watches.values()) {
      for (const [nodeId, observed] of watch.observed) {
        if (!observed.sources.delete(source)) {
          continue;
        }

        if (observed.sources.size > 0) {
          continue;
        }

        watch.observed.delete(nodeId);
        notify(watch.handlers, { type: "down", instance: observed.instance });
      }

      const unsubscribe = watch.unsubscribes.get(source);

      if (unsubscribe !== undefined) {
        unsubscribe();
        watch.unsubscribes.delete(source);
      }
    }
  }

  watch(service: string, handler: WatchHandler) {
    let watch = this.watches.get(service);

    if (watch === undefined) {
      watch = this.createWatch(service);
    }

    watch.handlers.push(handler);

    for (const { instance } of watch.observed.values()) {
      notify([handler], { type: "up", instance });
    }

    return () => {
      const index = watch.handlers.indexOf(handler);

      if (index < 0) {
        return;
      }

      watch.handlers.splice(index, 1);

      if (watch.handlers.length > 0) {
        return;
      }

      this.watches.delete(service);

      for (const unsubscribe of watch.unsubscribes.values()) {
        unsubscribe();
      }
    };
  }

  close() {
    for (const watch of this.watches.values()) {
      for (const unsubscribe of watch.unsubscribes.values()) {
        unsubscribe();
      }
    }

    this.watches.clear();
    this.sources.clear();
  }

  private createWatch(service: string) {
    const watch: Watch = {
      handlers: [],
      observed: new Map(),
      unsubscribes: new Map(),
    };

    this.watches.set(service, watch);

    for (const source of this.sources) {
      this.subscribe(watch, service, source);
    }

    return watch;
  }

  private subscribe(watch: Watch, service: string, source: Observable) {
    const unsubscribe = source.watch(service, (event) =>
      observe(watch, source, event),
    );

    watch.unsubscribes.set(source, unsubscribe);
  }
}

function observe(watch: Watch, source: Observable, event: WatchEvent) {
  const { instance } = event;

  if (event.type === "up") {
    const observed = watch.observed.get(instance.nodeId);

    if (observed !== undefined) {
      observed.sources.add(source);
      return;
    }

    watch.observed.set(instance.nodeId, {
      instance,
      sources: new Set([source]),
    });
    notify(watch.handlers, event);
    return;
  }

  const observed = watch.observed.get(instance.nodeId);

  if (observed === undefined || !observed.sources.delete(source)) {
    return;
  }

  if (observed.sources.size > 0) {
    return;
  }

  watch.observed.delete(instance.nodeId);
  notify(watch.handlers, { type: "down", instance: observed.instance });
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
