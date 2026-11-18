/*
 * service.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { SwarmError } from "./errors.ts";
import { rethrowAsync } from "./events.ts";
import type { Methods } from "./local.ts";
import type { Node } from "./node.ts";
import type { WatchEvent } from "./registry.ts";

export type ServiceState = "idle" | "waiting" | "ready";

export type SetupOptions = {
  node: Node;
};

export type ServiceOptions<M extends Methods> = {
  name: string;
  requires?: string[];
  setup: (options: SetupOptions) => M | Promise<M>;
};

export type Service<M extends Methods = Methods> = {
  readonly name: string;
  readonly state: ServiceState;
  attach(node: Node): Service<M>;
  detach(): void;
};

class ServiceImpl<M extends Methods> implements Service<M> {
  readonly name: string;

  private readonly requires: string[];
  private readonly setup: ServiceOptions<M>["setup"];
  private readonly providers = new Map<string, Set<string>>();
  private readonly unsubscribes: Array<() => void> = [];
  private node: Node | undefined;
  private methods: M | undefined;
  private current: ServiceState = "idle";
  private registered = false;
  private preparing = false;
  private generation = 0;

  constructor(options: ServiceOptions<M>) {
    this.name = options.name;
    this.requires = options.requires === undefined ? [] : options.requires;
    this.setup = options.setup;

    for (const dependency of this.requires) {
      this.providers.set(dependency, new Set<string>());
    }
  }

  get state() {
    return this.current;
  }

  attach(node: Node) {
    if (this.node !== undefined) {
      throw new SwarmError(
        "rejected",
        `service ${this.name} is already attached`,
        false,
        "SERVICE_ATTACHED",
      );
    }

    this.node = node;
    this.generation += 1;
    this.current = "waiting";

    for (const providers of this.providers.values()) {
      providers.clear();
    }

    for (const dependency of this.requires) {
      this.unsubscribes.push(
        node.watch(dependency, (event) => this.observe(dependency, event)),
      );
    }

    this.unsubscribes.push(node.events.on("close", () => this.detach()));

    this.evaluate();

    return this;
  }

  detach() {
    const node = this.node;

    this.generation += 1;
    this.methods = undefined;

    if (node === undefined) {
      this.current = "idle";
      return;
    }

    this.node = undefined;

    if (this.registered) {
      this.registered = false;

      if (!node.isClosed) {
        node.unregister(this.name);
      }
    }

    for (const unsubscribe of this.unsubscribes) {
      unsubscribe();
    }

    this.unsubscribes.length = 0;
    this.current = "idle";
  }

  private observe(dependency: string, event: WatchEvent) {
    const providers = this.providers.get(dependency);

    if (providers === undefined) {
      return;
    }

    if (event.type === "up") {
      providers.add(event.instance.nodeId);
    } else {
      providers.delete(event.instance.nodeId);
    }

    this.evaluate();
  }

  private present() {
    for (const providers of this.providers.values()) {
      if (providers.size === 0) {
        return false;
      }
    }

    return true;
  }

  private evaluate() {
    if (this.node === undefined) {
      return;
    }

    if (!this.present()) {
      this.withdraw();
      return;
    }

    void this.serve();
  }

  private withdraw() {
    const node = this.node;

    if (this.current !== "idle") {
      this.current = "waiting";
    }

    if (!this.registered) {
      return;
    }

    this.registered = false;

    if (node !== undefined) {
      node.unregister(this.name);
    }
  }

  private async serve() {
    const node = this.node;

    if (node === undefined || this.registered || this.preparing) {
      return;
    }

    this.preparing = true;

    const generation = this.generation;

    try {
      let methods = this.methods;

      if (methods === undefined) {
        methods = await this.setup({ node });

        if (this.generation === generation) {
          this.methods = methods;
        }
      }

      if (this.generation === generation && this.present()) {
        node.register(this.name, methods);
        this.registered = true;
        this.current = "ready";
      } else if (this.node !== undefined) {
        this.current = "waiting";
      }
    } catch (err) {
      this.fail(node, err);
    } finally {
      this.preparing = false;
    }

    if (!this.registered && this.node !== undefined) {
      this.evaluate();
    }
  }

  private fail(node: Node, error: unknown) {
    const err =
      error instanceof SwarmError
        ? error
        : new SwarmError(
            "failed",
            error instanceof Error ? error.message : String(error),
            false,
            "SETUP_FAILED",
          );

    this.detach();

    if (node.events.listenerCount("error") === 0) {
      rethrowAsync(err);
      return;
    }

    node.events.emit("error", err);
  }
}

export function createService<M extends Methods>(
  options: ServiceOptions<M>,
): Service<M> {
  return new ServiceImpl(options);
}
