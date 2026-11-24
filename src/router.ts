/*
 * router.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { SwarmError } from "./errors.ts";
import { withCancel } from "./timeout.ts";
import { Balancer } from "./balancer.ts";
import type { NodeLocal } from "./nodelocal.ts";
import type { NodeLink } from "./nodelink.ts";
import type { Instance } from "./registry.ts";
import type { CallOptions } from "./node.ts";

export const maxHopsLimit = 64;

export type RouterOptions = {
  nodeId: string;
  local: NodeLocal;
  maxHops: number;
  callTimeout: number;
  links: () => readonly NodeLink[];
  hubLinks: () => readonly NodeLink[];
  instances: (service: string) => Instance[];
};

export class Router {
  private readonly balancer = new Balancer();
  private readonly nodeId: string;
  private readonly local: NodeLocal;
  private readonly maxHops: number;
  private readonly callTimeout: number;
  private readonly links: RouterOptions["links"];
  private readonly hubLinks: RouterOptions["hubLinks"];
  private readonly instances: RouterOptions["instances"];

  constructor(options: RouterOptions) {
    this.nodeId = options.nodeId;
    this.local = options.local;
    this.maxHops = options.maxHops;
    this.callTimeout = options.callTimeout;
    this.links = options.links;
    this.hubLinks = options.hubLinks;
    this.instances = options.instances;
  }

  invoke(
    service: string,
    method: string,
    args: unknown[],
    timeout?: number,
    ttl?: number,
    source?: NodeLink,
    target?: string,
    signal?: AbortSignal,
    startedAt?: number,
  ) {
    if (signal !== undefined && signal.aborted) {
      return Promise.reject(
        new SwarmError("cancelled", "CANCELLED", "call was cancelled"),
      );
    }

    const budget =
      timeout !== undefined && startedAt !== undefined
        ? timeout - (Date.now() - startedAt)
        : timeout;

    if (budget !== undefined && budget <= 0) {
      return Promise.reject(
        new SwarmError(
          "timeout",
          "TIMEOUT",
          `call timed out: ${service}.${method}`,
        ),
      );
    }

    if (target !== undefined && target === this.nodeId) {
      return this.local.call(
        service,
        method,
        this.local.context(source, signal),
        args,
        budget,
      );
    }

    if (target === undefined && this.local.hasMethod(service, method)) {
      return this.local.call(
        service,
        method,
        this.local.context(source, signal),
        args,
        budget,
      );
    }

    const remaining = Math.min(
      ttl === undefined ? this.maxHops : ttl,
      maxHopsLimit,
    );

    if (remaining <= 0) {
      throw new SwarmError(
        "not-found",
        "NOT_FOUND",
        `service not found: ${service}`,
        true,
      );
    }

    const outbound = this.links();
    const inbound = this.hubLinks();
    const links =
      source === undefined
        ? outbound
        : outbound.filter((link) => link !== source);
    const hubLinks =
      source === undefined
        ? inbound
        : inbound.filter((link) => link !== source);

    const routed =
      target === undefined
        ? [links, hubLinks]
        : [
            links.filter((link) => link.has(service, target)),
            hubLinks.filter((link) => link.has(service, target)),
          ];

    if (
      target !== undefined &&
      source === undefined &&
      routed[0].length === 0 &&
      routed[1].length === 0
    ) {
      const direct = [...links, ...hubLinks].find(
        (link) => link.peerId === target,
      );

      if (direct !== undefined) {
        return direct.call(
          target,
          service,
          method,
          args,
          budget !== undefined ? budget : this.callTimeout,
          remaining - 1,
          target,
          signal,
        );
      }
    }

    return this.balancer.invoke(
      service,
      method,
      args,
      budget !== undefined ? budget : this.callTimeout,
      remaining - 1,
      target,
      signal,
      ...routed,
    );
  }

  callAll(
    service: string,
    method: string,
    args: unknown[] = [],
    options: CallOptions = {},
  ) {
    if (options.signal !== undefined && options.signal.aborted) {
      return Promise.reject(
        new SwarmError("cancelled", "CANCELLED", "call was cancelled"),
      );
    }

    const timeout =
      options.timeout === undefined ? this.callTimeout : options.timeout;
    const instances = this.instances(service);

    if (instances.length === 0) {
      throw new SwarmError(
        "not-found",
        "NOT_FOUND",
        `service ${service} is not available`,
        true,
      );
    }

    return withCancel(
      Promise.all(
        instances.map((instance) => {
          const promise = Promise.resolve().then(() =>
            this.invoke(
              service,
              method,
              args,
              timeout,
              options.ttl === undefined ? this.maxHops : options.ttl,
              undefined,
              instance.nodeId,
              options.signal,
            ),
          );

          return promise.then(
            (value) => ({ nodeId: instance.nodeId, ok: true as const, value }),
            (error) => ({ nodeId: instance.nodeId, ok: false as const, error }),
          );
        }),
      ),
      options.signal,
    );
  }
}
