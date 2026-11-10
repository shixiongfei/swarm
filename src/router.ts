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
import { Balancer } from "./balancer.ts";
import type { NodeLocal } from "./nodelocal.ts";
import type { NodeLink } from "./nodelink.ts";
import type { Instance } from "./registry.ts";

export type RouterOptions = {
  nodeId: string;
  local: NodeLocal;
  maxHops: number;
  callTimeout: number;
  links: () => readonly NodeLink[];
  hubLinks: () => readonly NodeLink[];
  instances: () => Instance[];
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
  ) {
    if (target !== undefined && target === this.nodeId) {
      return this.withCancel(
        this.local.invoke(service, method, args, timeout),
        signal,
      );
    }

    if (target === undefined && this.local.has(service)) {
      try {
        return this.withCancel(
          this.local.invoke(service, method, args, timeout),
          signal,
        );
      } catch (err) {
        if (!(err instanceof SwarmError) || err.kind !== "not-found") {
          throw err;
        }
      }
    }

    const remaining = ttl === undefined ? this.maxHops : ttl;

    if (remaining <= 0) {
      throw new SwarmError("not-found", `service not found: ${service}`);
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

    return this.balancer.invoke(
      service,
      method,
      args,
      timeout !== undefined ? timeout : this.callTimeout,
      remaining - 1,
      target,
      signal,
      ...routed,
    );
  }

  withCancel<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
    if (signal === undefined) {
      return promise;
    }

    if (signal.aborted) {
      return Promise.reject(new SwarmError("cancelled", "call was cancelled"));
    }

    return new Promise((resolve, reject) => {
      const onAbort = () =>
        reject(new SwarmError("cancelled", "call was cancelled"));

      signal.addEventListener("abort", onAbort, { once: true });

      promise.then(
        (value) => {
          signal.removeEventListener("abort", onAbort);
          resolve(value);
        },
        (error) => {
          signal.removeEventListener("abort", onAbort);
          reject(error);
        },
      );
    });
  }

  collect(
    service: string,
    method: string,
    args: unknown[] = [],
    options: { timeout?: number } = {},
  ) {
    const timeout =
      options.timeout === undefined ? this.callTimeout : options.timeout;
    const instances = this.instances().filter(
      (instance) => instance.service === service,
    );

    if (instances.length === 0) {
      throw new SwarmError("not-found", `service ${service} is not available`);
    }

    return Promise.all(
      instances.map((instance) => {
        const promise =
          instance.nodeId === this.nodeId
            ? Promise.resolve().then(() =>
                this.local.invoke(service, method, args, timeout),
              )
            : Promise.resolve().then(() =>
                this.invoke(
                  service,
                  method,
                  args,
                  timeout,
                  this.maxHops,
                  undefined,
                  instance.nodeId,
                ),
              );

        return promise.then(
          (value) => ({ nodeId: instance.nodeId, ok: true as const, value }),
          (error) => ({ nodeId: instance.nodeId, ok: false as const, error }),
        );
      }),
    );
  }
}
