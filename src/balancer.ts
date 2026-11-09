/*
 * balancer.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { SwarmError } from "./errors.ts";
import type { Transport } from "./link.ts";
import type { NodeLink } from "./nodelink.ts";

const bucketSize = 10;

export type Scannable = {
  readonly transport: Transport;
  readonly latency?: number;
  has(service: string): boolean;
};

export class Balancer {
  private readonly cursors = new Map<string, number>();

  order<T extends Scannable>(service: string, ...sets: Array<readonly T[]>) {
    const ports = [];
    const sockets = [];

    for (const set of sets) {
      for (const item of set) {
        if (!item.has(service)) {
          continue;
        }

        if (item.transport === "port") {
          ports.push(item);
        } else {
          sockets.push(item);
        }
      }
    }

    if (ports.length === 0 && sockets.length === 0) {
      return [];
    }

    let cursor = this.cursors.get(service);

    if (cursor === undefined) {
      cursor = 0;
    }

    this.cursors.set(service, cursor + 1);

    const portStart = ports.length === 0 ? 0 : cursor % ports.length;
    const ordered = [];

    for (let i = 0; i < ports.length; i++) {
      ordered.push(ports[(portStart + i) % ports.length]);
    }

    const buckets = new Map<number, T[]>();

    for (const socket of sockets) {
      const key =
        socket.latency === undefined
          ? Infinity
          : Math.floor(socket.latency / bucketSize);

      const bucket = buckets.get(key);

      if (bucket === undefined) {
        buckets.set(key, [socket]);
      } else {
        bucket.push(socket);
      }
    }

    const keys = Array.from(buckets.keys()).sort((a, b) => a - b);

    for (const key of keys) {
      const bucket = buckets.get(key);

      if (bucket === undefined) {
        continue;
      }

      const start = cursor % bucket.length;

      for (let i = 0; i < bucket.length; i++) {
        ordered.push(bucket[(start + i) % bucket.length]);
      }
    }

    return ordered;
  }

  invoke(
    service: string,
    method: string,
    args: unknown[],
    timeout: number,
    ttl: number,
    target: string | undefined,
    signal: AbortSignal | undefined,
    ...sets: Array<readonly NodeLink[]>
  ) {
    const links = this.order(service, ...sets);

    if (links.length === 0) {
      throw new SwarmError("not-found", `service not found: ${service}`);
    }

    const started = Date.now();

    const attempt = async (index: number): Promise<unknown> => {
      if (signal !== undefined && signal.aborted) {
        throw new SwarmError("cancelled", "call was cancelled");
      }

      const link = links[index];
      const peer = link.peerId;

      if (peer === undefined) {
        throw new SwarmError("failed", "link is not connected", true);
      }

      const remaining = timeout - (Date.now() - started);

      if (remaining <= 0) {
        throw new SwarmError("timeout", `call timed out: ${service}.${method}`);
      }

      try {
        return await link.call(
          peer,
          service,
          method,
          args,
          remaining,
          ttl,
          target,
          signal,
        );
      } catch (err) {
        if (
          index + 1 < links.length &&
          err instanceof SwarmError &&
          (err.retriable || err.kind === "not-found" || err.kind === "timeout")
        ) {
          return attempt(index + 1);
        }

        throw err;
      }
    };

    return attempt(0);
  }
}
