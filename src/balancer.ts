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

export type Scannable = {
  readonly transport: Transport;
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
    const socketStart = sockets.length === 0 ? 0 : cursor % sockets.length;
    const ordered = [];

    for (let i = 0; i < ports.length; i++) {
      ordered.push(ports[(portStart + i) % ports.length]);
    }

    for (let i = 0; i < sockets.length; i++) {
      ordered.push(sockets[(socketStart + i) % sockets.length]);
    }

    return ordered;
  }

  invoke(
    service: string,
    method: string,
    args: unknown[],
    timeout: number,
    ...sets: Array<readonly NodeLink[]>
  ) {
    const links = this.order(service, ...sets);

    if (links.length === 0) {
      throw new SwarmError("not-found", `service not found: ${service}`);
    }

    const attempt = async (index: number): Promise<unknown> => {
      const link = links[index];
      const peer = link.peerId;

      if (peer === undefined) {
        throw new SwarmError("failed", "link is not connected", true);
      }

      try {
        return await link.call(peer, service, method, args, timeout);
      } catch (err) {
        if (
          index + 1 < links.length &&
          err instanceof SwarmError &&
          err.retriable
        ) {
          return attempt(index + 1);
        }

        throw err;
      }
    };

    return attempt(0);
  }
}
