/*
 * provider.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import type { Instance } from "./registry.ts";
import type { NodeLink } from "./nodelink.ts";

export interface Provider {
  registrations(): Instance[];

  invoke(
    service: string,
    method: string,
    args: unknown[],
    timeout?: number,
    ttl?: number,
    source?: NodeLink,
    target?: string,
  ): any;

  publish(
    topic: string,
    data: unknown,
    origin: string,
    from: string,
    id: string,
    ttl: number,
  ): void;
}
