/*
 * errors.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

export type ErrorKind = "not-found" | "timeout" | "rejected" | "failed";

export class SwarmError extends Error {
  readonly kind: ErrorKind;
  readonly retriable: boolean;

  constructor(kind: ErrorKind, message: string, retriable = false) {
    super(message);
    this.name = "SwarmError";
    this.kind = kind;
    this.retriable = retriable;
  }
}
