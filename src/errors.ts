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

import { deepFreeze } from "./object.ts";

export const errorKinds = deepFreeze([
  "not-found",
  "timeout",
  "rejected",
  "failed",
  "cancelled",
] as const);

export const errorCodes = deepFreeze([
  "APP_CLOSED",
  "FAILED",
  "CANCELLED",
  "CLOSED",
  "CLONE_FAILED",
  "CLUSTER_MISMATCH",
  "CONGESTED",
  "DUPLICATE_CALL",
  "ENVELOPE_INVALID",
  "FRAME_DROPPED",
  "HANDSHAKE_FAILED",
  "HANDSHAKE_REJECTED",
  "HANDSHAKE_TIMEOUT",
  "HEARTBEAT_TIMEOUT",
  "INVALID_TIMEOUT",
  "MULTICAST_UNAVAILABLE",
  "NOT_FOUND",
  "PUB_DROPPED",
  "REJECTED",
  "SERVICE_ATTACHED",
  "SETUP_FAILED",
  "SIGNATURE_INVALID",
  "TIMEOUT",
  "VERSION_MISMATCH",
] as const);

export type ErrorKind = (typeof errorKinds)[number];
export type ErrorCode = (typeof errorCodes)[number];

export class SwarmError extends Error {
  readonly kind: ErrorKind;
  readonly code: ErrorCode;
  readonly retriable: boolean;

  constructor(
    kind: ErrorKind,
    code: ErrorCode,
    message: string,
    retriable = false,
  ) {
    super(message);
    this.name = "SwarmError";
    this.kind = kind;
    this.code = code;
    this.retriable = retriable;
  }
}
