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

export type ErrorKind =
  "not-found" | "timeout" | "rejected" | "failed" | "cancelled";

export type ErrorCode =
  | "APP_CLOSED"
  | "FAILED"
  | "CANCELLED"
  | "CLOSED"
  | "CLONE_FAILED"
  | "CONGESTED"
  | "ENVELOPE_INVALID"
  | "HANDSHAKE_FAILED"
  | "HANDSHAKE_REJECTED"
  | "HANDSHAKE_TIMEOUT"
  | "HEARTBEAT_TIMEOUT"
  | "MULTICAST_UNAVAILABLE"
  | "NOT_FOUND"
  | "REJECTED"
  | "SERVICE_ATTACHED"
  | "SETUP_FAILED"
  | "SIGNATURE_INVALID"
  | "TIMEOUT"
  | "VERSION_MISMATCH";

const defaultCodes: Record<ErrorKind, ErrorCode> = {
  cancelled: "CANCELLED",
  failed: "FAILED",
  "not-found": "NOT_FOUND",
  rejected: "REJECTED",
  timeout: "TIMEOUT",
};

export const errorKinds: readonly ErrorKind[] = deepFreeze(
  Object.keys(defaultCodes) as ErrorKind[],
);

export class SwarmError extends Error {
  readonly kind: ErrorKind;
  readonly code: string;
  readonly retriable: boolean;

  constructor(kind: ErrorKind, message: string, retriable = false, code = "") {
    super(message);
    this.name = "SwarmError";
    this.kind = kind;
    this.code = code === "" ? defaultCodes[kind] : code;
    this.retriable = retriable;
  }
}

export function errorCode(error: unknown) {
  if (error instanceof SwarmError) {
    return error.code;
  }

  if (error instanceof Error) {
    const code: unknown = (error as { code?: unknown }).code;

    if (typeof code === "string" && code !== "") {
      return code;
    }
  }

  return "FAILED";
}
