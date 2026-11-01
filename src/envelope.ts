/*
 * envelope.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { isMetaData } from "./registry.ts";
import type { MetaData } from "./registry.ts";
import type { ErrorKind } from "./errors.ts";

export type Service = { name: string; meta?: MetaData };

export type Envelope =
  | { t: "hello"; from: string; services: Service[]; secret?: string }
  | { t: "accept"; from: string; services: Service[]; secret?: string }
  | { t: "up"; from: string; service: string; meta?: MetaData }
  | { t: "down"; from: string; service: string }
  | {
      t: "call";
      id: string;
      from: string;
      to: string;
      service: string;
      method: string;
      args: unknown[];
    }
  | { t: "reply"; id: string; from: string; to: string; value: unknown }
  | {
      t: "error";
      id: string;
      from: string;
      to: string;
      kind: ErrorKind;
      message: string;
    }
  | { t: "pub"; from: string; topic: string; data: unknown };

export type EnvelopeHandler = (message: Envelope) => void;

export interface Enveloper {
  send(message: Envelope): void;
  close(): void;
}

const errorKinds: readonly ErrorKind[] = [
  "not-found",
  "timeout",
  "rejected",
  "failed",
];

function isService(value: unknown): value is Service {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const service = value as Service;

  return typeof service.name === "string" && isMetaData(service.meta);
}

function isServices(value: unknown): value is Service[] {
  return Array.isArray(value) && value.every(isService);
}

function isHello(
  message: Extract<Envelope, { t: "hello" }>,
): Envelope | undefined {
  if (
    typeof message.from !== "string" ||
    !isServices(message.services) ||
    (message.secret !== undefined && typeof message.secret !== "string")
  ) {
    return undefined;
  }

  return message;
}

function isAccept(
  message: Extract<Envelope, { t: "accept" }>,
): Envelope | undefined {
  if (
    typeof message.from !== "string" ||
    !isServices(message.services) ||
    (message.secret !== undefined && typeof message.secret !== "string")
  ) {
    return undefined;
  }

  return message;
}

function isUp(message: Extract<Envelope, { t: "up" }>): Envelope | undefined {
  if (
    typeof message.from !== "string" ||
    typeof message.service !== "string" ||
    !isMetaData(message.meta)
  ) {
    return undefined;
  }

  return message;
}

function isDown(
  message: Extract<Envelope, { t: "down" }>,
): Envelope | undefined {
  if (typeof message.from !== "string" || typeof message.service !== "string") {
    return undefined;
  }

  return message;
}

function isCall(
  message: Extract<Envelope, { t: "call" }>,
): Envelope | undefined {
  if (
    typeof message.id !== "string" ||
    typeof message.from !== "string" ||
    typeof message.to !== "string" ||
    typeof message.service !== "string" ||
    typeof message.method !== "string" ||
    !Array.isArray(message.args)
  ) {
    return undefined;
  }

  return message;
}

function isReply(
  message: Extract<Envelope, { t: "reply" }>,
): Envelope | undefined {
  if (
    typeof message.id !== "string" ||
    typeof message.from !== "string" ||
    typeof message.to !== "string"
  ) {
    return undefined;
  }

  return message;
}

function isError(
  message: Extract<Envelope, { t: "error" }>,
): Envelope | undefined {
  if (
    typeof message.id !== "string" ||
    typeof message.from !== "string" ||
    typeof message.to !== "string" ||
    !errorKinds.includes(message.kind) ||
    typeof message.message !== "string"
  ) {
    return undefined;
  }

  return message;
}

function isPub(message: Extract<Envelope, { t: "pub" }>): Envelope | undefined {
  if (typeof message.from !== "string" || typeof message.topic !== "string") {
    return undefined;
  }

  return message;
}

export function parseEnvelope(value: unknown): Envelope | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }

  const message = value as Envelope;

  switch (message.t) {
    case "hello":
      return isHello(message);
    case "accept":
      return isAccept(message);
    case "up":
      return isUp(message);
    case "down":
      return isDown(message);
    case "call":
      return isCall(message);
    case "reply":
      return isReply(message);
    case "error":
      return isError(message);
    case "pub":
      return isPub(message);
    default:
      return undefined;
  }
}
