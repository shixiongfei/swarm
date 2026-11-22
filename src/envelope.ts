/*
 * envelope.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { isMetaData } from "./registry.ts";
import { isTimeout } from "./timeout.ts";
import { errorCodes, errorKinds } from "./errors.ts";
import type { ErrorCode, ErrorKind } from "./errors.ts";
import type { MetaData } from "./registry.ts";

const majorVersion = 1;
const minorVersion = 0;

export const protocolVersion = majorVersion * 100 + minorVersion;

const maxPeerAddrLength = 1024;

export type PeerEntry = { id: string; addr: string; startup: number };

export type UpEnvelope = {
  t: "up";
  from: string;
  service: string;
  meta?: MetaData;
};

export type DownEnvelope = { t: "down"; from: string; service: string };

export type Change = UpEnvelope | DownEnvelope;

export type PubEnvelope = {
  t: "pub";
  from: string;
  origin: string;
  topic: string;
  id: string;
  ttl: number;
  data: unknown;
};

export type Envelope =
  | {
      t: "hello";
      from: string;
      version: number;
      startup: number;
      token: string;
    }
  | {
      t: "accept";
      from: string;
      version: number;
      startup: number;
      token: string;
    }
  | { t: "changes"; items: Change[] }
  | {
      t: "call";
      id: string;
      from: string;
      to: string;
      target?: string;
      service: string;
      method: string;
      args: unknown[];
      timeout?: number;
      ttl?: number;
    }
  | { t: "reply"; id: string; from: string; to: string; value: unknown }
  | {
      t: "error";
      id: string;
      from: string;
      to: string;
      kind: ErrorKind;
      code: ErrorCode;
      retriable: boolean;
      message: string;
    }
  | { t: "pubs"; items: PubEnvelope[] }
  | { t: "peers"; from: string; members: PeerEntry[] }
  | { t: "ping"; from: string }
  | { t: "pong"; from: string }
  | { t: "bye"; from: string };

export type EnvelopeHandler = (message: Envelope) => void;

function isHandshake(
  message: Extract<Envelope, { t: "hello" | "accept" }>,
): Envelope | undefined {
  if (
    typeof message.from !== "string" ||
    typeof message.token !== "string" ||
    typeof message.version !== "number" ||
    !Number.isInteger(message.startup) ||
    !Number.isInteger(message.version) ||
    message.version < 1
  ) {
    return undefined;
  }

  return message;
}

function isUp(message: UpEnvelope) {
  return (
    typeof message.from === "string" &&
    typeof message.service === "string" &&
    isMetaData(message.meta)
  );
}

function isDown(message: DownEnvelope) {
  return (
    typeof message.from === "string" && typeof message.service === "string"
  );
}

function isChange(value: unknown) {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const change = value as Change;

  if (change.t === "up") {
    return isUp(change);
  }

  if (change.t === "down") {
    return isDown(change);
  }

  return false;
}

function isChanges(
  message: Extract<Envelope, { t: "changes" }>,
): Envelope | undefined {
  if (
    !Array.isArray(message.items) ||
    message.items.length === 0 ||
    !message.items.every(isChange)
  ) {
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
    (message.target !== undefined && typeof message.target !== "string") ||
    typeof message.service !== "string" ||
    typeof message.method !== "string" ||
    !Array.isArray(message.args) ||
    (message.timeout !== undefined && !isTimeout(message.timeout)) ||
    (message.ttl !== undefined &&
      (!Number.isInteger(message.ttl) || message.ttl < 0))
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
    !errorCodes.includes(message.code) ||
    typeof message.retriable !== "boolean" ||
    typeof message.message !== "string"
  ) {
    return undefined;
  }

  return message;
}

function isPub(value: unknown) {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const pub = value as PubEnvelope;

  return (
    pub.t === "pub" &&
    typeof pub.from === "string" &&
    typeof pub.origin === "string" &&
    typeof pub.topic === "string" &&
    typeof pub.id === "string" &&
    Number.isInteger(pub.ttl) &&
    pub.ttl >= 0
  );
}

function isPubs(
  message: Extract<Envelope, { t: "pubs" }>,
): Envelope | undefined {
  if (
    !Array.isArray(message.items) ||
    message.items.length === 0 ||
    !message.items.every(isPub)
  ) {
    return undefined;
  }

  return message;
}

function isPeerEntry(entry: unknown) {
  if (typeof entry !== "object" || entry === null) {
    return false;
  }

  const peer = entry as PeerEntry;

  return (
    typeof peer.id === "string" &&
    typeof peer.addr === "string" &&
    peer.addr.length <= maxPeerAddrLength &&
    Number.isInteger(peer.startup)
  );
}

function isPeers(
  message: Extract<Envelope, { t: "peers" }>,
): Envelope | undefined {
  if (
    typeof message.from !== "string" ||
    !Array.isArray(message.members) ||
    !message.members.every(isPeerEntry)
  ) {
    return undefined;
  }

  return message;
}

function isPeer(
  message: Extract<Envelope, { t: "ping" | "pong" | "bye" }>,
): Envelope | undefined {
  return typeof message.from === "string" ? message : undefined;
}

export function parseEnvelope(value: unknown): Envelope | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }

  const message = value as Envelope;

  switch (message.t) {
    case "hello":
    case "accept":
      return isHandshake(message);
    case "changes":
      return isChanges(message);
    case "call":
      return isCall(message);
    case "reply":
      return isReply(message);
    case "error":
      return isError(message);
    case "pubs":
      return isPubs(message);
    case "peers":
      return isPeers(message);
    case "ping":
    case "pong":
    case "bye":
      return isPeer(message);
    default:
      return undefined;
  }
}
