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

const Unknown = Symbol("unknown");
const Meta = Symbol("meta");
const Kind = Symbol("kind");
const Code = Symbol("code");

type Tokens = {
  [Unknown]: unknown;
  [Meta]: MetaData;
  [Kind]: ErrorKind;
  [Code]: ErrorCode;
};

type Shape = {
  readonly code: number;
  readonly t?: string;
  readonly fields: Record<string, Field>;
  readonly required: readonly string[];
  readonly optional: readonly string[];
};

type Field =
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor
  | keyof Tokens
  | Shape
  | readonly (Field | Shape)[]
  | { readonly field: Field; readonly optional: true };

function optional<const F extends Field>(field: F) {
  return { field, optional: true as const };
}

export const fieldIds: Record<string, number> = {
  meta: 1,
  target: 2,
  timeout: 3,
  ttl: 4,
};

export const nameOfId = new Map<number, string>();

for (const [name, id] of Object.entries(fieldIds)) {
  if (nameOfId.has(id)) {
    throw new Error(`duplicate optional field id ${id}`);
  }

  nameOfId.set(id, name);
}

function shape<
  const C extends number,
  const T extends string | undefined,
  const F extends Record<string, Field>,
>(code: C, t: T, fields: F) {
  const required: string[] = [];
  const optionalNames: string[] = [];

  for (const [name, token] of Object.entries(fields)) {
    if (typeof token === "object" && token !== null && "optional" in token) {
      optionalNames.push(name);
    } else {
      required.push(name);
    }
  }

  for (const name of optionalNames) {
    if (!Object.hasOwn(fieldIds, name)) {
      throw new Error(`optional field ${name} has no stable id`);
    }
  }

  return { code, t, fields, required, optional: optionalNames };
}

const up = shape(12, "up", {
  from: String,
  service: String,
  meta: optional(Meta),
});

const down = shape(13, "down", {
  from: String,
  service: String,
});

const pub = shape(14, "pub", {
  from: String,
  origin: String,
  topic: String,
  id: String,
  ttl: Number,
  data: Unknown,
});

const peer = shape(15, undefined, {
  id: String,
  addr: String,
  startup: Number,
});

const hello = shape(1, "hello", {
  from: String,
  version: Number,
  startup: Number,
  token: String,
});

const accept = shape(2, "accept", {
  from: String,
  version: Number,
  startup: Number,
  token: String,
});

const bye = shape(3, "bye", { from: String });
const ping = shape(4, "ping", { from: String });
const pong = shape(5, "pong", { from: String });

const call = shape(6, "call", {
  id: String,
  from: String,
  to: String,
  target: optional(String),
  service: String,
  method: String,
  args: [Unknown],
  timeout: optional(Number),
  ttl: optional(Number),
});

const reply = shape(7, "reply", {
  id: String,
  from: String,
  to: String,
  value: Unknown,
});

const error = shape(8, "error", {
  id: String,
  from: String,
  to: String,
  kind: Kind,
  code: Code,
  retriable: Boolean,
  message: String,
});

const changes = shape(9, "changes", {
  items: [up, down],
});

const pubs = shape(10, "pubs", {
  items: [pub],
});

const peers = shape(11, "peers", {
  from: String,
  members: [peer],
});

export const shapes = [
  hello,
  accept,
  bye,
  ping,
  pong,
  call,
  reply,
  error,
  changes,
  pubs,
  peers,
  up,
  down,
  pub,
  peer,
] as const;

type InferField<F> = F extends { field: infer I }
  ? InferField<I>
  : F extends StringConstructor
    ? string
    : F extends NumberConstructor
      ? number
      : F extends BooleanConstructor
        ? boolean
        : F extends keyof Tokens
          ? Tokens[F]
          : F extends readonly (infer E)[]
            ? InferField<E>[]
            : F extends { fields: unknown }
              ? EnvelopeOf<F>
              : never;

type FieldsOf<F extends Record<string, Field>> = {
  -readonly [K in keyof F as F[K] extends { optional: true }
    ? never
    : K]: InferField<F[K]>;
} & {
  -readonly [K in keyof F as F[K] extends { optional: true }
    ? K
    : never]?: InferField<F[K]>;
};

type EnvelopeOf<S> = S extends {
  fields: infer F extends Record<string, Field>;
}
  ? (S extends { t: infer T extends string } ? { t: T } : {}) & FieldsOf<F>
  : never;

export type PeerEntry = EnvelopeOf<typeof peer>;
export type UpEnvelope = EnvelopeOf<typeof up>;
export type DownEnvelope = EnvelopeOf<typeof down>;
export type Change = UpEnvelope | DownEnvelope;
export type PubEnvelope = EnvelopeOf<typeof pub>;
type EnvelopeShape = Exclude<
  (typeof shapes)[number],
  typeof up | typeof down | typeof pub | typeof peer
>;
export type Envelope = EnvelopeOf<EnvelopeShape>;

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
