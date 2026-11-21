/*
 * codec.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { Packr, Unpackr } from "msgpackr";
import { SwarmError } from "./errors.ts";
import { parseEnvelope } from "./envelope.ts";
import type { Envelope } from "./envelope.ts";

export const maxEnvelopeSize = 8 * 1024 * 1024;

const packr = new Packr({
  useRecords: false,
  writeFunction: () => {
    throw new TypeError("functions cannot be encoded");
  },
});

const unpackr = new Unpackr({ useRecords: false });

export function assertEncodable(
  value: unknown,
  seen: WeakSet<object> = new WeakSet(),
): void {
  if (typeof value === "function" || typeof value === "symbol") {
    throw new SwarmError(
      "failed",
      "envelope is not encodable",
      false,
      "ENVELOPE_INVALID",
    );
  }

  if (typeof value !== "object" || value === null) {
    return;
  }

  if (value instanceof Map || value instanceof Set) {
    throw new SwarmError(
      "failed",
      "maps and sets cannot be encoded",
      false,
      "ENVELOPE_INVALID",
    );
  }

  if (
    value instanceof Date ||
    value instanceof ArrayBuffer ||
    ArrayBuffer.isView(value)
  ) {
    return;
  }

  if (seen.has(value)) {
    throw new SwarmError(
      "failed",
      "envelope is not encodable",
      false,
      "ENVELOPE_INVALID",
    );
  }

  seen.add(value);

  if (Array.isArray(value)) {
    for (const item of value) {
      assertEncodable(item, seen);
    }
  } else {
    for (const key of Object.keys(value)) {
      assertEncodable((value as Record<string, unknown>)[key], seen);
    }
  }

  seen.delete(value);
}

export function encode(message: Envelope): Uint8Array {
  assertEncodable(message);

  try {
    return packr.pack(message);
  } catch (err) {
    throw new SwarmError(
      "failed",
      "envelope is not encodable",
      false,
      "ENVELOPE_INVALID",
    );
  }
}

export function decode(data: unknown): Envelope | undefined {
  if (!(data instanceof Uint8Array) && !(data instanceof ArrayBuffer)) {
    return undefined;
  }

  const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);

  if (bytes.byteLength > maxEnvelopeSize) {
    return undefined;
  }

  try {
    return parseEnvelope(unpackr.unpack(bytes));
  } catch (err) {
    return undefined;
  }
}
