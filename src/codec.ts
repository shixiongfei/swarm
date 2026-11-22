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
import { parseEnvelope, shapes } from "./envelope.ts";
import { isObject, isPlainObject } from "./object.ts";
import type { Envelope } from "./envelope.ts";

export const maxEnvelopeSize = 8 * 1024 * 1024;

const packr = new Packr({
  useRecords: false,
  writeFunction: () => {
    throw new TypeError("functions cannot be encoded");
  },
});

const unpackr = new Unpackr({ useRecords: false });

type Shape = (typeof shapes)[number];

const byCode = new Map<number, Shape>();
const byType = new Map<string, Shape>();

for (const shape of shapes) {
  byCode.set(shape.code, shape);

  if (shape.t !== undefined) {
    byType.set(shape.t, shape);
  }
}

function fieldNames(shape: Shape): string[] {
  return Object.keys(shape.fields);
}

function isOptional(shape: Shape, name: string) {
  const token = (shape.fields as Record<string, unknown>)[name];
  return (
    typeof token === "object" &&
    token !== null &&
    "optional" in token &&
    token.optional === true
  );
}

function shapesOf(shape: Shape, field: string): Shape[] | undefined {
  const token = (shape.fields as Record<string, unknown>)[field];
  const value =
    typeof token === "object" && token !== null && "field" in token
      ? token.field
      : token;

  if (!Array.isArray(value) || typeof value[0] !== "object") {
    return undefined;
  }

  return value as Shape[];
}

function encodeShape(
  value: Record<string, unknown>,
  shape: Shape,
): unknown[] {
  const names = fieldNames(shape);
  let mask = 0;
  let bit = 0;

  for (const name of names) {
    if (isOptional(shape, name)) {
      if (value[name] === undefined) {
        mask |= 1 << bit;
      }
      bit += 1;
    }
  }

  const row: unknown[] = [((mask << 8) | shape.code) >>> 0];

  for (const name of names) {
    const item = value[name];

    if (isOptional(shape, name) && item === undefined) {
      continue;
    }

    const elements = shapesOf(shape, name);

    if (elements !== undefined && Array.isArray(item)) {
      row.push(
        item.map((entry) => {
          const record = entry as Record<string, unknown>;
          const shapeOf =
            elements.length === 1
              ? elements[0]
              : elements.find((candidate) => candidate.t === record?.t);

          if (shapeOf === undefined) {
            throw new SwarmError(
              "failed",
              "ENVELOPE_INVALID",
              "envelope is not encodable",
              false,
            );
          }

          return encodeShape(record, shapeOf);
        }),
      );
      continue;
    }

    row.push(item);
  }

  return row;
}

function decodeShape(value: unknown): Record<string, unknown> | undefined {
  if (!Array.isArray(value) || value.length === 0) {
    return undefined;
  }

  const header = value[0];

  if (
    typeof header !== "number" ||
    !Number.isInteger(header) ||
    header < 0 ||
    header > 0xffffffff
  ) {
    return undefined;
  }

  const shape = byCode.get(header & 0xff);

  if (shape === undefined) {
    return undefined;
  }

  const mask = header >>> 8;
  const names = fieldNames(shape);
  let optional = 0;

  for (const name of names) {
    if (isOptional(shape, name)) {
      optional += 1;
    }
  }

  if ((mask >>> optional) !== 0) {
    return undefined;
  }

  let kept = names.length;

  for (let i = 0; i < optional; i += 1) {
    if ((mask & (1 << i)) !== 0) {
      kept -= 1;
    }
  }

  if (value.length !== kept + 1) {
    return undefined;
  }

  const message: Record<string, unknown> =
    "t" in shape && shape.t !== undefined ? { t: shape.t } : {};
  let at = 1;
  let bit = 0;

  for (const name of names) {
    if (isOptional(shape, name)) {
      if ((mask & (1 << bit)) !== 0) {
        bit += 1;
        continue;
      }
      bit += 1;
    }

    let item = value[at];
    at += 1;

    const elements = shapesOf(shape, name);

    if (elements !== undefined && Array.isArray(item)) {
      const decoded: Record<string, unknown>[] = [];

      for (const entry of item) {
        const shapeOf = decodeShape(entry);

        if (shapeOf === undefined) {
          return undefined;
        }

        decoded.push(shapeOf);
      }

      item = decoded;
    }

    message[name] = item;
  }

  return message;
}

export function assertEncodable(
  value: unknown,
  seen: WeakSet<object> = new WeakSet(),
) {
  if (typeof value === "function" || typeof value === "symbol") {
    throw new SwarmError(
      "failed",
      "ENVELOPE_INVALID",
      "envelope is not encodable",
      false,
    );
  }

  if (!isObject(value)) {
    return;
  }

  if (value instanceof Map || value instanceof Set) {
    throw new SwarmError(
      "failed",
      "ENVELOPE_INVALID",
      "maps and sets cannot be encoded",
      false,
    );
  }

  if (value instanceof Date || value instanceof Uint8Array) {
    return;
  }

  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
    throw new SwarmError(
      "failed",
      "ENVELOPE_INVALID",
      "only byte arrays can be encoded as binary data",
      false,
    );
  }

  if (!Array.isArray(value) && !isPlainObject(value)) {
    throw new SwarmError(
      "failed",
      "ENVELOPE_INVALID",
      "only plain objects, dates and buffers can be encoded",
      false,
    );
  }

  if (seen.has(value)) {
    throw new SwarmError(
      "failed",
      "ENVELOPE_INVALID",
      "envelope is not encodable",
      false,
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
  try {
    assertEncodable(message);

    const shape = byType.get(message.t);

    if (shape === undefined) {
      throw new SwarmError(
        "failed",
        "ENVELOPE_INVALID",
        "envelope is not encodable",
        false,
      );
    }

    return packr.pack(
      encodeShape(message as unknown as Record<string, unknown>, shape),
    );
  } catch (err) {
    if (err instanceof SwarmError) {
      throw err;
    }

    throw new SwarmError(
      "failed",
      "ENVELOPE_INVALID",
      "envelope is not encodable",
      false,
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
    const shape = decodeShape(unpackr.unpack(bytes));

    if (shape === undefined) {
      return undefined;
    }

    return parseEnvelope(shape);
  } catch {
    return undefined;
  }
}
