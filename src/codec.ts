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
import { fieldIds, nameOfId, parseEnvelope, shapes } from "./envelope.ts";
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

function encodeField(
  value: Record<string, unknown>,
  shape: Shape,
  field: string,
  seen: WeakSet<object>,
): unknown {
  const item = value[field];
  const elements = shapesOf(shape, field);

  if (elements === undefined || !Array.isArray(item)) {
    assertEncodable(item, seen);

    return item;
  }

  const single = elements.length === 1;

  return item.map((entry) => {
    const record = entry as Record<string, unknown>;
    const shapeOf = single
      ? elements[0]
      : elements.find((candidate) => candidate?.t === record?.t);

    if (shapeOf === undefined) {
      throw new SwarmError(
        "failed",
        "ENVELOPE_INVALID",
        "envelope is not encodable",
        false,
      );
    }

    if (single) {
      assertEncodable(record, seen);

      if (!isObject(record) || !isPlainObject(record)) {
        throw new SwarmError(
          "failed",
          "ENVELOPE_INVALID",
          "envelope is not encodable",
          false,
        );
      }
    }

    return encodeShape(record, shapeOf, seen);
  });
}

function encodeShape(
  value: Record<string, unknown>,
  shape: Shape,
  seen: WeakSet<object>,
): unknown[] {
  const row: unknown[] = [shape.code];

  for (const field of shape.required) {
    row.push(encodeField(value, shape, field, seen));
  }

  let opts: unknown[] | undefined;

  for (const field of shape.optional) {
    if (value[field] === undefined) {
      continue;
    }

    opts ??= [];
    opts.push(fieldIds[field], encodeField(value, shape, field, seen));
  }

  if (opts !== undefined) {
    row.push(opts);
  }

  return row;
}

const failed = Symbol("failed");

function decodeField(item: unknown, shape: Shape, field: string): unknown {
  const elements = shapesOf(shape, field);

  if (elements === undefined || !Array.isArray(item)) {
    return item;
  }

  const decoded: Record<string, unknown>[] = [];

  for (const entry of item) {
    const shapeOf = decodeShape(entry);

    if (shapeOf === undefined) {
      return failed;
    }

    decoded.push(shapeOf);
  }

  return decoded;
}

function decodeShape(value: unknown): Record<string, unknown> | undefined {
  if (!Array.isArray(value) || value.length === 0) {
    return undefined;
  }

  const code = value[0];

  if (typeof code !== "number" || !Number.isInteger(code)) {
    return undefined;
  }

  const shape = byCode.get(code);

  if (shape === undefined) {
    return undefined;
  }

  const base = 1 + shape.required.length;

  if (value.length < base || value.length > base + 1) {
    return undefined;
  }

  const message: Record<string, unknown> =
    shape.t === undefined ? {} : { t: shape.t };
  let at = 1;

  for (const field of shape.required) {
    const item = decodeField(value[at], shape, field);

    if (item === failed) {
      return undefined;
    }

    message[field] = item;
    at += 1;
  }

  if (value.length === base) {
    return message;
  }

  const opts = value[at];

  if (!Array.isArray(opts) || opts.length % 2 !== 0) {
    return undefined;
  }

  for (let i = 0; i < opts.length; i += 2) {
    const id = opts[i];

    if (typeof id !== "number" || !Number.isInteger(id)) {
      return undefined;
    }

    const field = nameOfId.get(id);

    if (field === undefined || !shape.optional.includes(field)) {
      continue;
    }

    const item = decodeField(opts[i + 1], shape, field);

    if (item === failed) {
      return undefined;
    }

    message[field] = item;
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
      encodeShape(
        message as unknown as Record<string, unknown>,
        shape,
        new WeakSet(),
      ),
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
