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

export function encode(message: Envelope): Uint8Array {
  try {
    return packr.pack(message);
  } catch (err) {
    throw new SwarmError("failed", "envelope is not encodable");
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
