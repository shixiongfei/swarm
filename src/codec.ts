/*
 * codec.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { Packr, Unpackr } from "msgpackr";
import { MusterError } from "./errors.ts";
import { parseEnvelope } from "./envelope.ts";
import type { Envelope } from "./envelope.ts";

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
    throw new MusterError("failed", "envelope is not encodable");
  }
}

export function decode(data: unknown): Envelope | undefined {
  if (!(data instanceof Uint8Array) && !(data instanceof ArrayBuffer)) {
    return undefined;
  }

  const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);

  try {
    return parseEnvelope(unpackr.unpack(bytes));
  } catch (err) {
    return undefined;
  }
}
