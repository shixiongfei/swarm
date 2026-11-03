/*
 * codec.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { pack } from "msgpackr";
import { decode, encode } from "./codec.ts";
import type { MusterError } from "./errors.ts";
import type { Envelope } from "./envelope.ts";

const envelopes: Envelope[] = [
  {
    t: "hello",
    from: "node-a",
    services: [{ name: "math", meta: { build: 1 } }],
    secret: "s3cret",
  },
  { t: "accept", from: "node-b", services: [{ name: "echo" }] },
  { t: "up", from: "node-a", service: "math", meta: { build: 1 } },
  { t: "down", from: "node-a", service: "math" },
  {
    t: "call",
    id: "call-1",
    from: "node-a",
    to: "node-b",
    service: "math",
    method: "add",
    args: [1, { nested: [true, null] }],
  },
  { t: "reply", id: "call-1", from: "node-b", to: "node-a", value: { sum: 3 } },
  {
    t: "error",
    id: "call-1",
    from: "node-b",
    to: "node-a",
    kind: "timeout",
    message: "call timed out",
  },
  { t: "pub", from: "node-a", topic: "news", data: { ok: true } },
];

describe("codec", () => {
  test("round-trips every envelope type", () => {
    for (const envelope of envelopes) {
      assert.deepEqual(decode(encode(envelope)), envelope);
    }
  });

  test("encodes to bytes", () => {
    const encoded = encode({ t: "down", from: "node-a", service: "math" });

    assert.ok(encoded instanceof Uint8Array);
    assert.ok(encoded.length > 0);
  });

  test("round-trips undefined payload values", () => {
    const reply: Envelope = {
      t: "reply",
      id: "call-1",
      from: "node-b",
      to: "node-a",
      value: undefined,
    };
    const call: Envelope = {
      t: "call",
      id: "call-2",
      from: "node-a",
      to: "node-b",
      service: "math",
      method: "add",
      args: [undefined, null],
    };

    const decoded = decode(encode(reply));

    assert.deepEqual(decoded, reply);
    assert.ok(decoded !== undefined && Object.keys(decoded).includes("value"));
    assert.deepEqual(decode(encode(call)), call);
  });

  test("accepts an ArrayBuffer", () => {
    const envelope = envelopes[0];
    const encoded = encode(envelope);
    const buffer = encoded.buffer.slice(
      encoded.byteOffset,
      encoded.byteOffset + encoded.byteLength,
    );

    assert.deepEqual(decode(buffer), envelope);
  });

  test("rejects data that is not binary", () => {
    const values: unknown[] = [
      undefined,
      null,
      "text",
      42,
      {},
      [],
      new DataView(new ArrayBuffer(8)),
    ];

    for (const value of values) {
      assert.equal(decode(value), undefined);
    }
  });

  test("rejects packed values that are not envelopes", () => {
    const values: unknown[] = [
      null,
      42,
      "hello",
      [],
      {},
      { t: "unknown" },
      { t: "hello", from: "node-a" },
    ];

    for (const value of values) {
      assert.equal(decode(pack(value)), undefined);
    }
  });

  test("rejects corrupt data", () => {
    const encoded = encode({ t: "down", from: "node-a", service: "math" });
    const values: Uint8Array[] = [
      new Uint8Array(),
      new Uint8Array([0xc1, 0xc1]),
      encoded.slice(0, 2),
    ];

    for (const value of values) {
      assert.equal(decode(value), undefined);
    }
  });

  test("rejects envelopes that cannot be encoded", () => {
    const values: Envelope[] = [
      {
        t: "call",
        id: "call-1",
        from: "node-a",
        to: "node-b",
        service: "math",
        method: "add",
        args: [() => {}],
      },
      {
        t: "reply",
        id: "call-1",
        from: "node-b",
        to: "node-a",
        value: Symbol("nope"),
      },
    ];

    for (const value of values) {
      assert.throws(
        () => encode(value),
        (err: MusterError) =>
          err.kind === "failed" && err.message === "envelope is not encodable",
      );
    }
  });
});
