/*
 * codec.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { pack, Unpackr } from "msgpackr";
import { decode, encode, maxEnvelopeSize } from "./codec.ts";
import type { SwarmError } from "./errors.ts";
import type { Envelope } from "./envelope.ts";

const unpackr = new Unpackr({ useRecords: false });

const envelopes: Envelope[] = [
  {
    t: "hello",
    from: "node-a",
    version: 1,
    startup: 1,
    token: "token-value",
  },
  {
    t: "accept",
    from: "node-b",
    version: 1,
    startup: 1,
    token: "token-value",
  },
  {
    t: "changes",
    items: [
      { t: "up", from: "node-a", service: "math", meta: { build: 1 } },
      { t: "down", from: "node-a", service: "math" },
    ],
  },
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
    code: "TIMEOUT",
    retriable: false,
    message: "call timed out",
  },
  {
    t: "pubs",
    items: [
      {
        t: "pub",
        id: "pub-1",
        ttl: 8,
        from: "node-a",
        origin: "node-a",
        topic: "news",
        data: { ok: true },
      },
    ],
  },
];

describe("codec", () => {
  test("round-trips every envelope type", () => {
    for (const envelope of envelopes) {
      assert.deepEqual(decode(encode(envelope)), envelope);
    }
  });

  test("pins wire field order for every shape", () => {
    const wire: [Envelope, unknown[]][] = [
      [{ t: "hello", from: "f", version: 2, startup: 3, token: "t" }, [1, "f", 2, 3, "t"]],
      [{ t: "accept", from: "f", version: 2, startup: 3, token: "t" }, [2, "f", 2, 3, "t"]],
      [{ t: "bye", from: "f" }, [3, "f"]],
      [{ t: "ping", from: "f" }, [4, "f"]],
      [{ t: "pong", from: "f" }, [5, "f"]],
      [
        {
          t: "call",
          id: "i",
          from: "f",
          to: "to",
          target: "tg",
          service: "s",
          method: "m",
          args: [7],
          timeout: 8,
          ttl: 9,
        },
        [6, "i", "f", "to", "tg", "s", "m", [7], 8, 9],
      ],
      [{ t: "reply", id: "i", from: "f", to: "to", value: { v: 1 } }, [7, "i", "f", "to", { v: 1 }]],
      [
        {
          t: "error",
          id: "i",
          from: "f",
          to: "to",
          kind: "timeout",
          code: "TIMEOUT",
          retriable: false,
          message: "msg",
        },
        [8, "i", "f", "to", "timeout", "TIMEOUT", false, "msg"],
      ],
      [
        {
          t: "changes",
          items: [
            { t: "up", from: "uf", service: "us", meta: { m: 1 } },
            { t: "down", from: "df", service: "ds" },
          ],
        },
        [9, [[12, "uf", "us", { m: 1 }], [13, "df", "ds"]]],
      ],
      [
        {
          t: "pubs",
          items: [{ t: "pub", from: "pf", origin: "po", topic: "pt", id: "pi", ttl: 8, data: { d: 1 } }],
        },
        [10, [[14, "pf", "po", "pt", "pi", 8, { d: 1 }]]],
      ],
      [
        { t: "peers", from: "f", members: [{ id: "pid", addr: "tcp://a:1", startup: 1 }] },
        [11, "f", [[15, "pid", "tcp://a:1", 1]]],
      ],
    ];

    for (const [envelope, row] of wire) {
      assert.deepEqual(unpackr.unpack(encode(envelope)), row);
    }
  });

  test("encodes the type code in the low byte and missing fields in the high byte", () => {
    const unpack = (bytes: Uint8Array) => unpackr.unpack(bytes) as number[];

    const base: Envelope = {
      t: "call",
      id: "c",
      from: "a",
      to: "b",
      service: "s",
      method: "m",
      args: [],
    };

    assert.equal(unpack(encode(base))[0], (0b111 << 8) | 6);

    const withTarget: Envelope = { ...base, target: "t" };
    assert.equal(unpack(encode(withTarget))[0], (0b110 << 8) | 6);

    const withTargetAndTimeout: Envelope = {
      ...withTarget,
      timeout: 100,
    };
    assert.equal(unpack(encode(withTargetAndTimeout))[0], (0b100 << 8) | 6);

    const full: Envelope = { ...withTargetAndTimeout, ttl: 3 };
    assert.equal(unpack(encode(full))[0], 6);

    const up: Envelope = {
      t: "changes",
      items: [{ t: "up", from: "a", service: "s" }],
    };
    const items = unpack(encode(up))[1] as unknown as number[][];
    assert.equal(items[0][0], (1 << 8) | 12);
  });

  test("round-trips every optional field combination", () => {
    const base = {
      t: "call" as const,
      id: "c",
      from: "a",
      to: "b",
      service: "s",
      method: "m",
      args: [] as unknown[],
    };

    for (const target of [undefined, "t"]) {
      for (const timeout of [undefined, 100]) {
        for (const ttl of [undefined, 3]) {
          const envelope: Envelope = {
            ...base,
            ...(target === undefined ? {} : { target }),
            ...(timeout === undefined ? {} : { timeout }),
            ...(ttl === undefined ? {} : { ttl }),
          };
          assert.deepEqual(decode(encode(envelope)), envelope);
        }
      }
    }
  });

  test("rejects unknown type codes and out-of-range missing bits", () => {
    const headers = [
      99,
      0x0101,
      (0b1000 << 8) | 6,
      (0b0010 << 8) | 12,
      0xffff * 0x100 + 12,
      0x100000000 + 6,
    ];

    for (const header of headers) {
      assert.equal(decode(pack([header])), undefined);
    }
  });

  test("encodes to bytes", () => {
    const encoded = encode({
      t: "changes",
      items: [{ t: "down", from: "node-a", service: "math" }],
    });

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

  test("drops a payload larger than the envelope limit", () => {
    const encoded = encode({
      t: "pubs",
      items: [
        {
          t: "pub",
          id: "pub-1",
          ttl: 8,
          from: "node-a",
          origin: "node-a",
          topic: "news",
          data: "x".repeat(maxEnvelopeSize),
        },
      ],
    });

    assert.ok(encoded.byteLength > maxEnvelopeSize);
    assert.equal(decode(encoded), undefined);
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
      { t: "hello", from: 1, version: 1 },
    ];

    for (const value of values) {
      assert.equal(decode(pack(value)), undefined);
    }
  });

  test("rejects corrupt data", () => {
    const encoded = encode({
      t: "changes",
      items: [{ t: "down", from: "node-a", service: "math" }],
    });
    const values: Uint8Array[] = [
      new Uint8Array(),
      new Uint8Array([0xc1, 0xc1]),
      encoded.slice(0, 2),
    ];

    for (const value of values) {
      assert.equal(decode(value), undefined);
    }
  });

  test("rejects values that cannot round-trip", () => {
    const values: Envelope[] = [
      {
        t: "call",
        id: "call-1",
        from: "node-a",
        to: "node-b",
        service: "math",
        method: "add",
        args: [new Map([[{ id: 1 }, 2]])],
      },
      {
        t: "call",
        id: "call-2",
        from: "node-a",
        to: "node-b",
        service: "math",
        method: "add",
        args: [new Set([1])],
      },
    ];

    for (const value of values) {
      assert.throws(
        () => encode(value),
        (err: SwarmError) =>
          err.kind === "failed" &&
          err.message === "maps and sets cannot be encoded",
      );
    }

    class Point {
      x = 1;
    }

    const custom: Envelope = {
      t: "call",
      id: "call-4",
      from: "node-a",
      to: "node-b",
      service: "math",
      method: "add",
      args: [new Point()],
    };

    assert.throws(
      () => encode(custom),
      (err: SwarmError) =>
        err.kind === "failed" &&
        err.message === "only plain objects, dates and buffers can be encoded",
    );

    const kept: Envelope = {
      t: "call",
      id: "call-3",
      from: "node-a",
      to: "node-b",
      service: "math",
      method: "add",
      args: [new Date(0), { nested: [1] }],
    };
    const binary: Envelope = {
      ...kept,
      args: [new Uint8Array([1, 2])],
    } as Envelope;

    assert.deepEqual(decode(encode(kept)), kept);
    assert.doesNotThrow(() => encode(binary));
  });

  test("rejects binary values that would lose their bytes on decode", () => {
    const values: unknown[] = [
      new ArrayBuffer(2),
      new Int32Array([1, 2, 3]),
      new Float32Array([1.5, 2.5]),
      new Uint8ClampedArray([1]),
      new DataView(new ArrayBuffer(2)),
    ];

    for (const value of values) {
      assert.throws(
        () =>
          encode({
            t: "call",
            id: "call-binary",
            from: "node-a",
            to: "node-b",
            service: "math",
            method: "add",
            args: [value],
          }),
        (err: SwarmError) =>
          err.kind === "failed" &&
          err.code === "ENVELOPE_INVALID" &&
          err.message === "only byte arrays can be encoded as binary data",
      );
    }
  });

  test("rejects circular payloads but keeps shared references", () => {
    const args: Record<string, unknown> = { build: 1 };
    const shared = { build: 1 };

    args.self = args;

    assert.throws(
      () =>
        encode({
          t: "call",
          id: "call-1",
          from: "node-a",
          to: "node-b",
          service: "math",
          method: "add",
          args: [args],
        }),
      (err: SwarmError) =>
        err.kind === "failed" && err.message === "envelope is not encodable",
    );

    assert.doesNotThrow(() =>
      encode({
        t: "call",
        id: "call-2",
        from: "node-a",
        to: "node-b",
        service: "math",
        method: "add",
        args: [{ a: shared }, { b: shared }],
      }),
    );
  });

  test("turns unexpected payload errors into an encode error", () => {
    const hostile = new Proxy(
      {},
      {
        ownKeys() {
          throw new RangeError("payload cannot be inspected");
        },
      },
    );

    assert.throws(
      () =>
        encode({
          t: "call",
          id: "call-1",
          from: "node-a",
          to: "node-b",
          service: "math",
          method: "add",
          args: [hostile],
        }),
      (err: SwarmError) =>
        err.kind === "failed" && err.code === "ENVELOPE_INVALID",
    );
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
        (err: SwarmError) =>
          err.kind === "failed" && err.message === "envelope is not encodable",
      );
    }
  });
});
