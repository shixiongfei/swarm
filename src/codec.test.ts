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
    cluster: "default",
    passive: "leaf",
  },
  {
    t: "accept",
    from: "node-b",
    version: 1,
    startup: 1,
    token: "token-value",
    cluster: "default",
    passive: "leaf",
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
      [
        { t: "hello", from: "f", version: 2, startup: 3, token: "t", cluster: "c" },
        [1, "f", 2, 3, "t", "c"],
      ],
      [
        { t: "accept", from: "f", version: 2, startup: 3, token: "t", cluster: "c", passive: "leaf" },
        [2, "f", 2, 3, "t", "c", [5, "leaf"]],
      ],
      [{ t: "bye", from: "f" }, [3, "f"]],
      [{ t: "ping", from: "f" }, [4, "f"]],
      [{ t: "pong", from: "f" }, [5, "f"]],
      [
        {
          t: "call",
          id: "i",
          from: "f",
          to: "to",
          service: "s",
          method: "m",
          args: [7],
          target: "tg",
          timeout: 8,
          ttl: 9,
        },
        [6, "i", "f", "to", "s", "m", [7], [2, "tg", 3, 8, 4, 9]],
      ],
      [{ t: "cancel", id: "i", from: "f", to: "to" }, [7, "i", "f", "to"]],
      [{ t: "reply", id: "i", from: "f", to: "to", value: { v: 1 } }, [8, "i", "f", "to", { v: 1 }]],
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
        [9, "i", "f", "to", "timeout", "TIMEOUT", false, "msg"],
      ],
      [
        {
          t: "changes",
          items: [
            { t: "up", from: "uf", service: "us", meta: { m: 1 } },
            { t: "down", from: "df", service: "ds" },
          ],
        },
        [10, [[13, "uf", "us", [1, { m: 1 }]], [14, "df", "ds"]]],
      ],
      [
        {
          t: "pubs",
          items: [{ t: "pub", from: "pf", origin: "po", topic: "pt", id: "pi", ttl: 8, data: { d: 1 } }],
        },
        [11, [[15, "pf", "po", "pt", "pi", 8, { d: 1 }]]],
      ],
      [
        { t: "peers", from: "f", members: [{ id: "pid", addr: "tcp://a:1", startup: 1 }] },
        [12, "f", [[16, "pid", "tcp://a:1", 1]]],
      ],
      [
        { t: "peers", from: "f", members: [{ id: "pid", addr: "tcp://a:1", startup: 1, passive: "leaf" }] },
        [12, "f", [[16, "pid", "tcp://a:1", 1, [5, "leaf"]]]],
      ],
      [
        { t: "peersGone", from: "f", ids: ["gone"] },
        [17, "f", ["gone"]],
      ],
    ];

    for (const [envelope, row] of wire) {
      assert.deepEqual(unpackr.unpack(encode(envelope)), row);
    }
  });

  test("encodes optional fields as id-value pairs and omits missing ones", () => {
    const unpack = (bytes: Uint8Array) => unpackr.unpack(bytes) as unknown[];

    const base: Envelope = {
      t: "call",
      id: "c",
      from: "a",
      to: "b",
      service: "s",
      method: "m",
      args: [],
    };

    assert.deepEqual(unpack(encode(base)), [6, "c", "a", "b", "s", "m", []]);

    const withTarget: Envelope = { ...base, target: "t" };
    assert.deepEqual(unpack(encode(withTarget)), [
      6, "c", "a", "b", "s", "m", [], [2, "t"],
    ]);

    const withTimeout: Envelope = { ...base, timeout: 100 };
    assert.deepEqual(unpack(encode(withTimeout)), [
      6, "c", "a", "b", "s", "m", [], [3, 100],
    ]);

    const zeroTimeout: Envelope = { ...base, timeout: 0 };
    assert.deepEqual(unpack(encode(zeroTimeout)), [
      6, "c", "a", "b", "s", "m", [], [3, 0],
    ]);

    const badTimeout = encode({
      ...base,
      timeout: false,
    } as unknown as Envelope);
    assert.deepEqual(unpack(badTimeout), [
      6, "c", "a", "b", "s", "m", [], [3, false],
    ]);
    assert.equal(decode(badTimeout), undefined);

    const up: Envelope = {
      t: "changes",
      items: [{ t: "up", from: "a", service: "s" }],
    };
    assert.deepEqual(unpack(encode(up)), [10, [[13, "a", "s"]]]);
  });

  test("ignores optional fields it has no id for", () => {
    const wire = pack([6, "c", "a", "b", "s", "m", [], [99, "future", 2, "t", 1, "meta"]]);

    assert.deepEqual(decode(wire), {
      t: "call",
      id: "c",
      from: "a",
      to: "b",
      service: "s",
      method: "m",
      args: [],
      target: "t",
    });
  });

  test("ignores optional ids that name a required field", () => {
    const wire = pack([
      11,
      [[15, "f", "o", "topic", "id", 5, { d: 1 }, [4, 99]]],
    ]);

    assert.deepEqual(decode(wire), {
      t: "pubs",
      items: [
        {
          t: "pub",
          from: "f",
          origin: "o",
          topic: "topic",
          id: "id",
          ttl: 5,
          data: { d: 1 },
        },
      ],
    });
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

  test("rejects unknown type codes and malformed rows", () => {
    const rows: unknown[][] = [
      [99],
      [6.5, "c", "a", "b", "s", "m", []],
      [6],
      [6, "c", "a", "b", "s", "m"],
      [6, "c", "a", "b", "s", "m", [], "extra"],
      [6, "c", "a", "b", "s", "m", [], [2]],
      [6, "c", "a", "b", "s", "m", [], ["target", "t"]],
      [((0b111 << 8) | 6) >>> 0, "c", "a", "b", "s", "m", []],
    ];

    for (const row of rows) {
      assert.equal(decode(pack(row)), undefined);
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
