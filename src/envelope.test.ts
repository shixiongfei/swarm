/*
 * envelope.test.ts
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
import { parseEnvelope } from "./envelope.ts";
import type { Envelope } from "./envelope.ts";

describe("parseEnvelope", () => {
  test("accepts every envelope type", () => {
    const envelopes: Envelope[] = [
      {
        t: "hello",
        from: "node-a",
        services: [{ name: "math" }],
        secret: "s3cret",
      },
      { t: "accept", from: "node-a", services: [{ name: "math" }] },
      {
        t: "accept",
        from: "node-a",
        services: [{ name: "echo" }],
        secret: "s3cret",
      },
      { t: "up", from: "node-a", service: "math", meta: { build: 1 } },
      { t: "down", from: "node-a", service: "math" },
      {
        t: "call",
        id: "call-1",
        from: "node-a",
        to: "node-b",
        service: "math",
        method: "add",
        args: [1, 2],
      },
      { t: "reply", id: "call-1", from: "node-b", to: "node-a", value: 3 },
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

    for (const envelope of envelopes) {
      assert.deepEqual(parseEnvelope(envelope), envelope);
    }
  });

  test("rejects values that are not envelopes", () => {
    const values: unknown[] = [null, "hello", 42, [], {}, { t: "unknown" }];

    for (const value of values) {
      assert.equal(parseEnvelope(value), undefined);
    }
  });

  test("rejects malformed envelopes", () => {
    const values: unknown[] = [
      { t: "hello", from: "node-a" },
      { t: "hello", from: "node-a", services: [{ name: 1 }] },
      { t: "hello", from: "node-a", services: [], secret: 1 },
      { t: "hello", from: 1, services: [] },
      {
        t: "hello",
        from: "node-a",
        services: [Object.assign([], { name: "math" })],
      },
      { t: "hello", from: "node-a", services: [{ name: "math", meta: 1 }] },
      { t: "accept", from: "node-a" },
      { t: "accept", from: "node-a", services: [], secret: 1 },
      { t: "up", from: "node-a" },
      { t: "up", from: 1, service: "math" },
      { t: "up", from: "node-a", service: "math", meta: [] },
      { t: "up", from: "node-a", service: "math", meta: new Date() },
      { t: "down", service: "math" },
      {
        t: "call",
        id: "call-1",
        from: "node-a",
        to: "node-b",
        service: "math",
        method: "add",
      },
      {
        t: "call",
        id: "call-1",
        from: "node-a",
        to: "node-b",
        service: "math",
        method: "add",
        args: "no",
      },
      { t: "reply", id: "call-1", from: "node-b" },
      {
        t: "error",
        id: "call-1",
        from: "node-b",
        to: "node-a",
        kind: "nope",
        message: "call failed",
      },
      {
        t: "error",
        id: "call-1",
        from: "node-b",
        to: "node-a",
        kind: "failed",
        message: 1,
      },
      { t: "pub", from: "node-a" },
    ];

    for (const value of values) {
      assert.equal(parseEnvelope(value), undefined);
    }
  });
});
