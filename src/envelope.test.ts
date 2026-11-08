/*
 * envelope.test.ts
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
import { parseEnvelope } from "./envelope.ts";
import type { Envelope } from "./envelope.ts";

describe("parseEnvelope", () => {
  test("accepts every envelope type", () => {
    const envelopes: Envelope[] = [
      {
        t: "hello",
        from: "node-a",
        token: "token-value",
      },
      { t: "accept", from: "node-a" },
      {
        t: "accept",
        from: "node-a",
        token: "token-value",
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
      {
        t: "pub",
        from: "node-a",
        origin: "node-a",
        topic: "news",
        data: { ok: true },
      },
      {
        t: "peers",
        from: "node-a",
        members: [{ id: "node-b", addr: "tcp://127.0.0.1:9000" }],
      },
      { t: "bye", from: "node-a" },
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
      { t: "hello" },
      { t: "hello", from: 1 },
      { t: "hello", from: "node-a", token: 1 },
      { t: "accept" },
      { t: "accept", from: "node-a", token: 1 },
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
      {
        t: "call",
        id: "call-1",
        from: "node-a",
        to: "node-b",
        service: "math",
        method: "add",
        args: [],
        timeout: Infinity,
      },
      {
        t: "call",
        id: "call-1",
        from: "node-a",
        to: "node-b",
        service: "math",
        method: "add",
        args: [],
        timeout: NaN,
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
      { t: "pub", from: "node-a", origin: "node-a" },
      { t: "pub", from: "node-a", topic: "news" },
      { t: "peers", from: "node-a" },
      { t: "peers", from: "node-a", members: "no" },
      { t: "peers", from: "node-a", members: [null] },
      {
        t: "peers",
        from: "node-a",
        members: [{ id: 1, addr: "tcp://127.0.0.1:9000" }],
      },
      { t: "bye" },
      { t: "bye", from: 1 },
    ];

    for (const value of values) {
      assert.equal(parseEnvelope(value), undefined);
    }
  });
});
