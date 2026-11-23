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
import { errorKinds } from "./errors.ts";
import type { Envelope } from "./envelope.ts";

describe("error helpers", () => {
  test("exposes error kinds as a frozen list", () => {
    assert.equal(Object.isFrozen(errorKinds), true);
  });

  test("accepts every error kind", () => {
    for (const kind of errorKinds) {
      const envelope = parseEnvelope({
        t: "error",
        from: "node-a",
        to: "node-b",
        id: "error-1",
        kind,
        message: "boom",
        code: "FAILED",
        retriable: false,
      });

      assert.notEqual(envelope, undefined, kind);
    }
  });
});
describe("parseEnvelope", () => {
  test("accepts every envelope type", () => {
    const envelopes: Envelope[] = [
      {
        t: "hello",
        from: "node-a",
        version: 1,
        startup: 1,
        token: "token-value",
        cluster: "default",
        passive: false,
      },
      {
        t: "accept",
        from: "node-a",
        version: 1,
        startup: 1,
        token: "token-value",
        cluster: "default",
        passive: false,
      },
      {
        t: "changes",
        items: [
          { t: "up", from: "node-a", service: "math", meta: { build: 1 } },
          { t: "down", from: "node-b", service: "math" },
        ],
      },
      {
        t: "call",
        id: "call-1",
        from: "node-a",
        to: "node-b",
        service: "math",
        method: "add",
        args: [1, 2],
      },
      { t: "cancel", id: "call-1", from: "node-a", to: "node-b" },
      { t: "reply", id: "call-1", from: "node-b", to: "node-a", value: 3 },
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
            from: "node-a",
            origin: "node-a",
            topic: "news",
            id: "pub-1",
            ttl: 8,
            data: { ok: true },
          },
        ],
      },
      {
        t: "peers",
        from: "node-a",
        members: [
          { id: "node-b", addr: "tcp://127.0.0.1:9000", startup: 1, passive: false },
        ],
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
      { t: "hello", from: "node-a" },
      { t: "hello", from: 1, version: 1 },
      { t: "hello", from: "node-a", version: 1, token: 1 },
      { t: "hello", from: "node-a", version: 0 },
      { t: "hello", from: "node-a", version: 1.5 },
      { t: "hello", from: "node-a", version: "1" },
      { t: "hello", from: "node-a", version: 1, startup: 1 },
      { t: "hello", from: "node-a", version: 1, startup: 1, token: "t" },
      { t: "accept", from: "node-a", version: 1, startup: 1, token: "t" },
      { t: "accept" },
      { t: "accept", from: "node-a" },
      { t: "accept", from: "node-a", version: 1, startup: 1 },
      { t: "accept", from: "node-a", version: 1, token: 1 },
      { t: "changes" },
      { t: "changes", items: [] },
      { t: "changes", items: "up" },
      { t: "changes", items: [{ t: "up", from: "node-a" }] },
      { t: "changes", items: [{ t: "up", from: 1, service: "math" }] },
      {
        t: "changes",
        items: [{ t: "up", from: "node-a", service: "math", meta: [] }],
      },
      {
        t: "changes",
        items: [{ t: "up", from: "node-a", service: "math", meta: new Date() }],
      },
      { t: "changes", items: [{ t: "down", service: "math" }] },
      {
        t: "changes",
        items: [{ t: "peers", from: "node-a", members: [] }],
      },
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
      { t: "cancel", id: "call-1", from: "node-b" },
      { t: "cancel", id: "call-1", from: "node-b", to: 1 },
      { t: "reply", id: "call-1", from: "node-b" },
      {
        t: "error",
        id: "call-1",
        from: "node-b",
        to: "node-a",
        kind: "nope",
        code: "FAILED",
        retriable: false,
        message: "call failed",
      },
      {
        t: "error",
        id: "call-1",
        from: "node-b",
        to: "node-a",
        kind: "failed",
        code: "NOPE",
        retriable: false,
        message: "call failed",
      },
      {
        t: "error",
        id: "call-1",
        from: "node-b",
        to: "node-a",
        kind: "failed",
        code: 1,
        retriable: false,
        message: "call failed",
      },
      {
        t: "error",
        id: "call-1",
        from: "node-b",
        to: "node-a",
        kind: "failed",
        code: "FAILED",
        retriable: false,
        message: 1,
      },
      {
        t: "error",
        id: "call-1",
        from: "node-b",
        to: "node-a",
        kind: "failed",
        retriable: false,
        message: "call failed",
      },
      {
        t: "error",
        id: "call-1",
        from: "node-b",
        to: "node-a",
        kind: "failed",
        code: "FAILED",
        message: "call failed",
      },
      { t: "pubs" },
      { t: "pubs", items: [] },
      { t: "pubs", items: "pub" },
      { t: "pubs", items: [{ t: "pub", from: "node-a", origin: "node-a" }] },
      { t: "pubs", items: [{ t: "pub", from: "node-a", topic: "news" }] },
      {
        t: "pubs",
        items: [
          {
            t: "pub",
            from: "node-a",
            origin: "node-a",
            topic: "news",
            ttl: 8,
            data: 1,
          },
        ],
      },
      {
        t: "pubs",
        items: [
          {
            t: "pub",
            from: "node-a",
            origin: "node-a",
            topic: "news",
            id: "pub-1",
            ttl: -1,
            data: 1,
          },
        ],
      },
      { t: "pubs", items: [{ t: "up", from: "node-a", service: "math" }] },
      { t: "peers", from: "node-a" },
      { t: "peers", from: "node-a", members: "no" },
      { t: "peersGone", from: "node-a", ids: "no" },
      { t: "peersGone", from: "node-a", ids: [1] },
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

  test("rejects non-integer startup values", () => {
    for (const startup of [NaN, Infinity, 1.5]) {
      assert.equal(
        parseEnvelope({
          t: "hello",
          from: "node-a",
          version: 1,
          startup,
          token: "token-value",
        }),
        undefined,
      );
      assert.equal(
        parseEnvelope({
          t: "accept",
          from: "node-a",
          version: 1,
          startup,
          token: "token-value",
        }),
        undefined,
      );
      assert.equal(
        parseEnvelope({
          t: "peers",
          from: "node-a",
          members: [{ id: "node-b", addr: "tcp://127.0.0.1:9000", startup }],
        }),
        undefined,
      );
    }
  });

  test("accepts a large peers frame and rejects an oversized address", () => {
    const entry = { id: "peer", addr: "tcp://10.0.0.9:9", startup: 1, passive: false };
    const many = Array.from({ length: 8000 }, (_, index) => ({
      ...entry,
      id: `peer-${index}`,
    }));
    const frame = { t: "peers" as const, from: "node-a", members: many };

    assert.equal(parseEnvelope(frame), frame);

    const longAddr = `tcp://10.0.0.9:9/${"a".repeat(1024)}`;

    assert.equal(
      parseEnvelope({
        t: "peers",
        from: "node-a",
        members: [{ ...entry, addr: longAddr }],
      }),
      undefined,
    );
  });
});
