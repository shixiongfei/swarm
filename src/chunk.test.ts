/*
 * chunk.test.ts
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
import { maxEnvelopeSize } from "./codec.ts";
import { chunk } from "./chunk.ts";
import { changesFrame } from "./frame.ts";
import type { Change } from "./envelope.ts";
import type { SwarmError } from "./errors.ts";

const up = (service: string, meta?: Record<string, unknown>): Change => ({
  t: "up",
  from: "node-a",
  service,
  ...(meta === undefined ? {} : { meta }),
});

describe("chunk", () => {
  test("keeps a batch below the limit in one frame", () => {
    const items = [up("math"), up("news")];
    const { chunks, dropped } = chunk(items, changesFrame);

    assert.equal(dropped.length, 0);
    assert.equal(chunks.length, 1);
    assert.deepEqual(chunks[0].items, items);
    assert.deepEqual(chunks[0].frame.message, { t: "changes", items });
    assert.equal(chunks[0].frame.bytes(), chunks[0].frame.bytes());
  });

  test("splits a frame that exceeds the limit without losing order", () => {
    const items = Array.from({ length: 9 }, (_, i) =>
      up(`svc-${i}`, { blob: "x".repeat(3 * 1024 * 1024) }),
    );

    const { chunks, dropped } = chunk(items, changesFrame);

    assert.equal(dropped.length, 0);
    assert.ok(chunks.length > 1);
    assert.deepEqual(
      chunks.flatMap((c) => c.items),
      items,
    );

    for (const c of chunks) {
      assert.ok(c.frame.bytes().byteLength <= maxEnvelopeSize);
    }
  });

  test("splits by the chunk size when byte size is not the limit", () => {
    const items = Array.from({ length: 2500 }, (_, i) => up(`svc-${i}`));
    const { chunks } = chunk(items, changesFrame);

    assert.deepEqual(
      chunks.map((c) => c.items.length),
      [1024, 1024, 452],
    );
  });

  test("skips byte measurement when the limit is disabled", () => {
    let calls = 0;
    const items = [up("math")];
    const { chunks } = chunk(
      items,
      (batch) => {
        const built = changesFrame(batch);

        return {
          message: built.message,
          bytes() {
            calls += 1;

            return built.bytes();
          },
        };
      },
      Infinity,
    );

    assert.equal(chunks.length, 1);
    assert.equal(calls, 0);
  });

  test("drops a single item that cannot fit in one frame", () => {
    const items = [up("huge", { blob: "x".repeat(maxEnvelopeSize) })];
    const { chunks, dropped } = chunk(items, changesFrame);

    assert.equal(chunks.length, 0);
    assert.equal(dropped.length, 1);
    assert.equal(dropped[0].item, items[0]);
    assert.equal((dropped[0].error as SwarmError).code, "ENVELOPE_INVALID");
    assert.equal(dropped[0].error.message, "envelope is too large");
  });

  test("drops a single item that cannot be encoded", () => {
    const items = [up("broken", { fn: () => undefined })];
    const { chunks, dropped } = chunk(items, changesFrame);

    assert.equal(chunks.length, 0);
    assert.equal(dropped.length, 1);
    assert.equal((dropped[0].error as SwarmError).code, "ENVELOPE_INVALID");
  });
});
