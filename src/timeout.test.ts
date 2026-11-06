/*
 * timeout.test.ts
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
import { SwarmError } from "./errors.ts";
import { withTimeout } from "./timeout.ts";

describe("withTimeout", () => {
  test("resolves a value before the deadline", async () => {
    assert.equal(
      await withTimeout(7, 50, () => new SwarmError("timeout", "late")),
      7,
    );
  });

  test("resolves a promise before the deadline", async () => {
    assert.equal(
      await withTimeout(
        Promise.resolve(7),
        50,
        () => new SwarmError("timeout", "late"),
      ),
      7,
    );
  });

  test("rejects with the value error", async () => {
    const failure = new Error("boom");

    await assert.rejects(
      withTimeout(
        Promise.reject(failure),
        50,
        () => new SwarmError("timeout", "late"),
      ),
      (err: Error) => err === failure,
    );
  });

  test("does not call onTimeout after the value settles", async () => {
    let calls = 0;

    assert.equal(
      await withTimeout(
        7,
        20,
        () => new SwarmError("timeout", "late"),
        () => {
          calls += 1;
        },
      ),
      7,
    );

    await new Promise((resolve) => setTimeout(resolve, 40));

    assert.equal(calls, 0);
  });

  test("rejects with the timeout error and calls onTimeout", async () => {
    let calls = 0;

    await assert.rejects(
      withTimeout(
        new Promise<never>(() => {}),
        20,
        () => new SwarmError("timeout", "late"),
        () => {
          calls += 1;
        },
      ),
      (err: SwarmError) => err.kind === "timeout" && err.message === "late",
    );

    assert.equal(calls, 1);
  });
});
