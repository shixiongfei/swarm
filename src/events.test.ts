/*
 * events.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { describe, test } from "node:test";
import { reportError } from "./events.ts";

describe("reportError", () => {
  test("emits the original error when one is given", () => {
    const events = new EventEmitter();
    const failure = new Error("boom");
    const seen: Error[] = [];

    events.on("error", (error: Error) => {
      seen.push(error);
    });
    reportError(events, failure);

    assert.equal(seen.length, 1);
    assert.equal(seen[0], failure);
  });

  test("wraps a non-error value before emitting", () => {
    const events = new EventEmitter();
    const seen: Error[] = [];

    events.on("error", (error: Error) => {
      seen.push(error);
    });
    reportError(events, "boom");

    assert.equal(seen.length, 1);
    assert.ok(seen[0] instanceof Error);
    assert.equal(seen[0].message, "boom");
  });
});
