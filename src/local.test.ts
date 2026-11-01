/*
 * local.test.ts
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
import { Local } from "./local.ts";
import type { MusterError } from "./errors.ts";

function makeLocal() {
  return new Local();
}

describe("Local", () => {
  test("register makes the service available", () => {
    const local = makeLocal();

    local.register("math", { add: (a: number, b: number) => a + b });

    assert.equal(local.has("math"), true);
  });

  test("register rejects a duplicate service", () => {
    const local = makeLocal();
    local.register("math", { add: () => 0 });

    assert.throws(
      () => local.register("math", {}),
      (err: MusterError) => err.kind === "rejected",
    );
  });

  test("register keeps services independent", () => {
    const local = makeLocal();

    local.register("math", { add: () => 1 });
    local.register("echo", { one: (value: unknown) => value });

    assert.equal(local.has("math"), true);
    assert.equal(local.has("echo"), true);
  });

  test("unregister removes only the named service", () => {
    const local = makeLocal();

    local.register("math", { add: () => 1 });
    local.register("echo", { one: (value: unknown) => value });
    local.unregister("math");

    assert.equal(local.has("math"), false);
    assert.equal(local.has("echo"), true);
  });

  test("unregister unknown services does nothing", () => {
    const local = makeLocal();

    assert.doesNotThrow(() => local.unregister("nope"));
  });

  test("invoke runs the handler with the given arguments", async () => {
    const local = makeLocal();
    let received: unknown[] | undefined;

    local.register("echo", {
      count: (...args: unknown[]) => {
        received = args;
        return args.length;
      },
    });

    assert.equal(await local.invoke("echo", "count", [1, 2]), 2);
    assert.deepEqual(received, [1, 2]);
  });

  test("invoke awaits an async handler", async () => {
    const local = makeLocal();
    local.register("math", { add: async (a: number, b: number) => a + b });

    assert.equal(await local.invoke("math", "add", [2, 3]), 5);
  });

  test("invoke rejects unknown services and methods", () => {
    const local = makeLocal();
    local.register("math", { add: () => 0 });

    assert.throws(
      () => local.invoke("nope", "add", []),
      (err: MusterError) => err.kind === "not-found",
    );
    assert.throws(
      () => local.invoke("math", "nope", []),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("invoke ignores inherited properties", () => {
    const local = makeLocal();
    local.register("math", { add: () => 0 });

    assert.throws(
      () => local.invoke("math", "toString", []),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("invoke propagates a handler exception", () => {
    const local = makeLocal();
    const failure = new Error("boom");

    local.register("boom", {
      fail: () => {
        throw failure;
      },
    });

    assert.throws(
      () => local.invoke("boom", "fail", []),
      (err: Error) => err === failure,
    );
  });

  test("invoke rejects an async handler failure", async () => {
    const local = makeLocal();
    local.register("boom", {
      fail: async () => {
        throw new Error("boom");
      },
    });

    await assert.rejects(local.invoke("boom", "fail", []), /boom/);
  });

  test("publish delivers data and the sender", () => {
    const local = makeLocal();
    const received: Array<[unknown, string]> = [];

    const off = local.subscribe("news", (data, from) =>
      received.push([data, from]),
    );

    local.publish("news", { ok: true }, "node-a");
    assert.deepEqual(received, [[{ ok: true }, "node-a"]]);

    off();
    local.publish("news", { ok: false }, "node-a");
    assert.equal(received.length, 1);
  });

  test("a throwing subscriber does not break delivery", () => {
    const local = makeLocal();
    const received: unknown[] = [];

    local.subscribe("news", () => {
      throw new Error("boom");
    });
    local.subscribe("news", (data) => received.push(data));

    local.publish("news", 1, "node-a");

    assert.deepEqual(received, [1]);
  });

  test("close clears services and subscriptions", () => {
    const local = makeLocal();
    const received: unknown[] = [];

    local.register("math", { add: () => 0 });
    local.subscribe("news", (data) => received.push(data));
    local.close();

    assert.equal(local.has("math"), false);

    local.publish("news", 1, "node-a");
    assert.deepEqual(received, []);
  });
});
