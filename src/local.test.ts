/*
 * local.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { describe, test } from "node:test";
import { Local } from "./local.ts";
import type { SwarmError } from "./errors.ts";

function makeLocal() {
  return new Local();
}

describe("Local", () => {
  test("register makes the service available", () => {
    const local = makeLocal();

    local.register("math", { add: (a: number, b: number) => a + b });

    assert.equal(local.has("math"), true);
  });

  test("register replaces a duplicate service", () => {
    const local = makeLocal();

    local.register("math", { add: () => 0 });
    local.register("math", { add: () => 1 });

    assert.equal(local.invoke("math", "add", []), 1);
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
      (err: SwarmError) => err.kind === "not-found",
    );
    assert.throws(
      () => local.invoke("math", "nope", []),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("invoke ignores inherited properties", () => {
    const local = makeLocal();
    local.register("math", { add: () => 0 });

    assert.throws(
      () => local.invoke("math", "toString", []),
      (err: SwarmError) => err.kind === "not-found",
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

  test("invoke applies the call timeout", async () => {
    const local = makeLocal();

    local.register("slow", {
      wait: () => new Promise<void>((resolve) => setTimeout(resolve, 40)),
    });

    await assert.rejects(
      local.invoke("slow", "wait", [], 20),
      (err: SwarmError) => err.kind === "timeout",
    );
  });

  test("operations fail after close", () => {
    const local = makeLocal();

    local.register("math", { add: () => 0 });
    local.close();

    assert.equal(local.has("math"), false);
    assert.throws(
      () => local.register("math", {}),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => local.unregister("math"),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => local.invoke("math", "add", []),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => local.subscribe("news", () => {}),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => local.publish("news", 1, "node-a"),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.doesNotThrow(() => local.close());
  });

  test("publish delivers data and the sender", () => {
    const local = makeLocal();
    const received: Array<[unknown, string]> = [];

    const unsubscribe = local.subscribe("news", (data, from) =>
      received.push([data, from]),
    );

    local.publish("news", { ok: true }, "node-a");
    assert.deepEqual(received, [[{ ok: true }, "node-a"]]);

    unsubscribe();
    local.publish("news", { ok: false }, "node-a");
    assert.equal(received.length, 1);
  });

  test("a throwing subscriber crashes the process after delivery", () => {
    const script = `
import { Local } from "./local.ts";

const local = new Local();

local.subscribe("news", () => {
  throw new Error("boom-local");
});
local.subscribe("news", () => console.log("delivered"));

local.publish("news", 1, "local-node");
`;

    const result = spawnSync(
      process.execPath,
      ["--input-type=module", "-e", script],
      {
        cwd: new URL(".", import.meta.url),
        encoding: "utf8",
        timeout: 10000,
      },
    );

    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /boom-local/);
    assert.match(result.stdout, /delivered/);
  });
});
