/*
 * nodelocal.test.ts
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
import { NodeLocal } from "./nodelocal.ts";
import type { SwarmError } from "./errors.ts";
import type { WatchEvent } from "./registry.ts";

describe("NodeLocal", () => {
  test("register returns an instance owned by the node", () => {
    const local = new NodeLocal("node-a");
    const instance = local.register("math", { add: () => 1 }, { build: 1 });

    assert.deepEqual(instance, {
      service: "math",
      nodeId: "node-a",
      meta: { build: 1 },
    });
    assert.equal(local.has("math"), true);
  });

  test("unregister returns the removed instance", () => {
    const local = new NodeLocal("node-a");
    const instance = local.register("math", { add: () => 1 });

    assert.deepEqual(local.unregister("math"), instance);
    assert.equal(local.has("math"), false);
    assert.equal(local.unregister("math"), undefined);
  });

  test("registrations returns the instances of the node", () => {
    const local = new NodeLocal("node-a");

    local.register("math", { add: () => 1 });
    local.register("echo", { ping: () => "pong" });

    assert.deepEqual(local.registrations(), [
      { service: "math", nodeId: "node-a" },
      { service: "echo", nodeId: "node-a" },
    ]);
  });

  test("global watch replays registrations and receives updates", () => {
    const local = new NodeLocal("node-a");
    const math = local.register("math", { add: () => 1 });
    const events: WatchEvent[] = [];

    local.watch((event) => events.push(event));

    const echo = local.register("echo", { ping: () => "pong" });
    local.unregister("math");

    assert.deepEqual(events, [
      { type: "up", instance: math },
      { type: "up", instance: echo },
      { type: "down", instance: math },
    ]);
  });

  test("close emits down and rejects operations", () => {
    const local = new NodeLocal("node-a");
    const instance = local.register("math", { add: () => 1 });
    const events: WatchEvent[] = [];

    local.watch((event) => events.push(event));
    local.close();

    assert.deepEqual(events, [
      { type: "up", instance },
      { type: "down", instance },
    ]);
    assert.equal(local.has("math"), false);
    assert.throws(
      () => local.register("echo", { ping: () => "pong" }),
      (err: SwarmError) => err.kind === "rejected",
    );
  });
});
