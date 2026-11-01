/*
 * watcher.test.ts
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
import { Registry } from "./registry.ts";
import { Watcher } from "./watcher.ts";
import type { WatchEvent } from "./registry.ts";

describe("Watcher", () => {
  test("removeSource keeps an instance another source still reports", () => {
    const watcher = new Watcher();
    const local = new Registry();
    const peer = new Registry();
    const events: WatchEvent[] = [];

    watcher.addSource(local);
    watcher.addSource(peer);
    watcher.watch("math", (event) => events.push(event));

    local.add("math", "node-a");
    peer.add("math", "node-a");
    peer.add("math", "node-b");
    watcher.removeSource(local);

    assert.deepEqual(events, [
      { type: "up", instance: { service: "math", nodeId: "node-a" } },
      { type: "up", instance: { service: "math", nodeId: "node-b" } },
    ]);

    peer.remove("math", "node-a");

    assert.deepEqual(events, [
      { type: "up", instance: { service: "math", nodeId: "node-a" } },
      { type: "up", instance: { service: "math", nodeId: "node-b" } },
      { type: "down", instance: { service: "math", nodeId: "node-a" } },
    ]);
  });

  test("removeSource emits down once and is idempotent", () => {
    const watcher = new Watcher();
    const source = new Registry();
    const events: WatchEvent[] = [];

    watcher.addSource(source);
    watcher.watch("math", (event) => events.push(event));
    source.add("math", "node-a");

    watcher.removeSource(source);
    watcher.removeSource(source);

    assert.deepEqual(events, [
      { type: "up", instance: { service: "math", nodeId: "node-a" } },
      { type: "down", instance: { service: "math", nodeId: "node-a" } },
    ]);
  });

  test("addSource ignores a duplicate source", () => {
    const watcher = new Watcher();
    const source = new Registry();
    const events: WatchEvent[] = [];

    watcher.addSource(source);
    watcher.addSource(source);
    watcher.watch("math", (event) => events.push(event));
    watcher.removeSource(source);

    source.add("math", "node-a");

    assert.deepEqual(events, []);
  });
});
