/*
 * watcher.test.ts
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

  test("global watch replays observed instances", () => {
    const watcher = new Watcher();
    const source = new Registry();
    source.add("math", "node-a");

    watcher.addSource(source);

    const events: WatchEvent[] = [];
    watcher.watch((event) => events.push(event));

    assert.deepEqual(events, [
      { type: "up", instance: { service: "math", nodeId: "node-a" } },
    ]);
  });

  test("global watch receives up and down across sources", () => {
    const watcher = new Watcher();
    const local = new Registry();
    const peer = new Registry();
    const events: WatchEvent[] = [];

    watcher.addSource(local);
    watcher.addSource(peer);
    watcher.watch((event) => events.push(event));

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

  test("addSource replays the source into existing watchers", () => {
    const watcher = new Watcher();
    const source = new Registry();
    const events: WatchEvent[] = [];

    watcher.watch((event) => events.push(event));

    source.add("math", "node-a");
    watcher.addSource(source);

    assert.deepEqual(events, [
      { type: "up", instance: { service: "math", nodeId: "node-a" } },
    ]);
  });

  test("removeSource emits down to global watchers", () => {
    const watcher = new Watcher();
    const source = new Registry();
    const events: WatchEvent[] = [];

    watcher.addSource(source);
    watcher.watch((event) => events.push(event));
    source.add("math", "node-a");
    watcher.removeSource(source);

    assert.deepEqual(events, [
      { type: "up", instance: { service: "math", nodeId: "node-a" } },
      { type: "down", instance: { service: "math", nodeId: "node-a" } },
    ]);
  });

  test("global unsubscribe stops delivery", () => {
    const watcher = new Watcher();
    const source = new Registry();
    const events: WatchEvent[] = [];
    const unsubscribe = watcher.watch((event) => events.push(event));

    watcher.addSource(source);
    source.add("math", "node-a");
    const before = events.length;

    unsubscribe();
    unsubscribe();
    source.add("math", "node-b");

    assert.equal(events.length, before);
  });

  test("global and service watchers are independent", () => {
    const watcher = new Watcher();
    const source = new Registry();
    const globalEvents: WatchEvent[] = [];
    const serviceEvents: WatchEvent[] = [];

    watcher.addSource(source);
    watcher.watch((event) => globalEvents.push(event));
    watcher.watch("math", (event) => serviceEvents.push(event));

    source.add("echo", "node-a");

    assert.deepEqual(globalEvents, [
      { type: "up", instance: { service: "echo", nodeId: "node-a" } },
    ]);
    assert.deepEqual(serviceEvents, []);
  });

  test("instances returns every observed instance", () => {
    const watcher = new Watcher();
    const source = new Registry();

    watcher.addSource(source);
    source.add("math", "node-a");
    source.add("echo", "node-b");

    assert.deepEqual(watcher.instances(), [
      { service: "math", nodeId: "node-a" },
      { service: "echo", nodeId: "node-b" },
    ]);
  });

  test("close emits down to global and service watchers", () => {
    const watcher = new Watcher();
    const source = new Registry();
    const globalEvents: WatchEvent[] = [];
    const mathEvents: WatchEvent[] = [];

    watcher.addSource(source);
    watcher.watch((event) => globalEvents.push(event));
    watcher.watch("math", (event) => mathEvents.push(event));

    source.add("math", "node-a");
    watcher.close();
    source.add("echo", "node-b");

    const up = { type: "up", instance: { service: "math", nodeId: "node-a" } };
    const down = {
      type: "down",
      instance: { service: "math", nodeId: "node-a" },
    };

    assert.deepEqual(globalEvents, [up, down]);
    assert.deepEqual(mathEvents, [up, down]);
    assert.deepEqual(watcher.instances(), []);
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

  test("a throwing watch handler crashes the process", () => {
    const script = `
import { Watcher } from "./watcher.ts";
import { NodeLocal } from "./nodelocal.ts";

const local = new NodeLocal("local-node");
const watcher = new Watcher();

watcher.addSource(local);
watcher.watch("math", () => {
  throw new Error("boom-watch");
});

local.register("math", { add: (a, b) => a + b });

setTimeout(() => console.log("survived"), 100);
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
    assert.match(result.stderr, /boom-watch/);
    assert.doesNotMatch(result.stdout, /survived/);
  });
});
