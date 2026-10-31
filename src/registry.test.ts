/*
 * registry.test.ts
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
import type { WatchEvent } from "./registry.ts";

describe("Registry", () => {
  test("add stores an instance", () => {
    const registry = new Registry();
    const instance = registry.add("math", "node-a");

    assert.deepEqual(instance, { service: "math", owner: "node-a" });
    assert.deepEqual(registry.lookup("math"), [instance]);
  });

  test("add keeps meta when given", () => {
    const registry = new Registry();
    const instance = registry.add("math", "node-a", { build: 1 });

    assert.deepEqual(instance.meta, { build: 1 });
  });

  test("add emits up after a watcher is attached", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch("math", (event) => events.push(event));

    const instance = registry.add("math", "node-a");

    assert.deepEqual(events, [{ type: "up", instance }]);
  });

  test("watch delivers the snapshot before increments", () => {
    const registry = new Registry();
    const a = registry.add("math", "node-a");
    const b = registry.add("math", "node-b");
    const events: WatchEvent[] = [];

    registry.watch("math", (event) => events.push(event));
    const c = registry.add("math", "node-c");

    assert.deepEqual(events, [
      { type: "up", instance: a },
      { type: "up", instance: b },
      { type: "up", instance: c },
    ]);
  });

  test("remove emits down and drops the bucket", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch("math", (event) => events.push(event));
    const instance = registry.add("math", "node-a");

    registry.remove("math", "node-a");

    assert.deepEqual(events[1], { type: "down", instance });
    assert.deepEqual(registry.lookup("math"), []);
  });

  test("remove unknown service or owner does nothing", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch("math", (event) => events.push(event));
    registry.add("math", "node-a");
    const before = events.length;

    registry.remove("nope", "node-a");
    registry.remove("math", "nope");

    assert.equal(events.length, before);
    assert.equal(registry.lookup("math").length, 1);
  });

  test("remove only drops the matching owner", () => {
    const registry = new Registry();
    registry.add("math", "node-a");
    registry.add("math", "node-b");

    registry.remove("math", "node-a");

    assert.deepEqual(
      registry.lookup("math").map((item) => item.owner),
      ["node-b"],
    );
  });

  test("lookup returns a copy", () => {
    const registry = new Registry();
    registry.add("math", "node-a");

    registry.lookup("math").pop();

    assert.equal(registry.lookup("math").length, 1);
  });

  test("off stops delivery and is idempotent", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    const off = registry.watch("math", (event) => events.push(event));
    registry.add("math", "node-a");
    const before = events.length;

    off();
    off();
    registry.add("math", "node-b");

    assert.equal(events.length, before);
  });

  test("off only removes its own registration", () => {
    const registry = new Registry();
    const first: WatchEvent[] = [];
    const second: WatchEvent[] = [];
    const offFirst = registry.watch("math", (event) => first.push(event));
    registry.watch("math", (event) => second.push(event));

    offFirst();
    registry.add("math", "node-a");

    assert.equal(first.length, 0);
    assert.equal(second.length, 1);
  });

  test("a throwing watcher does not break add or other watchers", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch("math", () => {
      throw new Error("boom");
    });
    registry.watch("math", (event) => events.push(event));

    const instance = registry.add("math", "node-a");

    assert.deepEqual(registry.lookup("math"), [instance]);
    assert.deepEqual(events, [{ type: "up", instance }]);
  });

  test("a throwing watcher does not break the snapshot", () => {
    const registry = new Registry();
    registry.add("math", "node-a");

    assert.doesNotThrow(() => {
      registry.watch("math", () => {
        throw new Error("boom");
      });
    });
  });

  test("close clears the table and removes listeners", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch("math", (event) => events.push(event));

    registry.close();
    assert.deepEqual(registry.lookup("math"), []);

    registry.add("math", "node-a");
    assert.equal(events.length, 0);
  });
});
