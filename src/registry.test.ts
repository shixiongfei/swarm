/*
 * registry.test.ts
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
import { Registry } from "./registry.ts";
import type { SwarmError } from "./errors.ts";
import type { WatchEvent } from "./registry.ts";

describe("Registry", () => {
  test("add stores an instance", () => {
    const registry = new Registry();
    const instance = registry.add("math", "node-a");

    assert.deepEqual(instance, { service: "math", nodeId: "node-a" });
    assert.deepEqual(registry.lookup("math"), [instance]);
  });

  test("add keeps meta when given", () => {
    const registry = new Registry();
    const instance = registry.add("math", "node-a", { build: 1 });

    assert.deepEqual(instance.meta, { build: 1 });
  });

  test("add appends multiple nodes for the same service", () => {
    const registry = new Registry();

    registry.add("math", "node-a");
    registry.add("math", "node-b");

    assert.deepEqual(
      registry.lookup("math").map((item) => item.nodeId),
      ["node-a", "node-b"],
    );
  });

  test("add ignores a duplicate service and nodeId", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch("math", (event) => events.push(event));

    const instance = registry.add("math", "node-a");

    assert.equal(registry.add("math", "node-a", { build: 2 }), instance);
    assert.deepEqual(registry.lookup("math"), [instance]);
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

  test("remove unknown service or node does nothing", () => {
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

  test("remove only drops the matching node", () => {
    const registry = new Registry();
    registry.add("math", "node-a");
    registry.add("math", "node-b");

    registry.remove("math", "node-a");

    assert.deepEqual(
      registry.lookup("math").map((item) => item.nodeId),
      ["node-b"],
    );
  });

  test("lookup returns a copy", () => {
    const registry = new Registry();
    registry.add("math", "node-a");

    registry.lookup("math").pop();

    assert.equal(registry.lookup("math").length, 1);
  });

  test("lookup of an unknown service is empty", () => {
    const registry = new Registry();

    assert.deepEqual(registry.lookup("nope"), []);
  });

  test("has matches by service or by node", () => {
    const registry = new Registry();
    registry.add("math", "node-a");

    assert.equal(registry.has("math"), true);
    assert.equal(registry.has("math", "node-a"), true);
    assert.equal(registry.has("math", "node-b"), false);
    assert.equal(registry.has("nope"), false);
  });

  test("snapshot returns every instance in insertion order", () => {
    const registry = new Registry();

    registry.add("math", "node-a");
    registry.add("echo", "node-b");

    assert.deepEqual(registry.snapshot(), [
      { service: "math", nodeId: "node-a" },
      { service: "echo", nodeId: "node-b" },
    ]);
  });

  test("registrations filters by nodeId", () => {
    const registry = new Registry();

    registry.add("math", "node-a");
    registry.add("echo", "node-b");
    registry.add("text", "node-a", { build: 1 });

    assert.deepEqual(registry.registrations("node-a"), [
      { service: "math", nodeId: "node-a" },
      { service: "text", nodeId: "node-a", meta: { build: 1 } },
    ]);
    assert.deepEqual(registry.registrations("nope"), []);
  });

  test("drop removes every instance of one node", () => {
    const registry = new Registry();

    registry.add("math", "node-a");
    registry.add("math", "node-b");
    registry.add("echo", "node-a");

    registry.drop("node-a");

    assert.deepEqual(
      registry.lookup("math").map((item) => item.nodeId),
      ["node-b"],
    );
    assert.deepEqual(registry.lookup("echo"), []);
  });

  test("drop emits down for each removed instance", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch("math", (event) => events.push(event));

    const a = registry.add("math", "node-a");
    const b = registry.add("math", "node-b");
    registry.drop("node-a");

    assert.deepEqual(events, [
      { type: "up", instance: a },
      { type: "up", instance: b },
      { type: "down", instance: a },
    ]);
  });

  test("unsubscribe stops delivery and is idempotent", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    const unsubscribe = registry.watch("math", (event) => events.push(event));
    registry.add("math", "node-a");
    const before = events.length;

    unsubscribe();
    unsubscribe();
    registry.add("math", "node-b");

    assert.equal(events.length, before);
  });

  test("unsubscribe only removes its own registration", () => {
    const registry = new Registry();
    const first: WatchEvent[] = [];
    const second: WatchEvent[] = [];
    const unsubscribeFirst = registry.watch("math", (event) =>
      first.push(event),
    );
    registry.watch("math", (event) => second.push(event));

    unsubscribeFirst();
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

  test("global watch delivers the snapshot before increments", () => {
    const registry = new Registry();
    const a = registry.add("math", "node-a");
    const b = registry.add("echo", "node-b");
    const events: WatchEvent[] = [];

    registry.watch((event) => events.push(event));
    const c = registry.add("text", "node-c");

    assert.deepEqual(events, [
      { type: "up", instance: a },
      { type: "up", instance: b },
      { type: "up", instance: c },
    ]);
  });

  test("global watch receives down events", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    const instance = registry.add("math", "node-a");

    registry.watch((event) => events.push(event));
    registry.remove("math", "node-a");

    assert.deepEqual(events, [
      { type: "up", instance },
      { type: "down", instance },
    ]);
  });

  test("global watch unsubscribe stops delivery", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    const unsubscribe = registry.watch((event) => events.push(event));

    registry.add("math", "node-a");
    const before = events.length;

    unsubscribe();
    unsubscribe();
    registry.add("math", "node-b");

    assert.equal(events.length, before);
  });

  test("global watch receives down for every instance on close", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch((event) => events.push(event));

    const a = registry.add("math", "node-a");
    const b = registry.add("echo", "node-b");

    registry.close();

    assert.deepEqual(events, [
      { type: "up", instance: a },
      { type: "up", instance: b },
      { type: "down", instance: a },
      { type: "down", instance: b },
    ]);
  });

  test("a throwing global watcher does not break add or other watchers", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch(() => {
      throw new Error("boom");
    });
    registry.watch((event) => events.push(event));

    const instance = registry.add("math", "node-a");

    assert.deepEqual(registry.lookup("math"), [instance]);
    assert.deepEqual(events, [{ type: "up", instance }]);
  });

  test("a throwing global watcher does not break the snapshot", () => {
    const registry = new Registry();
    registry.add("math", "node-a");

    assert.doesNotThrow(() => {
      registry.watch(() => {
        throw new Error("boom");
      });
    });
  });

  test("global and service watchers both receive events", () => {
    const registry = new Registry();
    const globalEvents: WatchEvent[] = [];
    const serviceEvents: WatchEvent[] = [];

    registry.watch((event) => globalEvents.push(event));
    registry.watch("math", (event) => serviceEvents.push(event));

    const instance = registry.add("math", "node-a");
    registry.remove("math", "node-a");

    assert.deepEqual(globalEvents, [
      { type: "up", instance },
      { type: "down", instance },
    ]);
    assert.deepEqual(serviceEvents, [
      { type: "up", instance },
      { type: "down", instance },
    ]);
  });

  test("close emits down for every instance", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch("math", (event) => events.push(event));
    registry.watch("echo", (event) => events.push(event));

    const a = registry.add("math", "node-a");
    const b = registry.add("echo", "node-b");

    registry.close();
    registry.close();

    assert.deepEqual(events, [
      { type: "up", instance: a },
      { type: "up", instance: b },
      { type: "down", instance: a },
      { type: "down", instance: b },
    ]);

    assert.throws(
      () => registry.add("math", "node-c"),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.equal(events.length, 4);
  });

  test("close clears the table and removes listeners", () => {
    const registry = new Registry();
    const events: WatchEvent[] = [];
    registry.watch("math", (event) => events.push(event));

    registry.close();
    assert.deepEqual(registry.lookup("math"), []);
    assert.deepEqual(registry.snapshot(), []);
    assert.deepEqual(registry.registrations("node-a"), []);

    assert.throws(
      () => registry.add("math", "node-a"),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => registry.remove("math", "node-a"),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => registry.drop("node-a"),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => registry.watch("math", () => {}),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.equal(events.length, 0);
  });

  test("close rejects re-entry from a down watcher", () => {
    const registry = new Registry();
    let error: SwarmError | undefined;

    registry.watch("math", (event) => {
      if (event.type !== "down") {
        return;
      }

      try {
        registry.add("math", "node-b");
      } catch (err) {
        error = err as SwarmError;
      }
    });

    registry.add("math", "node-a");
    registry.close();

    assert.equal(error !== undefined && error.kind === "rejected", true);
    assert.deepEqual(registry.lookup("math"), []);
  });
});
