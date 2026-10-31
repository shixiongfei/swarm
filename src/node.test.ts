/*
 * node.test.ts
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
import { Node } from "./node.ts";
import type { MusterError } from "./errors.ts";
import type { WatchEvent } from "./registry.ts";

function makeNode(callTimeout = 50): Node {
  return new Node({ cluster: "test", callTimeout });
}

describe("Node", () => {
  test("register returns the node and publishes an instance", () => {
    const node = makeNode();
    const result = node.register("math", {
      add: (a: number, b: number) => a + b,
    });

    assert.equal(result, node);
    assert.deepEqual(node.instances("math"), [
      { service: "math", owner: node.nodeId },
    ]);
  });

  test("register keeps meta", () => {
    const node = makeNode();
    node.register("math", { add: () => 0 }, { build: 1 });

    assert.deepEqual(node.instances("math")[0].meta, { build: 1 });
  });

  test("register rejects a duplicate service", () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });

    assert.throws(
      () => node.register("math", {}),
      (err: MusterError) => err.kind === "rejected",
    );
  });

  test("unregister drops the service and emits down", async () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });
    const events: WatchEvent[] = [];
    const off = node.watch("math", (event) => events.push(event));

    const result = node.unregister("math");

    assert.equal(result, node);
    assert.deepEqual(events[1], {
      type: "down",
      instance: { service: "math", owner: node.nodeId },
    });
    assert.deepEqual(node.instances("math"), []);
    await assert.rejects(
      node.call("math", "add"),
      (err: MusterError) => err.kind === "not-found",
    );

    off();
    assert.doesNotThrow(() => node.register("math", { add: () => 0 }));
  });

  test("unregister unknown services does nothing", () => {
    const node = makeNode();

    assert.doesNotThrow(() => node.unregister("nope"));
  });

  test("call returns the handler result", async () => {
    const node = makeNode();
    node.register("math", { add: (a: number, b: number) => a + b });

    assert.equal(await node.call("math", "add", [2, 3]), 5);
  });

  test("call passes an empty argument list by default", async () => {
    const node = makeNode();
    let received: unknown[] | undefined;
    node.register("echo", {
      count: (...args: unknown[]) => {
        received = args;
        return args.length;
      },
    });

    assert.equal(await node.call("echo", "count"), 0);
    assert.deepEqual(received, []);
  });

  test("call awaits an async handler", async () => {
    const node = makeNode();
    node.register("math", { add: async (a: number, b: number) => a + b });

    assert.equal(await node.call("math", "add", [2, 3]), 5);
  });

  test("call rejects unknown services and methods", async () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });

    await assert.rejects(
      node.call("nope", "add"),
      (err: MusterError) => err.kind === "not-found",
    );
    await assert.rejects(
      node.call("math", "nope"),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("inherited object properties are not callable methods", async () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });

    await assert.rejects(
      node.call("math", "toString"),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("call rejects with timeout when the handler never settles", async () => {
    const node = makeNode(20);
    node.register("slow", { wait: () => new Promise(() => {}) });

    await assert.rejects(
      node.call("slow", "wait"),
      (err: MusterError) => err.kind === "timeout",
    );
  });

  test("call accepts a per-call timeout", async () => {
    const node = makeNode(5000);
    node.register("slow", { wait: () => new Promise(() => {}) });

    await assert.rejects(
      node.call("slow", "wait", [], { timeout: 20 }),
      (err: MusterError) => err.kind === "timeout",
    );
  });

  test("handler errors reach the caller unchanged", async () => {
    const node = makeNode();
    const failure = new Error("boom");
    node.register("boom", {
      fail: () => {
        throw failure;
      },
    });

    await assert.rejects(
      node.call("boom", "fail"),
      (err: Error) => err === failure,
    );
  });

  test("async handler rejection reaches the caller", async () => {
    const node = makeNode();
    node.register("boom", {
      fail: async () => {
        throw new Error("boom");
      },
    });

    await assert.rejects(node.call("boom", "fail"), /boom/);
  });

  test("get returns a proxy that calls the service", async () => {
    const node = makeNode();
    node.register("math", { add: (a: number, b: number) => a + b });
    const proxy = node.get("math");

    assert.equal(await proxy.add(2, 3), 5);
    assert.equal(await proxy, proxy);
    assert.equal(Reflect.get(proxy, Symbol.toStringTag), undefined);
  });

  test("watch delivers the snapshot", () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });
    const events: WatchEvent[] = [];

    const off = node.watch("math", (event) => events.push(event));

    assert.deepEqual(events, [
      { type: "up", instance: { service: "math", owner: node.nodeId } },
    ]);
    off();
  });

  test("publish delivers data and the node id", () => {
    const node = makeNode();
    const received: Array<[unknown, string]> = [];

    const off = node.subscribe("news", (data, from) =>
      received.push([data, from]),
    );
    node.publish("news", { ok: true });

    assert.deepEqual(received, [[{ ok: true }, node.nodeId]]);

    off();
    node.publish("news", { ok: false });
    assert.equal(received.length, 1);
  });

  test("a throwing subscriber does not break delivery", () => {
    const node = makeNode();
    const received: unknown[] = [];

    node.subscribe("news", () => {
      throw new Error("boom");
    });
    node.subscribe("news", (data) => received.push(data));
    node.publish("news", 1);

    assert.deepEqual(received, [1]);
  });

  test("publish without subscribers is a no-op", () => {
    const node = makeNode();

    assert.doesNotThrow(() => node.publish("nobody", 1));
  });

  test("close clears instances and is idempotent", () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });

    node.close();

    assert.deepEqual(node.instances("math"), []);
    assert.doesNotThrow(() => node.close());
  });

  test("operations fail after close", async () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });
    node.close();

    assert.throws(
      () => node.register("math", {}),
      (err: MusterError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.unregister("math"),
      (err: MusterError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.watch("math", () => {}),
      (err: MusterError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.subscribe("news", () => {}),
      (err: MusterError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.publish("news", 1),
      (err: MusterError) => err.kind === "rejected",
    );
    await assert.rejects(
      node.call("math", "add"),
      (err: MusterError) => err.kind === "rejected",
    );
  });
});
