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
import { MessageChannel } from "node:worker_threads";
import { Node } from "./node.ts";
import type { MusterError } from "./errors.ts";

function makeNode(callTimeout = 50) {
  return new Node({ cluster: "test", callTimeout });
}

describe("Node", () => {
  test("register and unregister return the node", () => {
    const node = makeNode();

    assert.equal(node.register("math", { add: () => 0 }), node);
    assert.equal(node.unregister("math"), node);
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

  test("call uses the node call timeout", async () => {
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

  test("get returns a proxy that calls the service", async () => {
    type MathApi = { add(a: number, b: number): number };

    const node = makeNode();
    node.register("math", { add: (a: number, b: number) => a + b });
    const math = node.get<MathApi>("math");

    assert.equal(await math.add(2, 3), 5);
    assert.equal(await math, math);
    assert.equal(Reflect.get(math, Symbol.toStringTag), undefined);
  });

  test("close clears instances and is idempotent", () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });

    node.close();

    assert.deepEqual(node.instances("math"), []);
    assert.doesNotThrow(() => node.close());
  });

  test("operations fail after close", async (t) => {
    const node = makeNode();
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      port1.close();
      port2.close();
    });

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
    assert.throws(
      () => node.attach(port1),
      (err: MusterError) => err.kind === "rejected",
    );
    await assert.rejects(
      node.call("math", "add"),
      (err: MusterError) => err.kind === "rejected",
    );
  });
});
