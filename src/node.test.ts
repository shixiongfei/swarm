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
import type { MetaData, WatchEvent } from "./registry.ts";

function makeNode(callTimeout = 50) {
  return new Node({ cluster: "test", callTimeout });
}

async function attachGhost(node: Node, service: string) {
  const { port1, port2 } = new MessageChannel();
  const ready = new Promise<WatchEvent>((resolve) =>
    node.watch(service, resolve),
  );

  node.attach(port1);
  port2.start();
  port2.postMessage({
    t: "hello",
    from: "ghost",
    services: [{ name: service }],
  });

  await ready;

  return {
    detach() {
      port2.close();
    },
    close() {
      node.close();
      port1.close();
      port2.close();
    },
  };
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

  test("call uses the node call timeout", async (t) => {
    const node = makeNode(20);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    await assert.rejects(
      node.call("math", "add"),
      (err: MusterError) => err.kind === "timeout",
    );
  });

  test("call accepts a per-call timeout", async (t) => {
    const node = makeNode(5000);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    await assert.rejects(
      node.call("math", "add", [], { timeout: 20 }),
      (err: MusterError) => err.kind === "timeout",
    );
  });

  test("call uses the node call timeout for a local service", async () => {
    const node = makeNode(20);

    node.register("slow", {
      wait: () => new Promise<void>((resolve) => setTimeout(resolve, 40)),
    });

    await assert.rejects(
      node.call("slow", "wait"),
      (err: MusterError) => err.kind === "timeout",
    );
  });

  test("register rejects metadata that cannot be cloned", async () => {
    const node = makeNode();

    assert.throws(
      () => node.register("math", { add: () => 0 }, { build: () => {} }),
      (err: MusterError) =>
        err.kind === "rejected" && err.message === "meta is not cloneable",
    );

    await assert.rejects(
      node.call("math", "add"),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("register rejects metadata that is not a plain object", async () => {
    const node = makeNode();

    assert.throws(
      () =>
        node.register(
          "math",
          { add: () => 0 },
          new Date() as unknown as MetaData,
        ),
      (err: MusterError) =>
        err.kind === "rejected" && err.message === "meta is not a plain object",
    );

    await assert.rejects(
      node.call("math", "add"),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("register stores a snapshot of meta", () => {
    const node = makeNode();
    const meta = { build: 1 };

    node.register("math", { add: () => 0 }, meta);
    meta.build = 2;

    let stored: unknown;

    node.watch("math", (event) => {
      stored = event.instance.meta;
    });

    assert.deepEqual(stored, { build: 1 });
  });

  test("call rejects a service no link serves", async () => {
    const node = makeNode();

    await assert.rejects(
      node.call("nope", "add"),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("routes calls to a local service before a link", async (t) => {
    const node = makeNode(20);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    node.register("math", { add: () => 10 });

    assert.equal(await node.call("math", "add"), 10);
    await assert.rejects(
      node.call("math", "mul"),
      (err: MusterError) => err.kind === "not-found",
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

  test("close is idempotent", () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });

    node.close();

    assert.doesNotThrow(() => node.close());
  });

  test("close emits down for local services", () => {
    const node = makeNode();
    const events: WatchEvent[] = [];

    node.watch("math", (event) => events.push(event));
    node.register("math", { add: () => 0 });

    node.close();

    assert.deepEqual(
      events.map((event) => event.type),
      ["up", "down"],
    );
  });

  test("close rejects pending calls", async (t) => {
    const node = makeNode(5000);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    const call = node.call("math", "add");

    node.close();

    await assert.rejects(
      call,
      (err: MusterError) =>
        err.kind === "failed" && err.message === "node is closed",
    );
  });

  test("operations fail after close", (t) => {
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
      () => node.get("math"),
      (err: MusterError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.attach(port1),
      (err: MusterError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.call("math", "add"),
      (err: MusterError) => err.kind === "rejected",
    );
  });

  test("publishes nothing when the payload cannot be cloned", async (t) => {
    const a = new Node({ cluster: "publish-a" });
    const b = new Node({ cluster: "publish-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.register("math", { add: () => 1 });

    let local = 0;
    let remote = 0;

    a.subscribe("news", () => {
      local += 1;
    });
    b.subscribe("news", () => {
      remote += 1;
    });

    const ready = new Promise<void>((resolve) =>
      b.watch("math", () => resolve()),
    );

    a.attach(port1);
    b.attach(port2);

    await ready;

    assert.throws(
      () => a.publish("news", () => {}),
      (err: MusterError) =>
        err.kind === "failed" && err.message === "data is not cloneable",
    );

    await new Promise<void>((resolve) => setTimeout(resolve, 20));

    assert.equal(local, 0);
    assert.equal(remote, 0);
  });
  test("routes to another link after the first link closes", async (t) => {
    const a = new Node({ cluster: "failover-a" });
    const b = new Node({ cluster: "failover-b" });
    const c = new Node({ cluster: "failover-c" });
    const first = new MessageChannel();
    const second = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      c.close();
      first.port1.close();
      first.port2.close();
      second.port1.close();
      second.port2.close();
    });

    b.register("math", { add: () => 1 });
    c.register("math", { add: () => 2 });

    const ready = new Promise<void>((resolve) => {
      let count = 0;

      a.watch("math", () => {
        count += 1;

        if (count === 2) {
          resolve();
        }
      });
    });

    a.attach(first.port1);
    b.attach(first.port2);
    a.attach(second.port1);
    c.attach(second.port2);

    await ready;

    assert.equal(await a.call("math", "add"), 1);

    const closed = new Promise<void>((resolve) =>
      first.port1.addEventListener("close", () => resolve()),
    );

    b.close();

    await closed;

    assert.equal(await a.call("math", "add"), 2);
  });

  test("keeps an instance while another link to the same node is open", async (t) => {
    const a = new Node({ cluster: "union-a" });
    const b = new Node({ cluster: "union-b" });
    const first = new MessageChannel();
    const second = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      first.port1.close();
      first.port2.close();
      second.port1.close();
      second.port2.close();
    });

    b.register("math", { add: () => 1 });

    const up = new Promise<WatchEvent>((resolve) => a.watch("math", resolve));

    a.attach(first.port1);
    b.attach(first.port2);

    await up;

    a.attach(second.port1);
    b.attach(second.port2);

    const closed = new Promise<void>((resolve) =>
      first.port1.addEventListener("close", () => resolve()),
    );

    first.port2.close();

    await closed;

    const echo = new Promise<WatchEvent>((resolve) => a.watch("echo", resolve));

    b.register("echo", { ping: () => "pong" });

    assert.equal((await echo).instance.nodeId, b.nodeId);
    assert.equal(await a.call("echo", "ping"), "pong");
    assert.equal(await a.call("math", "add"), 1);
  });

  test("watch unsubscribe stops delivery", async (t) => {
    const a = new Node({ cluster: "watch-unsubscribe-a" });
    const b = new Node({ cluster: "watch-unsubscribe-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.register("math", { add: () => 1 });

    const ready = new Promise<void>((resolve) =>
      b.watch("math", () => resolve()),
    );

    a.attach(port1);
    b.attach(port2);

    await ready;

    let observed = 0;

    const unsubscribe = b.watch("echo", () => {
      observed += 1;
    });

    unsubscribe();

    const up = new Promise<void>((resolve) => b.watch("echo", () => resolve()));

    a.register("echo", { ping: () => "pong" });

    await up;

    assert.equal(await b.call("echo", "ping"), "pong");
    assert.equal(observed, 0);
  });
  test("registers a service after attach", { timeout: 1000 }, async (t) => {
    const a = new Node({ cluster: "late-register-a" });
    const b = new Node({ cluster: "late-register-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const up = new Promise<WatchEvent>((resolve) => b.watch("late", resolve));

    a.attach(port1);
    b.attach(port2);

    a.register("late", { ping: () => 1 });

    assert.equal((await up).instance.service, "late");
    assert.equal(await b.call("late", "ping"), 1);
  });

  test("publishes after attach", { timeout: 1000 }, async (t) => {
    const a = new Node({ cluster: "late-publish-a" });
    const b = new Node({ cluster: "late-publish-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const received = new Promise<unknown>((resolve) =>
      b.subscribe("news", resolve),
    );

    a.attach(port1);
    b.attach(port2);

    a.publish("news", 1);

    assert.equal(await received, 1);
  });

});
