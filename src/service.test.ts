/*
 * service.test.ts
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
import { Service } from "./service.ts";
import type { MusterError } from "./errors.ts";

function makeService(nodeId: string) {
  return new Service({ nodeId });
}

function flush(delay = 10) {
  return new Promise((resolve) => setTimeout(resolve, delay));
}

function makePair() {
  const { port1, port2 } = new MessageChannel();
  const first = makeService("node-a");
  const second = makeService("node-b");

  first.attach(port1);
  second.attach(port2);

  return { first, second, port1, port2 };
}

function closePair(pair: ReturnType<typeof makePair>) {
  pair.first.close();
  pair.second.close();
  pair.port1.close();
  pair.port2.close();
}

function makeStar() {
  const hub = makeService("hub");
  const left = makeService("left");
  const right = makeService("right");
  const a = new MessageChannel();
  const b = new MessageChannel();

  hub.attach(a.port1);
  left.attach(a.port2);
  hub.attach(b.port1);
  right.attach(b.port2);

  return { hub, left, right, a, b };
}

function closeStar(star: ReturnType<typeof makeStar>) {
  star.hub.close();
  star.left.close();
  star.right.close();
  star.a.port1.close();
  star.a.port2.close();
  star.b.port1.close();
  star.b.port2.close();
}

function makeCycle() {
  const a = makeService("a");
  const b = makeService("b");
  const c = makeService("c");
  const ab = new MessageChannel();
  const bc = new MessageChannel();
  const ca = new MessageChannel();

  a.attach(ab.port1);
  b.attach(ab.port2);
  b.attach(bc.port1);
  c.attach(bc.port2);
  c.attach(ca.port1);
  a.attach(ca.port2);

  return { a, b, c, ab, bc, ca };
}

function closeCycle(cycle: ReturnType<typeof makeCycle>) {
  cycle.a.close();
  cycle.b.close();
  cycle.c.close();
  cycle.ab.port1.close();
  cycle.ab.port2.close();
  cycle.bc.port1.close();
  cycle.bc.port2.close();
  cycle.ca.port1.close();
  cycle.ca.port2.close();
}

describe("Service", () => {
  test("attach returns a detach that closes the link", async (t) => {
    const service = makeService("node-a");
    const { port1, port2 } = new MessageChannel();
    let closed = 0;

    t.after(() => {
      service.close();
      port1.close();
      port2.close();
    });

    port2.on("close", () => closed++);
    const detach = service.attach(port1);

    detach();
    await flush();

    assert.equal(closed, 1);
  });

  test("close closes the attached links", async (t) => {
    const service = makeService("node-a");
    const { port1, port2 } = new MessageChannel();
    let closed = 0;

    t.after(() => {
      port1.close();
      port2.close();
    });

    port2.on("close", () => closed++);
    service.attach(port1);

    service.close();
    await flush();

    assert.equal(closed, 1);
  });

  test("register reaches the connected service", async (t) => {
    const pair = makePair();
    t.after(() => closePair(pair));

    pair.first.register("math", { add: () => 0 });
    await flush();

    assert.deepEqual(pair.second.lookup("math"), [
      { service: "math", owner: "node-a" },
    ]);
  });

  test("unregister reaches the connected service", async (t) => {
    const pair = makePair();
    t.after(() => closePair(pair));

    pair.first.register("math", { add: () => 0 });
    await flush();
    pair.first.unregister("math");
    await flush();

    assert.deepEqual(pair.second.lookup("math"), []);
  });

  test("unregistering one service keeps the owner's other services", async (t) => {
    const pair = makePair();
    t.after(() => closePair(pair));

    pair.first.register("math", { add: () => 1 });
    pair.first.register("echo", { one: (value: unknown) => value });
    await flush();

    pair.first.unregister("math");
    await flush();

    assert.deepEqual(pair.second.lookup("math"), []);
    assert.equal(await pair.second.invoke("echo", "one", ["hi"], 100), "hi");
  });

  test("duplicate registration mirrors a single instance", async (t) => {
    const pair = makePair();
    t.after(() => closePair(pair));

    pair.first.register("math", { add: () => 0 });
    await flush();

    pair.port1.postMessage({ t: "reg", nodeId: "node-a", service: "math" });
    await flush();

    assert.equal(pair.second.lookup("math").length, 1);
  });

  test("invoke routes to the owner in both directions", async (t) => {
    const pair = makePair();
    t.after(() => closePair(pair));

    pair.first.register("math", { add: (a: number, b: number) => a + b });
    pair.second.register("echo", { one: (value: unknown) => value });
    await flush();

    assert.equal(await pair.second.invoke("math", "add", [2, 3], 100), 5);
    assert.equal(await pair.first.invoke("echo", "one", ["hi"], 100), "hi");
  });

  test("remote handler errors surface as failed", async (t) => {
    const pair = makePair();
    t.after(() => closePair(pair));

    pair.first.register("boom", {
      fail: () => {
        throw new Error("boom");
      },
    });
    await flush();

    await assert.rejects(
      pair.second.invoke("boom", "fail", [], 100),
      (err: MusterError) => err.kind === "failed" && err.message === "boom",
    );
  });

  test("unknown remote services are not found", async (t) => {
    const pair = makePair();
    t.after(() => closePair(pair));

    await assert.rejects(
      pair.second.invoke("nope", "run", [], 100),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("remote timeouts use the call label", async (t) => {
    const pair = makePair();
    t.after(() => closePair(pair));

    pair.first.register("slow", { wait: () => new Promise(() => {}) });
    await flush();

    await assert.rejects(
      pair.second.invoke("slow", "wait", [], 20),
      (err: MusterError) =>
        err.kind === "timeout" &&
        err.message === "call timed out: slow.wait",
    );
  });

  test("an incoming call uses the timeout from the envelope", async (t) => {
    const service = makeService("node-a");
    const { port1, port2 } = new MessageChannel();
    const replies: unknown[] = [];

    t.after(() => {
      service.close();
      port1.close();
      port2.close();
    });

    service.register("slow", { wait: () => new Promise(() => {}) });
    port2.on("message", (message) => replies.push(message));
    service.attach(port1);

    port2.postMessage({
      t: "call",
      id: "c1",
      service: "slow",
      method: "wait",
      args: [],
      timeout: 20,
    });
    await flush(50);

    assert.deepEqual(replies, [
      {
        t: "error",
        id: "c1",
        kind: "timeout",
        message: "call timed out: slow.wait",
      },
    ]);
  });

  test("closing a service drops its instances", async (t) => {
    const pair = makePair();
    t.after(() => pair.second.close());

    pair.first.register("math", { add: () => 0 });
    await flush();

    pair.first.close();
    await flush();

    assert.deepEqual(pair.second.lookup("math"), []);
  });

  test("a pending call fails when the peer closes", async (t) => {
    const pair = makePair();
    t.after(() => pair.second.close());

    pair.first.register("slow", { wait: () => new Promise(() => {}) });
    await flush();

    const pending = pair.second.invoke("slow", "wait", [], 500);
    pair.first.close();

    await assert.rejects(pending, (err: MusterError) => err.kind === "failed");
  });

  test("calls are forwarded by a service with several links", async (t) => {
    const star = makeStar();
    t.after(() => closeStar(star));

    star.right.register("math", { add: (x: number, y: number) => x + y });
    await flush();

    assert.equal(await star.left.invoke("math", "add", [2, 3], 100), 5);
  });

  test("publish reaches the connected service with the original sender", async (t) => {
    const pair = makePair();
    t.after(() => closePair(pair));

    const firstEvents: Array<[unknown, string]> = [];
    const secondEvents: Array<[unknown, string]> = [];

    pair.first.subscribe("news", (data, from) =>
      firstEvents.push([data, from]),
    );
    pair.second.subscribe("news", (data, from) =>
      secondEvents.push([data, from]),
    );

    pair.first.publish("news", { ok: true });
    await flush();

    assert.deepEqual(firstEvents, [[{ ok: true }, "node-a"]]);
    assert.deepEqual(secondEvents, [[{ ok: true }, "node-a"]]);
  });

  test("publish is forwarded across several links", async (t) => {
    const star = makeStar();
    t.after(() => closeStar(star));

    const leftEvents: unknown[] = [];
    const rightEvents: unknown[] = [];

    star.left.subscribe("news", (data) => leftEvents.push(data));
    star.right.subscribe("news", (data) => rightEvents.push(data));

    star.left.publish("news", 7);
    await flush(20);

    assert.deepEqual(leftEvents, [7]);
    assert.deepEqual(rightEvents, [7]);
  });

  test("publish stops circulating in a cycle", async (t) => {
    const cycle = makeCycle();
    t.after(() => closeCycle(cycle));

    const aEvents: unknown[] = [];
    const bEvents: unknown[] = [];
    const cEvents: unknown[] = [];

    cycle.a.subscribe("news", (data) => aEvents.push(data));
    cycle.b.subscribe("news", (data) => bEvents.push(data));
    cycle.c.subscribe("news", (data) => cEvents.push(data));

    cycle.a.publish("news", 1);
    await flush(30);

    assert.deepEqual(aEvents, [1]);
    assert.deepEqual(bEvents, [1]);
    assert.deepEqual(cEvents, [1]);
  });

  test("unsubscribe stops remote delivery", async (t) => {
    const pair = makePair();
    t.after(() => closePair(pair));

    const events: unknown[] = [];
    const off = pair.second.subscribe("news", (data) => events.push(data));

    off();
    pair.first.publish("news", 1);
    await flush();

    assert.deepEqual(events, []);
  });
});
