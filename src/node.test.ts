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
    nodeId: "ghost",
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

  test("rejects a call whose arguments cannot be cloned", async (t) => {
    const node = makeNode(5000);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    await assert.rejects(
      node.call("math", "add", [() => {}]),
      (err: MusterError) =>
        err.kind === "failed" && err.message === "arguments are not cloneable",
    );
  });

  test("throws when the payload cannot be cloned", async (t) => {
    const node = makeNode(5000);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    let received: unknown;

    node.subscribe("news", (data) => {
      received = data;
    });

    assert.throws(
      () => node.publish("news", () => {}),
      (err: MusterError) =>
        err.kind === "failed" && err.message === "data is not cloneable",
    );

    assert.equal(received, undefined);
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

  test("call rejects a service no link serves", async () => {
    const node = makeNode();

    await assert.rejects(
      node.call("nope", "add"),
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

  test("fails pending calls when the link closes", async (t) => {
    const node = makeNode(5000);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    const call = node.call("math", "add");

    ghost.detach();

    await assert.rejects(
      call,
      (err: MusterError) =>
        err.kind === "failed" && err.message === "link is closed",
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
      () => node.attach(port1),
      (err: MusterError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.call("math", "add"),
      (err: MusterError) => err.kind === "rejected",
    );
  });
});

describe("Node link", () => {
  test("routes calls, errors, topics and unregistrations over a port", async (t) => {
    const a = new Node({ cluster: "link-a", secret: "s3cret" });
    const b = new Node({ cluster: "link-b", secret: "s3cret" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.register("math", { add: (x: number, y: number) => x + y });
    a.register("boom", {
      fail: () => {
        throw new Error("boom");
      },
      crash: () => {
        throw "oops";
      },
    });

    const up = new Promise<WatchEvent>((resolve) => b.watch("math", resolve));
    const down = new Promise<WatchEvent>((resolve) =>
      b.watch("math", (event) => {
        if (event.type === "down") {
          resolve(event);
        }
      }),
    );

    b.attach(port2);
    a.attach(port1);

    assert.equal((await up).instance.nodeId, a.nodeId);
    assert.equal(await b.call("math", "add", [2, 3]), 5);
    await assert.rejects(
      b.call("boom", "fail"),
      (err: MusterError) => err.kind === "failed" && err.message === "boom",
    );
    await assert.rejects(
      b.call("boom", "crash"),
      (err: MusterError) => err.kind === "failed" && err.message === "oops",
    );
    await assert.rejects(
      b.call("math", "nope"),
      (err: MusterError) => err.kind === "not-found",
    );

    const news = new Promise((resolve) => b.subscribe("news", resolve));

    a.publish("news", { ok: true });

    assert.deepEqual(await news, { ok: true });

    a.unregister("math");

    assert.equal((await down).instance.nodeId, a.nodeId);

    const late = new Promise<WatchEvent>((resolve) => b.watch("late", resolve));
    const lateDown = new Promise<WatchEvent>((resolve) =>
      b.watch("late", (event) => {
        if (event.type === "down") {
          resolve(event);
        }
      }),
    );

    a.register("late", { ping: () => "pong" });

    assert.equal((await late).instance.nodeId, a.nodeId);
    assert.equal(await b.call("late", "ping"), "pong");

    a.close();

    assert.equal((await lateDown).instance.nodeId, a.nodeId);
  });

  test("ignores envelopes that do not match the protocol", async (t) => {
    const node = new Node({ cluster: "malformed" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      node.close();
      port1.close();
      port2.close();
    });

    node.attach(port1);
    port2.start();

    port2.postMessage({ t: "hello" });
    port2.postMessage({ t: "up", nodeId: "peer", service: 123 });
    port2.postMessage({ t: "call", id: "x" });
    port2.postMessage({ t: "pub", topic: "news" });

    await assert.rejects(
      node.call("math", "add"),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("closes a link that presents a second nodeId", async (t) => {
    const node = new Node({ cluster: "two-ids" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      node.close();
      port1.close();
      port2.close();
    });

    const up = (service: string) =>
      new Promise<WatchEvent>((resolve) => node.watch(service, resolve));

    node.attach(port1);
    port2.start();

    port2.postMessage({
      t: "hello",
      nodeId: "first",
      services: [{ name: "math" }],
    });

    await up("math");

    const closed = new Promise<void>((resolve) =>
      port2.addEventListener("close", () => resolve()),
    );

    port2.postMessage({
      t: "hello",
      nodeId: "second",
      services: [{ name: "echo" }],
    });

    await closed;

    await assert.rejects(
      node.call("math", "add"),
      (err: MusterError) => err.kind === "not-found",
    );
    await assert.rejects(
      node.call("echo", "ping"),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("reports a reply that cannot be cloned", async (t) => {
    const a = new Node({ cluster: "reply-a" });
    const b = new Node({ cluster: "reply-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.register("ret", { bad: () => () => {} });

    const up = new Promise<WatchEvent>((resolve) => b.watch("ret", resolve));

    a.attach(port1);
    b.attach(port2);

    await up;

    await assert.rejects(
      b.call("ret", "bad"),
      (err: MusterError) =>
        err.kind === "failed" && err.message === "reply is not cloneable",
    );
  });

  test("closes a link whose hello secret does not match", async (t) => {
    const a = new Node({ cluster: "secret-a", secret: "s3cret" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      port1.close();
      port2.close();
    });

    const closed = new Promise<void>((resolve) =>
      port2.addEventListener("close", () => resolve()),
    );

    a.attach(port1);
    port2.start();
    port2.postMessage({
      t: "hello",
      nodeId: "peer-node",
      services: [{ name: "math" }],
    });

    await closed;

    await assert.rejects(
      a.call("math", "add"),
      (err: MusterError) => err.kind === "not-found",
    );
  });

  test("keeps the first link and closes a later link to the same node", async (t) => {
    const a = new Node({ cluster: "dup-a" });
    const b = new Node({ cluster: "dup-b" });
    const first = new MessageChannel();
    const second = new MessageChannel();
    const events: string[] = [];

    t.after(() => {
      a.close();
      b.close();
      first.port1.close();
      first.port2.close();
      second.port1.close();
      second.port2.close();
    });

    a.register("math", { add: () => 1 });

    const up = new Promise<void>((resolve) =>
      b.watch("math", () => {
        events.push("up");
        resolve();
      }),
    );

    a.attach(first.port1);
    b.attach(first.port2);

    await up;

    const closed = new Promise<void>((resolve) =>
      second.port2.addEventListener("close", () => resolve()),
    );

    a.attach(second.port1);
    b.attach(second.port2);

    await closed;

    assert.deepEqual(events, ["up"]);
    assert.equal(await b.call("math", "add"), 1);
  });
});
