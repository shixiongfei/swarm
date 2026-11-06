/*
 * nodelink.test.ts
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
import { Node } from "./node.ts";
import { NodeLink } from "./nodelink.ts";
import { NodeLocal } from "./nodelocal.ts";
import { LinkPort } from "./linkport.ts";
import { SwarmError } from "./errors.ts";
import { signToken } from "./token.ts";
import type { WatchEvent } from "./registry.ts";

describe("NodeLink", () => {
  test("notifies its owner once when the link closes", (t) => {
    const local = new NodeLocal("local-node");
    const { port1, port2 } = new MessageChannel();
    let closed = 0;

    const link = new NodeLink(new LinkPort(port1, "local-node"), {
      provider: local,
    });

    link.events.on("close", () => {
      closed += 1;
    });

    t.after(() => {
      local.close();
      port2.close();
    });

    link.hello();
    link.close();
    link.close();

    assert.equal(closed, 1);
  });

  test("rejects a call after the link closes", async (t) => {
    const local = new NodeLocal("local-node");
    const { port1, port2 } = new MessageChannel();
    const link = new NodeLink(new LinkPort(port1, "local-node"), {
      provider: local,
    });

    t.after(() => {
      local.close();
      port2.close();
    });

    link.close();

    await assert.rejects(
      link.call("peer", "math", "add", [], 50),
      (err: SwarmError) =>
        err.kind === "failed" && err.message === "link is closed",
    );
  });

  test("propagates the handler error kind over a link", async (t) => {
    const a = new Node({ cluster: "kind-a" });
    const b = new Node({ cluster: "kind-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.register("boom", {
      fail: () => {
        throw new SwarmError("rejected", "denied");
      },
    });

    const up = new Promise<WatchEvent>((resolve) => b.watch("boom", resolve));

    b.attach(port2);
    a.attach(port1);

    await up;

    await assert.rejects(
      b.call("boom", "fail"),
      (err: SwarmError) => err.kind === "rejected" && err.message === "denied",
    );
  });

  test("allows a watch while a link is closing", async (t) => {
    const a = new Node({ cluster: "closing-a" });
    const b = new Node({ cluster: "closing-b" });
    const { port1, port2 } = new MessageChannel();
    let error: unknown;

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.register("math", { add: () => 0 });

    const up = new Promise<WatchEvent>((resolve) => b.watch("math", resolve));
    const closing = new Promise<void>((resolve) => {
      b.watch("math", (event) => {
        if (event.type !== "down") {
          return;
        }

        try {
          b.watch("late", () => {});
        } catch (err) {
          error = err;
        }

        resolve();
      });
    });

    b.attach(port2);
    a.attach(port1);

    await up;

    a.close();
    await closing;

    assert.equal(error, undefined);
  });

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

    assert.equal(b.attach(port2), b);
    assert.equal(a.attach(port1), a);

    assert.equal((await up).instance.nodeId, a.nodeId);
    assert.equal(await b.call("math", "add", [2, 3]), 5);
    await assert.rejects(
      b.call("boom", "fail"),
      (err: SwarmError) => err.kind === "failed" && err.message === "boom",
    );
    await assert.rejects(
      b.call("boom", "crash"),
      (err: SwarmError) => err.kind === "failed" && err.message === "oops",
    );
    await assert.rejects(
      b.call("math", "nope"),
      (err: SwarmError) => err.kind === "not-found",
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

    port2.postMessage({ t: "hello", from: "first" });
    port2.postMessage({ t: "up", from: "first", service: "math" });

    await up("math");

    const closed = new Promise<void>((resolve) =>
      port2.addEventListener("close", () => resolve()),
    );

    port2.postMessage({ t: "hello", from: "second" });

    await closed;

    await assert.rejects(
      node.call("math", "add"),
      (err: SwarmError) => err.kind === "not-found",
    );
    await assert.rejects(
      node.call("echo", "ping"),
      (err: SwarmError) => err.kind === "not-found",
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
      (err: SwarmError) =>
        err.kind === "failed" && err.message === "reply is not cloneable",
    );
  });

  test("closes a link whose hello token is invalid", async (t) => {
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
      from: "peer-node",
      token: signToken("wrong-secret", "peer-node"),
    });

    await closed;

    await assert.rejects(
      a.call("math", "add"),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("closes a link whose accept token is missing", async (t) => {
    const node = new Node({ cluster: "accept-secret", secret: "s3cret" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      node.close();
      port1.close();
      port2.close();
    });

    const closed = new Promise<void>((resolve) =>
      port2.addEventListener("close", () => resolve()),
    );

    node.attach(port1);
    port2.start();
    port2.postMessage({
      t: "accept",
      from: "peer-node",
    });

    await closed;

    await assert.rejects(
      node.call("math", "add"),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("uses the link secret over the node secret", async (t) => {
    const a = new Node({ cluster: "link-secret-a", secret: "node-secret" });
    const b = new Node({ cluster: "link-secret-b", secret: "link-secret" });
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

    a.attach(port1, "link-secret");
    b.attach(port2);

    await ready;

    assert.equal(await b.call("math", "add"), 1);
  });

  test("ignores messages that spoof another sender", async (t) => {
    const node = new Node({ cluster: "spoof-sender" });
    const { port1, port2 } = new MessageChannel();
    const received: unknown[] = [];
    const up = new Promise<WatchEvent>((resolve) =>
      node.watch("math", resolve),
    );
    const second = new Promise<void>((resolve) =>
      node.subscribe("news", (data) => {
        received.push(data);

        if (data === 2) {
          resolve();
        }
      }),
    );

    t.after(() => {
      node.close();
      port1.close();
      port2.close();
    });

    node.attach(port1);
    port2.start();
    port2.postMessage({ t: "hello", from: "peer" });
    port2.postMessage({ t: "up", from: "peer", service: "math" });

    await up;

    port2.postMessage({ t: "up", from: "spoof", service: "fake" });
    port2.postMessage({ t: "pub", from: "spoof", topic: "news", data: 1 });
    port2.postMessage({ t: "pub", from: "peer", topic: "news", data: 2 });

    await second;

    assert.deepEqual(received, [2]);
    await assert.rejects(
      node.call("fake", "x"),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("ignores a call addressed to another node", async (t) => {
    const node = new Node({ cluster: "call-target" });
    const { port1, port2 } = new MessageChannel();
    let calls = 0;

    node.register("math", {
      add: () => {
        calls += 1;
        return 7;
      },
    });

    const up = new Promise<WatchEvent>((resolve) =>
      node.watch("remote", resolve),
    );

    t.after(() => {
      node.close();
      port1.close();
      port2.close();
    });

    node.attach(port1);
    port2.start();
    port2.postMessage({ t: "hello", from: "peer" });
    port2.postMessage({ t: "up", from: "peer", service: "remote" });

    await up;

    const reply = new Promise<{ id: string; value: unknown }>((resolve) =>
      port2.addEventListener("message", (event) => {
        if (event.data !== null && event.data.t === "reply") {
          resolve(event.data);
        }
      }),
    );

    port2.postMessage({
      t: "call",
      id: "wrong",
      from: "peer",
      to: "other",
      service: "math",
      method: "add",
      args: [1, 2],
    });
    port2.postMessage({
      t: "call",
      id: "right",
      from: "peer",
      to: node.nodeId,
      service: "math",
      method: "add",
      args: [1, 2],
    });

    const message = await reply;

    assert.equal(message.id, "right");
    assert.equal(message.value, 7);
    assert.equal(calls, 1);
  });
  test("exposes the peer id and follows service changes", async (t) => {
    const local = new NodeLocal("local-node");
    const { port1, port2 } = new MessageChannel();
    const link = new NodeLink(new LinkPort(port1, "local-node"), {
      provider: local,
    });

    t.after(() => {
      link.close();
      local.close();
      port2.close();
    });

    assert.equal(link.peerId, undefined);

    const events: WatchEvent[] = [];
    const up = new Promise<WatchEvent>((resolve) => {
      link.watch((event) => {
        events.push(event);

        if (event.type === "up") {
          resolve(event);
        }
      });
    });

    port2.start();
    port2.postMessage({ t: "hello", from: "peer-node" });
    port2.postMessage({ t: "up", from: "peer-node", service: "math" });

    assert.deepEqual((await up).instance, {
      service: "math",
      nodeId: "peer-node",
    });
    assert.equal(link.peerId, "peer-node");

    const replay: WatchEvent[] = [];
    link.watch((event) => replay.push(event));

    assert.deepEqual(replay, [
      { type: "up", instance: { service: "math", nodeId: "peer-node" } },
    ]);

    const down = new Promise<WatchEvent>((resolve) => {
      link.watch((event) => {
        if (event.type === "down") {
          resolve(event);
        }
      });
    });

    port2.postMessage({ t: "down", from: "peer-node", service: "math" });

    assert.deepEqual((await down).instance, {
      service: "math",
      nodeId: "peer-node",
    });
    assert.deepEqual(
      events.map((event) => event.type),
      ["up", "down"],
    );
  });

  test("rejects a call whose arguments cannot be cloned", async (t) => {
    const local = new NodeLocal("local-node");
    const { port1, port2 } = new MessageChannel();
    const link = new NodeLink(new LinkPort(port1, "local-node"), {
      provider: local,
    });

    t.after(() => {
      link.close();
      local.close();
      port2.close();
    });

    await assert.rejects(
      link.call("peer-node", "math", "add", [() => {}], 50),
      (err: SwarmError) =>
        err.kind === "failed" && err.message === "arguments are not cloneable",
    );
  });

  test("fails pending calls when the peer closes", async (t) => {
    const local = new NodeLocal("local-node");
    const { port1, port2 } = new MessageChannel();
    const link = new NodeLink(new LinkPort(port1, "local-node"), {
      provider: local,
    });

    t.after(() => {
      link.close();
      local.close();
      port2.close();
    });

    const call = link.call("peer-node", "math", "add", [], 5000);

    port2.close();

    await assert.rejects(
      call,
      (err: SwarmError) =>
        err.kind === "failed" && err.message === "link is closed",
    );
  });
});
