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
import { describe, mock, test } from "node:test";
import { Node } from "./node.ts";
import { NodeLink } from "./nodelink.ts";
import { NodeLocal } from "./nodelocal.ts";
import { Link } from "./link.ts";
import { changesFrame, pubsFrame } from "./frame.ts";
import { LinkPort } from "./linkport.ts";
import { SwarmError } from "./errors.ts";
import { protocolVersion } from "./envelope.ts";
import { signToken } from "./token.ts";
import type { Frame } from "./frame.ts";
import type { Envelope } from "./envelope.ts";
import type { WatchEvent } from "./registry.ts";

class StubLink extends Link {
  readonly transport = "socket";
  readonly local = true;
  readonly sent: Envelope[] = [];
  closed = false;
  failNextSend = false;
  failSendType: Envelope["t"] | undefined;
  nextError: Error | undefined;

  constructor() {
    super("stub-node");
  }

  send(frame: Frame) {
    const message = frame.message;

    if (this.failNextSend || this.failSendType === message.t) {
      this.failNextSend = false;
      throw this.nextError !== undefined ? this.nextError : new Error("boom");
    }

    this.sent.push(message);
  }

  close() {
    this.closed = true;
  }
}

function attachStub(local: NodeLocal, maxInboundTimeout?: number) {
  const link = new StubLink();
  const nodeLink = new NodeLink(link, {
    provider: local,
    cluster: "default",
    passive: false,
    startup: 1,
    secret: "",
    heartbeatInterval: 15000,
    heartbeatTimeout: 45000,
    maxInboundTimeout,
  });

  link.events.emit("envelope", {
    t: "hello",
    passive: false,
    cluster: "default",
    from: "peer-node",
    version: protocolVersion,
    startup: 1,
    token: signToken("", "peer-node"),
  });

  return { link, nodeLink };
}

describe("handshake helpers", () => {
  test("rejects a peer from another cluster", () => {
    const local = new NodeLocal("local-node");
    const link = new StubLink();
    const nodeLink = new NodeLink(link, {
      provider: local,
      cluster: "local-cluster",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    link.events.emit("envelope", {
      t: "hello",
      passive: false,
      cluster: "peer-cluster",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    assert.equal(nodeLink.closeCode, "CLUSTER_MISMATCH");
    assert.equal(link.closed, true);
    nodeLink.close();
  });

  test("carries the passive flag through the handshake", () => {
    const local = new NodeLocal("local-node");
    const link = new StubLink();
    const nodeLink = new NodeLink(link, {
      provider: local,
      cluster: "default",
      passive: true,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    link.events.emit("envelope", {
      t: "hello",
      passive: true,
      cluster: "default",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    const reply = link.sent.find((message) => message.t === "accept");

    assert.equal(nodeLink.peerPassive, true);
    assert.equal(reply?.t === "accept" ? reply.passive : undefined, true);

    nodeLink.close();
  });

  test("disposes the link when the handshake reply fails", () => {
    const local = new NodeLocal("local-node");
    const link = new StubLink();
    const nodeLink = new NodeLink(link, {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
      handshakeTimeout: 50,
    });
    let closes = 0;

    nodeLink.events.on("close", () => (closes += 1));

    link.failNextSend = true;
    link.events.emit("envelope", {
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    assert.equal(closes, 1);
    assert.equal(link.closed, true);
    assert.equal(nodeLink.closeCode, "HANDSHAKE_FAILED");
  });

  test("disposes the link when the handshake announcement fails", () => {
    const local = new NodeLocal("local-node");
    const link = new StubLink();
    const nodeLink = new NodeLink(link, {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
      handshakeTimeout: 50,
    });
    let closes = 0;

    local.register("math", { run: () => 1 });
    nodeLink.events.on("close", () => (closes += 1));

    link.failSendType = "changes";
    link.nextError = new SwarmError(
      "failed",
      "CONGESTED",
      "link is congested",
      true,
    );

    link.events.emit("envelope", {
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    assert.equal(closes, 1);
    assert.equal(link.closed, true);
    assert.equal(nodeLink.closeCode, "HANDSHAKE_FAILED");
  });

  test("announces a large registration set in several frames", () => {
    const local = new NodeLocal("local-node");

    for (let i = 0; i < 9; i += 1) {
      local.register(`svc-${i}`, {}, { blob: "x".repeat(3 * 1024 * 1024) });
    }

    const { link, nodeLink } = attachStub(local);
    const frames = link.sent.filter((message) => message.t === "changes");
    const items = frames.flatMap((message) =>
      message.t === "changes" ? message.items : [],
    );

    assert.ok(frames.length > 1);
    assert.equal(items.length, 9);
    assert.equal(
      frames.every(
        (message) => message.t === "changes" && message.items.length > 0,
      ),
      true,
    );

    nodeLink.close();
    local.close();
  });

  test("does not leak an unhandled rejection when a failure reply is congested", async (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local);
    const unhandled: unknown[] = [];
    const onUnhandled = (error: unknown) => unhandled.push(error);

    t.after(() => {
      process.off("unhandledRejection", onUnhandled);
      nodeLink.close();
      local.close();
    });

    process.on("unhandledRejection", onUnhandled);

    link.failNextSend = true;
    link.nextError = new SwarmError(
      "failed",
      "CONGESTED",
      "link is congested",
      true,
    );

    link.events.emit("envelope", {
      t: "call",
      id: "call-1",
      from: "peer-node",
      to: "stub-node",
      service: "missing",
      method: "run",
      args: [],
    });

    await new Promise<void>((resolve) => setTimeout(resolve, 20));

    assert.deepEqual(unhandled, []);
  });

  test("reports a reply that cannot be encoded", async (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local);

    local.register("echo", { run: () => 1 });

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    link.failNextSend = true;
    link.nextError = new SwarmError(
      "failed",
      "ENVELOPE_INVALID",
      "envelope is not encodable",
      false,
    );

    link.events.emit("envelope", {
      t: "call",
      id: "call-2",
      from: "peer-node",
      to: "stub-node",
      service: "echo",
      method: "run",
      args: [],
    });

    await new Promise<void>((resolve) => setTimeout(resolve, 20));

    const error = link.sent.find((message) => message.t === "error");

    assert.equal(
      error !== undefined && error.t === "error" ? error.code : undefined,
      "CLONE_FAILED",
    );
  });

  test("clamps the inbound timeout to the link configuration", async (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local, 20);

    local.register("slow", {
      run: () => new Promise<void>((resolve) => setTimeout(resolve, 200)),
    });

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    link.events.emit("envelope", {
      t: "call",
      id: "call-3",
      from: "peer-node",
      to: "stub-node",
      service: "slow",
      method: "run",
      args: [],
      timeout: 5000,
    });

    await new Promise<void>((resolve) => setTimeout(resolve, 80));

    const error = link.sent.find((message) => message.t === "error");

    assert.equal(
      error !== undefined && error.t === "error" ? error.kind : undefined,
      "timeout",
    );
  });

  test("applies the inbound timeout when a call carries no deadline", async (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local, 20);

    local.register("slow", {
      run: () => new Promise<void>((resolve) => setTimeout(resolve, 200)),
    });

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    link.events.emit("envelope", {
      t: "call",
      id: "call-4",
      from: "peer-node",
      to: "stub-node",
      service: "slow",
      method: "run",
      args: [],
    });

    await new Promise<void>((resolve) => setTimeout(resolve, 80));

    const error = link.sent.find((message) => message.t === "error");

    assert.equal(
      error !== undefined && error.t === "error" ? error.kind : undefined,
      "timeout",
    );
  });

  test("closes the link when a state change cannot be delivered", async (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local);
    let closes = 0;
    const dropped: SwarmError[] = [];

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    nodeLink.events.on("close", () => (closes += 1));
    nodeLink.events.on("drop", (error) => dropped.push(error));

    const pending = nodeLink.call("peer-node", "math", "add", [1, 2], 5000);

    link.failNextSend = true;
    link.nextError = new SwarmError(
      "failed",
      "CONGESTED",
      "link is congested",
      true,
    );

    nodeLink.notify(
      changesFrame([{ t: "up", from: "other-node", service: "math" }]),
    );

    await assert.rejects(
      pending,
      (err: SwarmError) => err.code === "CONGESTED" && err.retriable === false,
    );

    assert.equal(link.closed, true);
    assert.equal(closes, 1);
    assert.equal(dropped.length, 1);
    assert.equal(dropped[0].code, "CONGESTED");
  });

  test("keeps the link when a pub frame is congested", (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local);
    const dropped: SwarmError[] = [];

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    nodeLink.events.on("drop", (error) => dropped.push(error));

    link.failNextSend = true;
    link.nextError = new SwarmError(
      "failed",
      "CONGESTED",
      "link is congested",
      true,
    );

    nodeLink.notify(
      pubsFrame([
        {
          t: "pub",
          id: "pub-1",
          ttl: 1,
          from: "stub-node",
          origin: "peer-node",
          topic: "news",
          data: 1,
        },
      ]),
    );

    assert.equal(link.closed, false);
    assert.equal(dropped.length, 1);
    assert.equal(dropped[0].code, "CONGESTED");
  });

  test("drops a state update that cannot be encoded", (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local);
    const dropped: SwarmError[] = [];

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    nodeLink.events.on("drop", (error) => dropped.push(error));

    link.failNextSend = true;
    link.nextError = new SwarmError(
      "failed",
      "ENVELOPE_INVALID",
      "envelope is not encodable",
      false,
    );

    nodeLink.notify(
      changesFrame([{ t: "up", from: "other-node", service: "math" }]),
    );

    assert.equal(link.closed, false);
    assert.equal(nodeLink.peerId, "peer-node");
    assert.equal(dropped.length, 1);
    assert.equal(dropped[0].code, "ENVELOPE_INVALID");
  });

  test("closes the link after repeated send failures", (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local);
    const congested = new SwarmError(
      "failed",
      "CLOSED",
      "link is not writable",
      true,
    );

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    const fail = () => {
      link.failNextSend = true;
      link.nextError = congested;

      assert.throws(() =>
        nodeLink.publish(
          pubsFrame([
            {
              t: "pub",
              id: "pub-fail",
              ttl: 1,
              from: "stub-node",
              origin: "peer-node",
              topic: "news",
              data: 1,
            },
          ]),
        ),
      );
    };

    fail();
    fail();

    assert.equal(link.closed, false);

    nodeLink.publish(
      pubsFrame([
        {
          t: "pub",
          id: "pub-ok",
          ttl: 1,
          from: "stub-node",
          origin: "peer-node",
          topic: "news",
          data: 1,
        },
      ]),
    );

    fail();
    fail();

    assert.equal(link.closed, false);

    fail();

    assert.equal(link.closed, true);
    assert.equal(nodeLink.closeCode, "CLOSED");
  });
});
describe("NodeLink", () => {
  test("emits bye and closes when the peer leaves", async (t) => {
    const local = new NodeLocal("local-node");
    const { port1, port2 } = new MessageChannel();
    const events: string[] = [];
    const link = new NodeLink(new LinkPort(port1, "local-node"), {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    link.events.on("bye", () => events.push("bye"));
    link.events.on("close", () => events.push("close"));

    t.after(() => {
      local.close();
      port2.close();
    });

    link.hello();
    port2.start();
    port2.postMessage({
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    while (link.peerId === undefined) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    port2.postMessage({ t: "bye", from: "peer-node" });

    while (events.length < 2) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    assert.deepEqual(events, ["bye", "close"]);
  });

  test("notifies its owner once when the link closes", (t) => {
    const local = new NodeLocal("local-node");
    const { port1, port2 } = new MessageChannel();
    let closed = 0;

    const link = new NodeLink(new LinkPort(port1, "local-node"), {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
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
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    t.after(() => {
      local.close();
      port2.close();
    });

    link.close();

    await assert.rejects(
      link.call("peer", "math", "add", [], 50),
      (err: SwarmError) =>
        err.kind === "failed" &&
        err.code === "CLOSED" &&
        err.retriable === true &&
        err.message === "link is closed",
    );
  });

  test("propagates the handler error kind over a link", async (t) => {
    const a = new Node({ cluster: "kind-a" });
    const b = new Node({ cluster: "kind-a" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.register("boom", {
      fail: () => {
        throw new SwarmError("rejected", "REJECTED", "denied");
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

  test("propagates the handler error code and retriable over a link", async (t) => {
    const a = new Node({ cluster: "code-a" });
    const b = new Node({ cluster: "code-a" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
    });

    a.register("boom", {
      fail: () => {
        throw new SwarmError("failed", "CONGESTED", "peer is gone", true);
      },
    });

    const up = new Promise<WatchEvent>((resolve) => b.watch("boom", resolve));

    b.attach(port2);
    a.attach(port1);

    await up;

    await assert.rejects(
      b.call("boom", "fail"),
      (err: SwarmError) =>
        err.kind === "failed" &&
        err.code === "CONGESTED" &&
        err.retriable === true &&
        err.message === "peer is gone",
    );
  });

  test("allows a watch while a link is closing", async (t) => {
    const a = new Node({ cluster: "closing-a" });
    const b = new Node({ cluster: "closing-a" });
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
    const b = new Node({ cluster: "link-a", secret: "s3cret" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.register("math", { add: (_ctx, x: number, y: number) => x + y });
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

    port2.postMessage({
      t: "hello",
      passive: false,
      cluster: "two-ids",
      from: "first",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "first"),
    });
    port2.postMessage({
      t: "changes",
      items: [{ t: "up", from: "first", service: "math" }],
    });

    await up("math");

    const closed = new Promise<void>((resolve) =>
      port2.addEventListener("close", () => resolve()),
    );

    port2.postMessage({
      t: "hello",
      passive: false,
      cluster: "two-ids",
      from: "second",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "second"),
    });

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
    const b = new Node({ cluster: "reply-a" });
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
      passive: false,
      cluster: "secret-a",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: await signToken("wrong-secret", "peer-node"),
    });

    await closed;

    await assert.rejects(
      a.call("math", "add"),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("uses the link secret over the node secret", async (t) => {
    const a = new Node({ cluster: "link-secret-a", secret: "node-secret" });
    const b = new Node({ cluster: "link-secret-a", secret: "link-secret" });
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

  test("accepts registrations relayed for another origin", async (t) => {
    const node = new Node({ cluster: "relayed-sender" });
    const { port1, port2 } = new MessageChannel();
    const up = new Promise<WatchEvent>((resolve) =>
      node.watch("fake", resolve),
    );

    t.after(() => {
      node.close();
      port1.close();
      port2.close();
    });

    node.attach(port1);
    port2.start();
    port2.postMessage({
      t: "hello",
      passive: false,
      cluster: "relayed-sender",
      from: "peer",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer"),
    });
    port2.postMessage({
      t: "changes",
      items: [{ t: "up", from: "origin", service: "fake" }],
    });

    const event = await up;

    assert.equal(event.instance.nodeId, "origin");

    const down = new Promise<WatchEvent>((resolve) =>
      node.watch("fake", (message) => {
        if (message.type === "down") {
          resolve(message);
        }
      }),
    );

    port2.postMessage({
      t: "changes",
      items: [{ t: "down", from: "origin", service: "fake" }],
    });

    assert.equal((await down).instance.nodeId, "origin");
  });

  test("applies every item of a changes frame", (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local);
    const downs: string[] = [];

    t.after(() => nodeLink.close());

    nodeLink.events.on("down", (instance) => downs.push(instance.nodeId));

    link.events.emit("envelope", {
      t: "changes",
      items: [
        { t: "up", from: "peer-node", service: "math" },
        { t: "up", from: "origin", service: "math" },
        { t: "down", from: "origin", service: "math" },
      ],
    });

    assert.deepEqual(
      nodeLink.lookup("math").map((instance) => instance.nodeId),
      ["peer-node"],
    );
    assert.deepEqual(downs, ["origin"]);
  });

  test("ignores a changes frame from an unauthenticated link", (t) => {
    const local = new NodeLocal("local-node");
    const link = new StubLink();
    const nodeLink = new NodeLink(link, {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    t.after(() => nodeLink.close());

    link.events.emit("envelope", {
      t: "changes",
      items: [{ t: "up", from: "origin", service: "math" }],
    });

    assert.deepEqual(nodeLink.lookup("math"), []);
  });

  test("drops a publish that returns to its origin or spoofs the sender", async (t) => {
    const node = new Node({ cluster: "pub-origin" });
    const { port1, port2 } = new MessageChannel();
    const received: unknown[] = [];
    const done = new Promise<void>((resolve) =>
      node.subscribe("news", (data, origin) => {
        received.push({ data, origin });

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
    port2.postMessage({
      t: "hello",
      passive: false,
      cluster: "pub-origin",
      from: "peer",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer"),
    });
    port2.postMessage({
      t: "pubs",
      items: [
        {
          t: "pub",
          id: "pub-1",
          ttl: 8,
          from: "peer",
          origin: node.nodeId,
          topic: "news",
          data: 1,
        },
      ],
    });
    port2.postMessage({
      t: "pubs",
      items: [
        {
          t: "pub",
          id: "pub-1",
          ttl: 8,
          from: "spoof",
          origin: "peer",
          topic: "news",
          data: 3,
        },
      ],
    });
    port2.postMessage({
      t: "pubs",
      items: [
        {
          t: "pub",
          id: "pub-1",
          ttl: 8,
          from: "peer",
          origin: "peer",
          topic: "news",
          data: 2,
        },
      ],
    });

    await done;

    assert.deepEqual(received, [{ data: 2, origin: "peer" }]);
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
    port2.postMessage({
      t: "hello",
      passive: false,
      cluster: "call-target",
      from: "peer",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer"),
    });
    port2.postMessage({
      t: "changes",
      items: [{ t: "up", from: "peer", service: "remote" }],
    });

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
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
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
    port2.postMessage({
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });
    port2.postMessage({
      t: "changes",
      items: [{ t: "up", from: "peer-node", service: "math" }],
    });

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

    port2.postMessage({
      t: "changes",
      items: [{ t: "down", from: "peer-node", service: "math" }],
    });

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
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
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

  test("sends a cancel frame when the caller aborts", async (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local);
    const controller = new AbortController();

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    const call = nodeLink.call(
      "peer-node",
      "math",
      "add",
      [],
      5000,
      undefined,
      undefined,
      controller.signal,
    );

    controller.abort();

    await assert.rejects(call, (err: SwarmError) => err.code === "CANCELLED");

    const sent = link.sent.find((message) => message.t === "call");
    const cancel = link.sent.find((message) => message.t === "cancel");

    assert.ok(sent !== undefined && sent.t === "call");
    assert.ok(cancel !== undefined && cancel.t === "cancel");
    assert.equal(cancel.id, sent.id);
    assert.equal(cancel.to, "peer-node");
  });

  test("sends a cancel frame when the call times out", async (t) => {
    const local = new NodeLocal("local-node");
    const { link, nodeLink } = attachStub(local);

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    await assert.rejects(
      nodeLink.call("peer-node", "math", "add", [], 20),
      (err: SwarmError) => err.code === "TIMEOUT",
    );

    assert.equal(
      link.sent.some((message) => message.t === "cancel"),
      true,
    );
  });

  test("passes the frame's arrival time so a relayed call keeps its budget", async (t) => {
    const local = new NodeLocal("local-node");
    const invoke = local.invoke.bind(local);

    let startedAt: number | undefined;

    local.invoke = ((...args: Parameters<typeof invoke>) => {
      startedAt = (args as unknown[])[8] as number | undefined;
      return invoke(...args);
    }) as typeof local.invoke;

    local.register("math", { add: () => 1 });

    const { link, nodeLink } = attachStub(local);

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    const before = Date.now();

    link.events.emit("envelope", {
      t: "call",
      id: "c1",
      from: "peer-node",
      to: "stub-node",
      service: "math",
      method: "add",
      args: [],
      timeout: 200,
    });

    await Promise.resolve();

    assert.ok(startedAt !== undefined);
    assert.ok(startedAt >= before);
  });

  test("rejects a second call that reuses an in-flight id", async (t) => {
    const local = new NodeLocal("local-node");
    let calls = 0;
    let release: (() => void) | undefined;

    local.register("slow", {
      wait: () =>
        new Promise((resolve) => {
          calls += 1;
          release = () => resolve("done");
        }),
    });

    const { link, nodeLink } = attachStub(local);
    const drops: string[] = [];

    nodeLink.events.on("drop", (error) => drops.push(error.code));

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    const call = {
      t: "call" as const,
      id: "c1",
      from: "peer-node",
      to: "stub-node",
      service: "slow",
      method: "wait",
      args: [],
    };

    link.events.emit("envelope", call);
    link.events.emit("envelope", call);

    await new Promise((resolve) => setTimeout(resolve, 10));

    assert.equal(calls, 1);
    assert.equal(nodeLink.inboundCalls, 1);
    assert.deepEqual(drops, ["DUPLICATE_CALL"]);

    release?.();

    await new Promise((resolve) => setTimeout(resolve, 10));

    assert.equal(
      link.sent.filter((message) => message.t === "reply").length,
      1,
    );
  });

  test("aborts the handler and skips the reply when the peer cancels", async (t) => {
    const local = new NodeLocal("local-node");
    let seen: { from: string; signal: AbortSignal } | undefined;

    local.register("slow", {
      wait: (ctx) =>
        new Promise((resolve) => {
          seen = { from: ctx.from, signal: ctx.signal };
          ctx.signal.addEventListener("abort", () => resolve("late"));
        }),
    });

    const { link, nodeLink } = attachStub(local);

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    link.events.emit("envelope", {
      t: "call",
      id: "c1",
      from: "peer-node",
      to: "stub-node",
      service: "slow",
      method: "wait",
      args: [],
    });

    await Promise.resolve();

    assert.equal(seen?.from, "peer-node");
    assert.equal(seen?.signal.aborted, false);

    link.events.emit("envelope", {
      t: "cancel",
      id: "c1",
      from: "peer-node",
      to: "stub-node",
    });

    assert.equal(seen?.signal.aborted, true);

    await new Promise((resolve) => setTimeout(resolve, 10));

    assert.equal(
      link.sent.some((message) => message.t === "reply" || message.t === "error"),
      false,
    );
  });

  test("ignores a cancel that names another node", async (t) => {
    const local = new NodeLocal("local-node");
    let signal: AbortSignal | undefined;

    local.register("slow", {
      wait: (ctx) =>
        new Promise((resolve) => {
          signal = ctx.signal;
          ctx.signal.addEventListener("abort", () => resolve("late"));
        }),
    });

    const { link, nodeLink } = attachStub(local);

    t.after(() => {
      nodeLink.close();
      local.close();
    });

    link.events.emit("envelope", {
      t: "call",
      id: "c1",
      from: "peer-node",
      to: "stub-node",
      service: "slow",
      method: "wait",
      args: [],
    });

    await Promise.resolve();

    for (const cancel of [
      { t: "cancel" as const, id: "c1", from: "other-node", to: "stub-node" },
      { t: "cancel" as const, id: "c1", from: "peer-node", to: "other-node" },
      { t: "cancel" as const, id: "nope", from: "peer-node", to: "stub-node" },
    ]) {
      link.events.emit("envelope", cancel);
    }

    assert.equal(signal?.aborted, false);

    link.events.emit("envelope", {
      t: "cancel",
      id: "c1",
      from: "peer-node",
      to: "stub-node",
    });

    assert.equal(signal?.aborted, true);
  });

  test("fails pending calls when the peer closes", async (t) => {
    const local = new NodeLocal("local-node");
    const { port1, port2 } = new MessageChannel();
    const link = new NodeLink(new LinkPort(port1, "local-node"), {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
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

  test("closes a link that never completes the handshake", (t) => {
    mock.timers.enable({ apis: ["setInterval", "setTimeout", "Date"] });

    const local = new NodeLocal("local-node");
    const stub = new StubLink();
    const link = new NodeLink(stub, {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    t.after(() => {
      link.close();
      local.close();
      mock.timers.reset();
    });

    mock.timers.tick(15000);

    assert.equal(link.peerId, undefined);
    assert.equal(stub.closed, true);
  });

  test("beat tolerates a failing ping send", (t) => {
    mock.timers.enable({ apis: ["setInterval", "Date"] });
    t.after(() => mock.timers.reset());

    const local = new NodeLocal("local-node");
    const stub = new StubLink();
    const link = new NodeLink(stub, {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    t.after(() => {
      link.close();
      local.close();
    });

    stub.events.emit("envelope", {
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    stub.failNextSend = true;

    mock.timers.tick(15000);

    assert.equal(stub.closed, false);

    mock.timers.tick(15000);

    const pings = stub.sent.filter((message) => message.t === "ping");

    assert.equal(pings.length, 1);
  });

  test("beat resends a ping when the previous pong is lost", (t) => {
    mock.timers.enable({ apis: ["setInterval", "Date"] });
    t.after(() => mock.timers.reset());

    const local = new NodeLocal("local-node");
    const stub = new StubLink();
    const link = new NodeLink(stub, {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    t.after(() => {
      link.close();
      local.close();
    });

    stub.events.emit("envelope", {
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    mock.timers.tick(15000);
    mock.timers.tick(15000);

    const pings = stub.sent.filter((message) => message.t === "ping");

    assert.equal(pings.length, 2);
  });

  test("honors a custom heartbeat timeout", (t) => {
    mock.timers.enable({ apis: ["setInterval", "setTimeout", "Date"] });
    t.after(() => mock.timers.reset());

    const local = new NodeLocal("local-node");
    const stub = new StubLink();
    const link = new NodeLink(stub, {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 1000,
      heartbeatTimeout: 2000,
    });

    t.after(() => {
      link.close();
      local.close();
    });

    stub.events.emit("envelope", {
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    mock.timers.tick(3000);

    assert.equal(stub.closed, true);
  });

  test("accepts a peer with the same major protocol version", (t) => {
    const local = new NodeLocal("local-node");
    const stub = new StubLink();
    const link = new NodeLink(stub, {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    t.after(() => {
      link.close();
      local.close();
    });

    stub.events.emit("envelope", {
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: Math.floor(protocolVersion / 100) * 100 + 99,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    assert.equal(link.peerId, "peer-node");
    assert.equal(stub.closed, false);
  });

  test("closes a link from another major protocol version", (t) => {
    const local = new NodeLocal("local-node");
    const stub = new StubLink();
    const link = new NodeLink(stub, {
      provider: local,
      cluster: "default",
      passive: false,
      startup: 1,
      secret: "",
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    t.after(() => {
      link.close();
      local.close();
    });

    stub.events.emit("envelope", {
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: (Math.floor(protocolVersion / 100) + 1) * 100,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    assert.equal(link.peerId, undefined);
    assert.equal(stub.closed, true);
    assert.equal(link.closeCode, "VERSION_MISMATCH");
  });

  test("caps an inbound call timeout at 60 seconds", async (t) => {
    const stub = new StubLink();
    const timeouts: Array<number | undefined> = [];
    const provider = {
      registrations: () => [],
      invoke: (
        _service: string,
        _method: string,
        _args: unknown[],
        timeout?: number,
      ) => {
        timeouts.push(timeout);
      },
      publish: () => {},
    };
    const link = new NodeLink(stub, {
      provider,
      startup: 1,
      secret: "",
      cluster: "default",
      passive: false,
      heartbeatInterval: 15000,
      heartbeatTimeout: 45000,
    });

    t.after(() => link.close());

    stub.events.emit("envelope", {
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    stub.events.emit("envelope", {
      t: "call",
      id: "1",
      from: "peer-node",
      to: stub.nodeId,
      service: "math",
      method: "add",
      args: [],
      timeout: 120000,
    });
    stub.events.emit("envelope", {
      t: "call",
      id: "2",
      from: "peer-node",
      to: stub.nodeId,
      service: "math",
      method: "add",
      args: [],
      timeout: 5000,
    });
    stub.events.emit("envelope", {
      t: "call",
      id: "3",
      from: "peer-node",
      to: stub.nodeId,
      service: "math",
      method: "add",
      args: [],
    });

    await new Promise<void>((resolve) => setImmediate(resolve));

    assert.deepEqual(timeouts, [60000, 5000, 60000]);
  });
});

describe("ready", () => {
  const makeLink = () => {
    const link = new StubLink();

    return {
      link,
      nodeLink: new NodeLink(link, {
        provider: new NodeLocal("local-node"),
        cluster: "default",
        passive: false,
        startup: 1,
        secret: "",
        heartbeatInterval: 15000,
        heartbeatTimeout: 45000,
      }),
    };
  };

  test("resolves after the handshake completes", async () => {
    const { link, nodeLink } = makeLink();
    const ready = nodeLink.ready();

    link.events.emit("envelope", {
      t: "hello",
      passive: false,
      cluster: "default",
      from: "peer-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-node"),
    });

    await ready;

    assert.equal(nodeLink.peerId, "peer-node");

    nodeLink.close();
  });

  test("rejects when the link closes before the peer answers", async () => {
    const { nodeLink } = makeLink();
    const ready = nodeLink.ready();

    nodeLink.close();

    await assert.rejects(
      ready,
      (err: SwarmError) => err.kind === "failed" && err.code === "CLOSED",
    );
  });

  test("rejects when it is called on a closed link", async () => {
    const { nodeLink } = makeLink();

    nodeLink.close();

    await assert.rejects(
      nodeLink.ready(),
      (err: SwarmError) => err.kind === "failed" && err.code === "CLOSED",
    );
  });
});
