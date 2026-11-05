/*
 * node.tcp.test.ts
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
import { createServer } from "node:net";
import { Node } from "./node.ts";
import type { AddressInfo } from "node:net";
import type { MusterError } from "./errors.ts";
import type { WatchEvent } from "./registry.ts";

async function freePort() {
  const server = createServer();

  await new Promise<void>((resolve) =>
    server.listen(0, "127.0.0.1", resolve),
  );

  const port = (server.address() as AddressInfo).port;

  await new Promise<void>((resolve) => server.close(() => resolve()));

  return port;
}

async function connectPair(hub: Node, peer: Node) {
  const port = await freePort();
  const url = `tcp://127.0.0.1:${port}`;

  assert.equal(await hub.listen(url), hub);
  assert.equal(await peer.connect(url), peer);

  return port;
}

describe("Node TCP", () => {
  test("listen and connect return the node", async (t) => {
    const hub = new Node({ cluster: "tcp-return-hub" });
    const peer = new Node({ cluster: "tcp-return-peer" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    await connectPair(hub, peer);
  });

  test("calls a service registered on the connected peer", { timeout: 3000 }, async (t) => {
    const hub = new Node({ cluster: "tcp-call-hub" });
    const peer = new Node({ cluster: "tcp-call-peer" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    const up = new Promise<WatchEvent>((resolve) => hub.watch("math", resolve));

    await connectPair(hub, peer);

    peer.register("math", { add: (a: number, b: number) => a + b });

    assert.equal((await up).instance.nodeId, peer.nodeId);
    assert.equal(await hub.call("math", "add", [2, 3]), 5);
  });

  test("publishes from the peer to the hub", { timeout: 3000 }, async (t) => {
    const hub = new Node({ cluster: "tcp-publish-hub" });
    const peer = new Node({ cluster: "tcp-publish-peer" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    await connectPair(hub, peer);

    const ready = new Promise<WatchEvent>((resolve) =>
      hub.watch("math", resolve),
    );
    const received = new Promise<unknown>((resolve) =>
      hub.subscribe("news", resolve),
    );

    peer.register("math", { add: () => 0 });

    await ready;
    peer.publish("news", { id: 1 });

    assert.deepEqual(await received, { id: 1 });
  });

  test("accepts a url object and a url secret", { timeout: 3000 }, async (t) => {
    const hub = new Node({ cluster: "tcp-secret-hub" });
    const peer = new Node({ cluster: "tcp-secret-peer" });
    const port = await freePort();

    t.after(() => {
      hub.close();
      peer.close();
    });

    const up = new Promise<WatchEvent>((resolve) => hub.watch("math", resolve));

    await hub.listen(new URL(`tcp://:s3cret@127.0.0.1:${port}`));
    await peer.connect(`tcp://:s3cret@127.0.0.1:${port}`);

    peer.register("math", { add: () => 7 });

    await up;
    assert.equal(await hub.call("math", "add"), 7);
  });

  test("listen rejects a malformed url", async (t) => {
    const node = new Node({ cluster: "tcp-invalid-listen" });

    t.after(() => node.close());

    await assert.rejects(
      node.listen("http://127.0.0.1:1234"),
      (err: MusterError) =>
        err.kind === "rejected" && err.message === "invalid tcp url: http://127.0.0.1:1234",
    );
    await assert.rejects(
      node.listen("tcp://127.0.0.1"),
      (err: MusterError) =>
        err.kind === "rejected" && err.message === "invalid tcp url: tcp://127.0.0.1",
    );
  });

  test("connect rejects when nothing listens", { timeout: 3000 }, async (t) => {
    const node = new Node({ cluster: "tcp-refused" });
    const port = await freePort();

    t.after(() => node.close());

    await assert.rejects(
      node.connect(`tcp://127.0.0.1:${port}`),
      (err: MusterError) => err.kind === "failed",
    );
  });

  test("close stops listening and rejects later calls", { timeout: 3000 }, async (t) => {
    const hub = new Node({ cluster: "tcp-close-hub" });
    const peer = new Node({ cluster: "tcp-close-peer" });

    t.after(() => peer.close());

    const up = new Promise<WatchEvent>((resolve) => hub.watch("math", resolve));

    await connectPair(hub, peer);

    peer.register("math", { add: () => 1 });

    await up;
    assert.equal(await hub.call("math", "add"), 1);

    hub.close();

    assert.throws(
      () => hub.call("math", "add"),
      (err: MusterError) => err.kind === "rejected",
    );
  });
});
