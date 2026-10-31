/*
 * link.test.ts
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
import { Link } from "./link.ts";

function makePair() {
  const { port1, port2 } = new MessageChannel();
  const receivedByA: unknown[] = [];
  const receivedByB: unknown[] = [];
  const closedA: number[] = [];
  const closedB: number[] = [];

  const a = new Link(port1, {
    receive: (message) => receivedByA.push(message),
    close: () => closedA.push(1),
  });

  const b = new Link(port2, {
    receive: (message) => receivedByB.push(message),
    close: () => closedB.push(1),
  });

  return { a, b, receivedByA, receivedByB, closedA, closedB };
}

function flush() {
  return new Promise((resolve) => setTimeout(resolve, 10));
}

describe("Link", () => {
  test("send delivers a message to the other end", async (t) => {
    const pair = makePair();
    t.after(() => {
      pair.a.close();
      pair.b.close();
    });

    pair.a.send({ hello: "world" });
    await flush();

    assert.deepEqual(pair.receivedByB, [{ hello: "world" }]);
  });

  test("both ends send independently", async (t) => {
    const pair = makePair();
    t.after(() => {
      pair.a.close();
      pair.b.close();
    });

    pair.a.send(1);
    pair.b.send(2);
    pair.a.send(3);
    await flush();

    assert.deepEqual(pair.receivedByB, [1, 3]);
    assert.deepEqual(pair.receivedByA, [2]);
  });

  test("close notifies the local handler once", () => {
    const pair = makePair();

    pair.a.close();
    pair.a.close();

    assert.equal(pair.closedA.length, 1);
    pair.b.close();
  });

  test("closing one end notifies the other", async (t) => {
    const pair = makePair();
    t.after(() => pair.b.close());

    pair.a.close();
    await flush();

    assert.equal(pair.closedB.length, 1);
  });

  test("send after close is dropped", async (t) => {
    const pair = makePair();
    t.after(() => pair.b.close());

    pair.a.close();

    assert.equal(pair.a.send("late"), false);
    await flush();

    assert.deepEqual(pair.receivedByB, []);
    assert.equal(pair.closedB.length, 1);
  });
});
