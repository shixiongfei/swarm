/*
 * serial.test.ts
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
import { serial } from "./serial.ts";
import { within } from "./testutil.ts";
import { SwarmError } from "./errors.ts";
import type { CallContext } from "./local.ts";

describe("serial", () => {
  test("runs the calls of one service one at a time in arrival order", async (t) => {
    const node = new Node({ cluster: "serial-order" });
    const order: string[] = [];

    t.after(() => node.close());

    node.register(
      "svc",
      serial({
        hit: async (_ctx, n: number) => {
          order.push(`start${n}`);

          await new Promise((resolve) =>
            setTimeout(resolve, n === 1 ? 30 : 0),
          );

          order.push(`end${n}`);

          return n;
        },
      }),
    );

    const [one, two] = await Promise.all([
      node.call("svc", "hit", [1]),
      node.call("svc", "hit", [2]),
    ]);

    assert.deepEqual(order, ["start1", "end1", "start2", "end2"]);
    assert.equal(one, 1);
    assert.equal(two, 2);
  });

  test("does not run a queued call after the peer closes the link", async (t) => {
    const host = new Node({ cluster: "serial-close" });
    const peer = new Node({ cluster: "serial-close" });
    const { port1, port2 } = new MessageChannel();
    let ran = 0;

    t.after(() => {
      host.close();
      peer.close();
      port1.close();
      port2.close();
    });

    const up = new Promise<void>((resolve) =>
      host.watch("svc", () => resolve()),
    );

    peer.register(
      "svc",
      serial({
        slow: () =>
          new Promise((resolve) => setTimeout(() => resolve("slow"), 100)),
        next: () => {
          ran += 1;

          return "next";
        },
      }),
    );

    host.attach(port1);
    peer.attach(port2);

    await within(up, 2000);

    const slow = host.call("svc", "slow");

    await new Promise((resolve) => setTimeout(resolve, 10));

    const queued = host.call("svc", "next");

    await new Promise((resolve) => setTimeout(resolve, 30));

    peer.close();

    await assert.rejects(slow, (err: SwarmError) => err.code === "CLOSED");
    await assert.rejects(queued, (err: SwarmError) => err.code === "CLOSED");

    await new Promise((resolve) => setTimeout(resolve, 150));

    assert.equal(ran, 0);
  });

  test("does not run a queued local call after the node closes", async (t) => {
    const node = new Node({ cluster: "serial-local-close" });
    let ran = 0;

    t.after(() => node.close());

    node.register(
      "svc",
      serial({
        slow: () =>
          new Promise((resolve) => setTimeout(() => resolve("slow"), 100)),
        next: () => {
          ran += 1;

          return "next";
        },
      }),
    );

    const slow = node.call("svc", "slow");

    await new Promise((resolve) => setTimeout(resolve, 10));

    const queued = node.call("svc", "next");

    await new Promise((resolve) => setTimeout(resolve, 10));

    node.close();

    await assert.rejects(queued, (err: SwarmError) => err.code === "CLOSED");

    await new Promise((resolve) => setTimeout(resolve, 150));

    assert.equal(ran, 0);
    assert.equal(await slow, "slow");
  });

  test("skips a queued call whose caller gave up before it ran", async () => {
    const svc = serial({
      slow: async (_ctx: CallContext) =>
        new Promise((resolve) => setTimeout(resolve, 20)),
      next: async (_ctx: CallContext) => "next",
    });
    const ctx = (controller: AbortController) => ({
      from: "peer-node",
      signal: controller.signal,
    });
    const timedOut = new AbortController();
    const plain = new AbortController();
    const reason = new SwarmError("timeout", "TIMEOUT", "call timed out");

    const first = svc.slow(ctx(new AbortController()));
    const second = svc.next(ctx(timedOut));
    const third = svc.next(ctx(plain));

    timedOut.abort(reason);
    plain.abort(new Error("no reason"));

    await first;
    await assert.rejects(second, (err: unknown) => err === reason);
    await assert.rejects(third, (err: SwarmError) => err.code === "CANCELLED");
  });

  test("runs the dispose hook of the wrapped methods", async () => {
    const node = new Node({ cluster: "serial-dispose" });
    let disposed = 0;

    const methods = {
      ping: () => "pong",
      [Symbol.dispose]: () => {
        disposed += 1;
      },
    };

    node.register("svc", serial(methods));
    node.close();

    assert.equal(disposed, 1);
  });
});
