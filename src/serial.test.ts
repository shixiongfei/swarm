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
import type { SwarmError } from "./errors.ts";

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
});
