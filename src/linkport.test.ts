/*
 * linkport.test.ts
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
import { LinkPort } from "./linkport.ts";
import type { MusterError } from "./errors.ts";
import type { Envelope } from "./envelope.ts";

describe("LinkPort", () => {
  test("ignores data that is not an envelope", async (t) => {
    const { port1, port2 } = new MessageChannel();
    const link = new LinkPort(port1);
    let count = 0;

    t.after(() => {
      link.close();
      port2.close();
    });

    const done = new Promise<void>((resolve) => {
      link.events.on("envelope", (message) => {
        count += 1;

        if (message.t === "pub") {
          resolve();
        }
      });
    });

    port2.start();
    port2.postMessage(null);
    port2.postMessage("text");
    port2.postMessage(42);
    port2.postMessage({
      t: "pub",
      nodeId: "node-a",
      topic: "news",
      data: null,
    });

    await done;

    assert.equal(count, 1);
  });

  test("close closes the port and is idempotent", async (t) => {
    const { port1, port2 } = new MessageChannel();
    const link = new LinkPort(port1);

    t.after(() => {
      port2.close();
    });

    const closed = new Promise<void>((resolve) =>
      port2.addEventListener("close", () => resolve()),
    );

    port2.start();
    link.close();

    await closed;

    assert.doesNotThrow(() => {
      link.close();
      link.send({ t: "pub", nodeId: "node-a", topic: "news", data: 1 });
    });
  });

  test("emits close once when the peer closes the port", async (t) => {
    const { port1, port2 } = new MessageChannel();
    const link = new LinkPort(port1);
    let count = 0;

    t.after(() => {
      link.close();
      port2.close();
    });

    const closed = new Promise<void>((resolve) => {
      link.events.on("close", () => {
        count += 1;
        resolve();
      });
    });

    port2.start();
    port2.close();

    await closed;

    assert.equal(count, 1);
  });

  test("rejects a port that is already attached", async (t) => {
    const { port1, port2 } = new MessageChannel();
    const link = new LinkPort(port1);

    t.after(() => {
      link.close();
      port2.close();
    });

    assert.throws(
      () => new LinkPort(port1),
      (err: MusterError) =>
        err.kind === "rejected" && err.message === "port is already attached",
    );

    const received = new Promise<Envelope>((resolve) =>
      link.events.on("envelope", resolve),
    );

    port2.start();
    port2.postMessage({ t: "pub", nodeId: "node-a", topic: "news", data: 1 });

    assert.equal((await received).t, "pub");
  });
});
