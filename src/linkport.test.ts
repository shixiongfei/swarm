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
    const link = new LinkPort(port1, "node-a");
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
      from: "node-a",
      topic: "news",
      data: null,
    });

    await done;

    assert.equal(count, 1);
  });

  test("close closes the port and is idempotent", async (t) => {
    const { port1, port2 } = new MessageChannel();
    const link = new LinkPort(port1, "node-a");

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
      link.send({ t: "pub", from: "node-a", topic: "news", data: 1 });
    });
  });

  test("emits close once when the peer closes the port", async (t) => {
    const { port1, port2 } = new MessageChannel();
    const link = new LinkPort(port1, "node-a");
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

  test("rejects a port that is already linked", async (t) => {
    const { port1, port2 } = new MessageChannel();
    const link = new LinkPort(port1, "node-a");

    t.after(() => {
      link.close();
      port2.close();
    });

    assert.throws(
      () => new LinkPort(port1, "node-a"),
      (err: MusterError) =>
        err.kind === "rejected" && err.message === "port is already linked",
    );

    const received = new Promise<Envelope>((resolve) =>
      link.events.on("envelope", resolve),
    );

    port2.start();
    port2.postMessage({ t: "pub", from: "node-a", topic: "news", data: 1 });

    assert.equal((await received).t, "pub");
  });

  test("links different ports independently", async (t) => {
    const first = new MessageChannel();
    const second = new MessageChannel();
    const a = new LinkPort(first.port1, "node-a");
    const b = new LinkPort(second.port1, "node-b");
    const received: Envelope[] = [];

    t.after(() => {
      a.close();
      b.close();
      first.port2.close();
      second.port2.close();
    });

    const both = new Promise<void>((resolve) => {
      const collect = (message: Envelope) => {
        received.push(message);

        if (received.length === 2) {
          resolve();
        }
      };

      a.events.on("envelope", collect);
      b.events.on("envelope", collect);
    });

    first.port2.start();
    second.port2.start();
    first.port2.postMessage({ t: "pub", from: "node-a", topic: "a", data: 1 });
    second.port2.postMessage({ t: "pub", from: "node-b", topic: "b", data: 2 });

    await both;

    assert.deepEqual(
      received
        .map((message) => (message.t === "pub" ? message.topic : ""))
        .sort(),
      ["a", "b"],
    );
  });
  test("propagates data clone errors", (t) => {
    const { port1, port2 } = new MessageChannel();
    const link = new LinkPort(port1, "node-a");

    t.after(() => {
      link.close();
      port2.close();
    });

    assert.throws(
      () =>
        link.send({
          t: "pub",
          from: "node-a",
          topic: "news",
          data: () => {},
        }),
      (err: Error) => err.name === "DataCloneError",
    );
  });

  test("ignores a send after the port is closed", (t) => {
    const { port1, port2 } = new MessageChannel();
    const link = new LinkPort(port1, "node-a");

    t.after(() => {
      link.close();
      port2.close();
    });

    port2.start();
    port1.close();

    assert.doesNotThrow(() =>
      link.send({ t: "pub", from: "node-a", topic: "news", data: 1 }),
    );
  });

});
