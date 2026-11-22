/*
 * pubsub.test.ts
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
import { PubSub } from "./pubsub.ts";
import { SwarmError } from "./errors.ts";
import type { Frame } from "./frame.ts";
import type { NodeLink } from "./nodelink.ts";

type Sent = {
  topic: string;
  data: unknown;
  origin: string;
  id: string;
  ttl: number;
};

function makeLink(
  peerId: string,
  sent: Sent[],
  fail?: Error,
  transport: string = "tcp",
) {
  return {
    peerId,
    transport,
    publish(frame: Frame) {
      if (fail !== undefined) {
        throw fail;
      }

      if (frame.message.t !== "pubs") {
        return;
      }

      for (const item of frame.message.items) {
        const { topic, data, origin, id, ttl } = item;

        sent.push({ topic, data, origin, id, ttl });
      }
    },
  } as unknown as NodeLink;
}

function makePubSub(links: NodeLink[], maxHops = 3) {
  const local: Array<{ topic: string; data: unknown; origin: string }> = [];
  const dropped: SwarmError[] = [];

  const pubsub = new PubSub({
    nodeId: "self",
    maxHops,
    links: () => links,
    local: (topic, data, origin) => local.push({ topic, data, origin }),
    drop: (error) => dropped.push(error),
  });

  return { pubsub, local, dropped };
}

function flush() {
  return new Promise<void>((resolve) => setImmediate(resolve));
}

describe("PubSub", () => {
  test("publish keeps fanning out when a link fails", async () => {
    const sent: Sent[] = [];
    const { pubsub, local, dropped } = makePubSub([
      makeLink("a", [], new Error("congested")),
      makeLink("b", sent),
    ]);

    pubsub.publish("topic", "data", undefined, undefined, "pub-1");

    await flush();

    assert.equal(sent.length, 1);
    assert.deepEqual(local, [{ topic: "topic", data: "data", origin: "self" }]);
    assert.equal(dropped.length, 1);
    assert.equal(dropped[0].code, "PUB_DROPPED");
    assert.equal(dropped[0].message, "congested");
  });

  test("reports a frame that a link rejects", async () => {
    const sent: Sent[] = [];
    const invalid = new SwarmError("failed", "ENVELOPE_INVALID", "bad payload");
    const { pubsub, local, dropped } = makePubSub([
      makeLink("a", [], invalid),
      makeLink("b", sent),
    ]);

    pubsub.publish("topic", "data", undefined, undefined, "pub-1");

    await flush();

    assert.equal(sent.length, 1);
    assert.equal(local.length, 1);
    assert.equal(dropped.length, 1);
    assert.equal(dropped[0].code, "PUB_DROPPED");
    assert.equal(dropped[0].message, "bad payload");
  });

  test("clamps a ttl above the hop limit", async () => {
    const sent: Sent[] = [];
    const { pubsub } = makePubSub([makeLink("a", sent)], 3);

    pubsub.publish("topic", "data", undefined, undefined, "pub-1", 1000);

    await flush();

    assert.equal(sent[0].ttl, 2);
  });

  test("publish forwards to every link and publishes locally", async () => {
    const first: Sent[] = [];
    const second: Sent[] = [];
    const { pubsub, local } = makePubSub([
      makeLink("a", first),
      makeLink("b", second),
    ]);

    pubsub.publish("topic", { value: 1 }, undefined, undefined, "pub-1");

    await flush();

    assert.deepEqual(first, [
      {
        topic: "topic",
        data: { value: 1 },
        origin: "self",
        id: "pub-1",
        ttl: 2,
      },
    ]);
    assert.deepEqual(second, first);
    assert.deepEqual(local, [
      { topic: "topic", data: { value: 1 }, origin: "self" },
    ]);
  });

  test("publish coalesces a burst into one frame per link", async () => {
    const sent: Sent[] = [];
    const { pubsub } = makePubSub([makeLink("a", sent)]);

    for (let i = 0; i < 3; i += 1) {
      pubsub.publish("topic", i, undefined, undefined, `pub-${i}`);
    }

    assert.equal(sent.length, 0);

    await flush();

    assert.deepEqual(
      sent.map((item) => item.data),
      [0, 1, 2],
    );
  });

  test("publish skips the link the message came from", async () => {
    const first: Sent[] = [];
    const second: Sent[] = [];
    const { pubsub } = makePubSub([
      makeLink("a", first),
      makeLink("b", second),
    ]);

    pubsub.publish("topic", "data", "origin", "a", "pub-1");

    await flush();

    assert.equal(first.length, 0);
    assert.equal(second.length, 1);
    assert.equal(second[0].origin, "origin");
  });

  test("publish sends a given id only once", async () => {
    const sent: Sent[] = [];
    const { pubsub, local } = makePubSub([makeLink("a", sent)]);

    pubsub.publish("topic", "data", undefined, undefined, "pub-1");
    pubsub.publish("topic", "data", undefined, undefined, "pub-1");

    await flush();

    assert.equal(sent.length, 1);
    assert.equal(local.length, 1);
  });

  test("publish does not forward when the ttl is exhausted", async () => {
    const sent: Sent[] = [];
    const { pubsub, local } = makePubSub([makeLink("a", sent)]);

    pubsub.publish("topic", "data", undefined, undefined, "pub-1", 0);

    await flush();

    assert.equal(sent.length, 0);
    assert.equal(local.length, 1);
  });

  test("publish reports a clone failure from a port link", async () => {
    const error = Object.assign(new Error("clone"), { name: "DataCloneError" });
    const { pubsub, local, dropped } = makePubSub([makeLink("a", [], error)]);

    pubsub.publish("topic", "data", undefined, undefined, "pub-1");

    await flush();

    assert.equal(local.length, 1);
    assert.equal(dropped.length, 1);
    assert.equal(dropped[0].code, "PUB_DROPPED");
    assert.equal(dropped[0].message, "data is not cloneable");
  });

  test("publish delivers locally when the payload cannot be encoded", async () => {
    const sent: Sent[] = [];
    const { pubsub, local, dropped } = makePubSub([makeLink("a", sent)]);

    pubsub.publish("topic", () => {}, undefined, undefined, "pub-1");

    await flush();

    assert.equal(sent.length, 0);
    assert.equal(local.length, 1);
    assert.equal(dropped.length, 1);
    assert.equal(dropped[0].code, "PUB_DROPPED");
    assert.equal(dropped[0].message, "envelope is not encodable");
  });

  test("close clears the seen ids so they can be published again", async () => {
    const sent: Sent[] = [];
    const { pubsub } = makePubSub([makeLink("a", sent)]);

    pubsub.publish("topic", "data", undefined, undefined, "pub-1");
    await flush();
    pubsub.close();

    pubsub.publish("topic", "data", undefined, undefined, "pub-1");
    await flush();

    assert.equal(sent.length, 2);
  });

  test("close drops the queued publishes", async () => {
    const sent: Sent[] = [];
    const { pubsub } = makePubSub([makeLink("a", sent)]);

    pubsub.publish("topic", "data", undefined, undefined, "pub-1");
    pubsub.close();

    await flush();

    assert.equal(sent.length, 0);
  });

  test("port links deliver payloads that cannot be encoded", async () => {
    const sent: Sent[] = [];
    const map = new Map([["a", 1]]);
    const { pubsub, dropped } = makePubSub([
      makeLink("a", sent, undefined, "port"),
    ]);

    pubsub.publish("topic", map, undefined, undefined, "pub-map");
    pubsub.publish("topic", "x".repeat(9 * 1024 * 1024), undefined, undefined, "pub-big");

    await flush();

    assert.equal(sent.length, 2);
    assert.equal(sent[0].data, map);
    assert.equal((sent[1].data as string).length, 9 * 1024 * 1024);
    assert.equal(dropped.length, 0);
  });

  test("no links does not report an undeliverable payload", async () => {
    const { pubsub, dropped } = makePubSub([]);

    pubsub.publish("topic", new Map(), undefined, undefined, "pub-1");

    await flush();

    assert.equal(dropped.length, 0);
  });

  test("keeps publishes queued during a flush", async () => {
    const sent: Sent[] = [];
    const local: unknown[] = [];
    const dropped: SwarmError[] = [];
    let fail: Error | undefined = new Error("congested");
    const link = {
      peerId: "a",
      transport: "tcp",
      publish(frame: Frame) {
        if (fail !== undefined) {
          const error = fail;
          fail = undefined;
          throw error;
        }

        if (frame.message.t !== "pubs") {
          return;
        }

        for (const item of frame.message.items) {
          sent.push({
            topic: item.topic,
            data: item.data,
            origin: item.origin,
            id: item.id,
            ttl: item.ttl,
          });
        }
      },
    } as unknown as NodeLink;
    const pubsub = new PubSub({
      nodeId: "self",
      maxHops: 3,
      links: () => [link],
      local: (topic, data, origin) => local.push({ topic, data, origin }),
      drop: (error) => {
        dropped.push(error);
        pubsub.publish("retry", "data", undefined, undefined, "pub-2");
      },
    });

    pubsub.publish("topic", "data", undefined, undefined, "pub-1");

    await flush();
    await flush();

    assert.equal(sent.length, 1);
    assert.equal(sent[0].topic, "retry");
    assert.equal(dropped.length, 1);
  });

  test("evicts the oldest seen id once the cap is exceeded", async () => {
    const sent: Sent[] = [];
    const { pubsub } = makePubSub([makeLink("a", sent)]);

    for (let i = 0; i <= 1024; i += 1) {
      pubsub.publish("topic", "data", undefined, undefined, `pub-${i}`);
    }

    await flush();

    assert.equal(sent.length, 1025);

    pubsub.publish("topic", "data", undefined, undefined, "pub-0");
    await flush();

    assert.equal(sent.length, 1026);

    pubsub.publish("topic", "data", undefined, undefined, "pub-1024");
    await flush();

    assert.equal(sent.length, 1026);
  });
});
