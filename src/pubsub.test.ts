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
import { test } from "node:test";
import { PubSub } from "./pubsub.ts";
import type { SwarmError } from "./errors.ts";
import type { NodeLink } from "./nodelink.ts";

type Sent = {
  topic: string;
  data: unknown;
  origin: string;
  id: string;
  ttl: number;
};

function makeLink(peerId: string, sent: Sent[], fail?: Error) {
  return {
    peerId,
    publish(
      topic: string,
      data: unknown,
      origin: string,
      id: string,
      ttl: number,
    ) {
      if (fail !== undefined) {
        throw fail;
      }

      sent.push({ topic, data, origin, id, ttl });
    },
  } as unknown as NodeLink;
}

function makePubSub(links: NodeLink[], maxHops = 3) {
  const local: Array<{ topic: string; data: unknown; origin: string }> = [];

  const pubsub = new PubSub({
    nodeId: "self",
    maxHops,
    links: () => links,
    local: (topic, data, origin) => local.push({ topic, data, origin }),
  });

  return { pubsub, local };
}

test("publish forwards to every link and publishes locally", () => {
  const first: Sent[] = [];
  const second: Sent[] = [];
  const { pubsub, local } = makePubSub([
    makeLink("a", first),
    makeLink("b", second),
  ]);

  pubsub.publish("topic", { value: 1 }, undefined, undefined, "pub-1");

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

test("publish skips the link the message came from", () => {
  const first: Sent[] = [];
  const second: Sent[] = [];
  const { pubsub } = makePubSub([makeLink("a", first), makeLink("b", second)]);

  pubsub.publish("topic", "data", "origin", "a", "pub-1");

  assert.equal(first.length, 0);
  assert.equal(second.length, 1);
  assert.equal(second[0].origin, "origin");
});

test("publish sends a given id only once", () => {
  const sent: Sent[] = [];
  const { pubsub, local } = makePubSub([makeLink("a", sent)]);

  pubsub.publish("topic", "data", undefined, undefined, "pub-1");
  pubsub.publish("topic", "data", undefined, undefined, "pub-1");

  assert.equal(sent.length, 1);
  assert.equal(local.length, 1);
});

test("publish does not forward when the ttl is exhausted", () => {
  const sent: Sent[] = [];
  const { pubsub, local } = makePubSub([makeLink("a", sent)]);

  pubsub.publish("topic", "data", undefined, undefined, "pub-1", 0);

  assert.equal(sent.length, 0);
  assert.equal(local.length, 1);
});

test("publish maps a clone failure to a rejected swarm error", () => {
  const error = Object.assign(new Error("clone"), { name: "DataCloneError" });
  const { pubsub, local } = makePubSub([makeLink("a", [], error)]);

  assert.throws(
    () => pubsub.publish("topic", () => {}, undefined, undefined, "pub-1"),
    (err: SwarmError) =>
      err.kind === "failed" && err.message === "data is not cloneable",
  );
  assert.equal(local.length, 0);
});

test("close clears the seen ids so they can be published again", () => {
  const sent: Sent[] = [];
  const { pubsub } = makePubSub([makeLink("a", sent)]);

  pubsub.publish("topic", "data", undefined, undefined, "pub-1");
  pubsub.close();
  pubsub.publish("topic", "data", undefined, undefined, "pub-1");

  assert.equal(sent.length, 2);
});

test("evicts the oldest seen id once the cap is exceeded", () => {
  const sent: Sent[] = [];
  const { pubsub } = makePubSub([makeLink("a", sent)]);

  for (let i = 0; i <= 1024; i += 1) {
    pubsub.publish("topic", "data", undefined, undefined, `pub-${i}`);
  }

  assert.equal(sent.length, 1025);

  pubsub.publish("topic", "data", undefined, undefined, "pub-0");

  assert.equal(sent.length, 1026);

  pubsub.publish("topic", "data", undefined, undefined, "pub-1024");

  assert.equal(sent.length, 1026);
});
