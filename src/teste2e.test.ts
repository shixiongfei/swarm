/*
 * teste2e.test.ts
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
import { createNode } from "./node.ts";
import { SwarmError } from "./errors.ts";
import { Peer } from "./teste2e.ts";
import { waitFor, within } from "./testutil.ts";
import type { Node } from "./node.ts";

function tagged(node: Node, tag: string) {
  return node.call("worker", "tag").then(
    (value) => value === tag,
    () => false,
  );
}

test("calls across processes in both directions", async (t) => {
  const cluster = "e2e-calls";

  const parent = createNode({ cluster, discovery: false });
  t.after(() => parent.close());

  const { port } = await parent.listen("tcp://127.0.0.1:0");
  const url = `tcp://127.0.0.1:${port}`;

  parent.register("sum", { add: (_ctx, a: number, b: number) => a + b });

  const peers = ["a", "b"].map(
    (name) =>
      new Peer(`
const node = createNode({ cluster: ${JSON.stringify(cluster)}, discovery: false });
node.register("echo", { who: () => ${JSON.stringify(name)} });
await node.connect(${JSON.stringify(url)});
const deadline = Date.now() + 5000;
while (!node.registrations().some((instance) => instance.service === "sum")) {
  if (Date.now() > deadline) {
    throw new Error("sum was never published");
  }
  await new Promise((resolve) => setTimeout(resolve, 20));
}
const sum = await node.call("sum", "add", [1, 2]);
console.log(JSON.stringify({ name: ${JSON.stringify(name)}, sum }));
`),
  );

  t.after(() => Promise.all(peers.map((peer) => peer.stop())));

  for (const peer of peers) {
    const message = await peer.waitFor((report) => report.name !== undefined);

    assert.equal(message.sum, 3);
  }

  await waitFor(
    () =>
      parent.registrations().filter((instance) => instance.service === "echo")
        .length === 2,
  );

  const results = await parent.callAll("echo", "who");

  assert.equal(results.length, 2);
  assert.deepEqual(
    results.map((result) => (result.ok ? result.value : result.error)).sort(),
    ["a", "b"],
  );
});

test("recovers after a peer is killed and restarted", async (t) => {
  const cluster = "e2e-restart";

  const parent = createNode({ cluster, discovery: false });

  t.after(() => parent.close());

  const start = (tag: string, listenAt = "tcp://127.0.0.1:0") =>
    new Peer(`
const node = createNode({ cluster: ${JSON.stringify(cluster)}, discovery: false });
node.register("worker", { tag: () => ${JSON.stringify(tag)} });
const { port } = await node.listen(${JSON.stringify(listenAt)});
console.log(JSON.stringify({ tag: ${JSON.stringify(tag)}, url: "tcp://127.0.0.1:" + port }));
`);

  const first = start("one");
  t.after(() => first.stop());

  const ready = await first.waitFor((report) => report.tag === "one");
  const url = String(ready.url);

  parent.addSeed(url);
  await waitFor(() => tagged(parent, "one"), 5000);

  const down = new Promise<number>((resolve) =>
    parent.events.once("link-down", () => resolve(Date.now())),
  );

  await first.stop();

  const detected = await within(down, 1000);

  assert.ok(detected !== undefined, "link-down was not reported within 1s");

  const second = start("two", url);
  t.after(() => second.stop());

  const restarted = await second.waitFor((report) => report.tag === "two");

  assert.equal(restarted.url, url);
  await waitFor(() => tagged(parent, "two"), 8000);
});

test("keeps calling while a peer blocks its event loop", async (t) => {
  const cluster = "e2e-stall";

  const parent = createNode({
    cluster,
    discovery: false,
    maxInflight: 1,
  });

  t.after(() => parent.close());

  const { port } = await parent.listen("tcp://127.0.0.1:0");
  const url = `tcp://127.0.0.1:${port}`;

  parent.register("local", { ping: () => "pong" });

  const peer = new Peer(`
const node = createNode({ cluster: ${JSON.stringify(cluster)}, discovery: false });
let blocked = false;
node.register("stall", {
  spin: (_ctx, ms) => {
    if (!blocked) {
      blocked = true;
      const end = Date.now() + ms;
      while (Date.now() < end) {}
    }
    return "done";
  },
});
await node.connect(${JSON.stringify(url)});
console.log(JSON.stringify({ ready: true }));
`);

  t.after(() => peer.stop());

  await peer.waitFor((report) => report.ready !== undefined);
  await waitFor(() =>
    parent.registrations().some((instance) => instance.service === "stall"),
  );

  const first = parent
    .call("stall", "spin", [1500], { timeout: 400 })
    .catch((err: unknown) => err);
  const second = parent
    .call("stall", "spin", [0], { timeout: 400 })
    .catch((err: unknown) => err);

  const [timeout, congestion] = await Promise.all([first, second]);

  assert.ok(timeout instanceof SwarmError);
  assert.equal(timeout.code, "TIMEOUT");

  assert.ok(congestion instanceof SwarmError);
  assert.equal(congestion.code, "CONGESTED");

  assert.equal(await parent.call("local", "ping"), "pong");

  await waitFor(
    () =>
      parent
        .call("stall", "spin", [0], { timeout: 200 })
        .then((value) => value === "done", () => false),
    8000,
  );
});

test("close releases every subsystem so the process can exit", async (t) => {
  const peer = new Peer(`
const node = createNode({ cluster: "e2e-shutdown" });
await node.listen("tcp://127.0.0.1:0");
node.register("worker", { tag: () => "x" });
node.close();
console.log(JSON.stringify({ closed: true }));
`);

  t.after(() => peer.stop());

  await peer.waitFor((report) => report.closed !== undefined);

  assert.equal(await peer.waitForExit(3000), 0);
});
