/*
 * nodehub.test.ts
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
import { Worker } from "node:worker_threads";
import type { Transferable } from "node:worker_threads";
import { Node } from "./node.ts";
import type { SwarmError } from "./errors.ts";
import type { WatchEvent } from "./registry.ts";

const source = new URL("./index.ts", import.meta.url).href;

type Role = "server" | "watcher" | "client" | "late" | "self";

function createWorker(role: Role) {
  const { port1, port2 } = new MessageChannel();
  const worker = new Worker(
    `
const { parentPort, workerData } = require("node:worker_threads");

(async () => {
  const { createNode } = await import(${JSON.stringify(source)});
  const node = createNode({ cluster: "hub-test" });
  const port = workerData.port;

  if (${JSON.stringify(role)} === "server") {
    node.register("echo", { ping: (value) => value * 2 });
    node.subscribe("news", (data) => parentPort.postMessage({ kind: "news", value: data }));
    node.attach(port);
    return;
  }

  if (${JSON.stringify(role)} === "watcher") {
    node.attach(port);
    node.watch("echo", (event) => parentPort.postMessage({ kind: "echo-" + event.type }));
    return;
  }

  if (${JSON.stringify(role)} === "late") {
    node.attach(port);
    node.register("late", { ping: () => 1 });
    return;
  }

  if (${JSON.stringify(role)} === "self") {
    node.register("self", { ping: () => 1 });
    node.watch("self", (event) => {
      if (event.type === "up") parentPort.postMessage({ kind: "self-up", value: event.instance.nodeId });
    });
    node.attach(port);
    return;
  }

  node.subscribe("news", (data) => parentPort.postMessage({ kind: "client-news", value: data }));
  node.attach(port);
  node.watch("echo", async (event) => {
    if (event.type !== "up") return;

    const value = await node.call("echo", "ping", [21]);
    parentPort.postMessage({ kind: "call", value });
    node.publish("news", "hello");
  });
})();
`,
    {
      eval: true,
      workerData: { port: port2 },
      transferList: [port2 as unknown as Transferable],
    },
  );

  return { worker, port: port1 };
}

function message(worker: Worker, kind: string) {
  return new Promise<unknown>((resolve) => {
    worker.on("message", (value) => {
      if (value.kind === kind) {
        resolve(value.value);
      }
    });
  });
}

function collect(worker: Worker, kind: string, values: unknown[]) {
  worker.on("message", (value) => {
    if (value.kind === kind) {
      values.push(value.value);
    }
  });
}

async function waitFor(check: () => boolean) {
  const deadline = Date.now() + 2000;

  while (!check()) {
    if (Date.now() > deadline) {
      throw new Error("timed out");
    }

    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
}

describe("NodeHub", () => {
  test(
    "routes calls and topics between workers",
    { timeout: 5000 },
    async (t) => {
      const node = new Node({ cluster: "hub-routes", callTimeout: 2000 });
      const server = createWorker("server");
      const client = createWorker("client");
      const serverNews: unknown[] = [];
      const clientNews: unknown[] = [];
      const mainNews: unknown[] = [];

      node.subscribe("news", (data) => mainNews.push(data));

      t.after(async () => {
        node.close();
        await Promise.allSettled([
          server.worker.terminate(),
          client.worker.terminate(),
        ]);
      });

      collect(server.worker, "news", serverNews);
      collect(client.worker, "client-news", clientNews);

      const up = new Promise<WatchEvent>((resolve) =>
        node.watch("echo", (event) => {
          if (event.type === "up") {
            resolve(event);
          }
        }),
      );
      const called = message(client.worker, "call");

      node.join(server.port);
      node.join(client.port);

      await up;

      assert.equal(await node.call("echo", "ping", [5]), 10);
      assert.equal(await called, 42);

      await waitFor(
        () =>
          serverNews.length === 1 &&
          clientNews.length === 1 &&
          mainNews.length === 1,
      );

      assert.deepEqual(serverNews, ["hello"]);
      assert.deepEqual(clientNews, ["hello"]);
      assert.deepEqual(mainNews, ["hello"]);

      node.publish("news", "from-main");

      await waitFor(
        () =>
          serverNews.length === 2 &&
          clientNews.length === 2 &&
          mainNews.length === 2,
      );

      assert.deepEqual(serverNews, ["hello", "from-main"]);
      assert.deepEqual(clientNews, ["hello", "from-main"]);
      assert.deepEqual(mainNews, ["hello", "from-main"]);
    },
  );

  test(
    "closes only the link of the worker that exits",
    { timeout: 5000 },
    async (t) => {
      const node = new Node({ cluster: "hub-exit", callTimeout: 1000 });
      const server = createWorker("server");
      const watcher = createWorker("watcher");

      t.after(async () => {
        node.close();
        await Promise.allSettled([
          server.worker.terminate(),
          watcher.worker.terminate(),
        ]);
      });

      const up = new Promise<WatchEvent>((resolve) =>
        node.watch("echo", (event) => {
          if (event.type === "up") {
            resolve(event);
          }
        }),
      );
      const down = new Promise<WatchEvent>((resolve) =>
        node.watch("echo", (event) => {
          if (event.type === "down") {
            resolve(event);
          }
        }),
      );
      const echoUp = message(watcher.worker, "echo-up");
      const echoDown = message(watcher.worker, "echo-down");

      node.join(server.port);
      node.join(watcher.port);

      const instance = (await up).instance;

      await echoUp;
      await server.worker.terminate();

      assert.equal((await down).instance.nodeId, instance.nodeId);
      await echoDown;

      await assert.rejects(
        node.call("echo", "ping", [1]),
        (err: SwarmError) => err.kind === "not-found",
      );
    },
  );
  test(
    "keeps a registration made right after attach",
    { timeout: 5000 },
    async (t) => {
      const node = new Node({ cluster: "hub-late", callTimeout: 1000 });
      const late = createWorker("late");

      t.after(async () => {
        node.close();
        await Promise.allSettled([late.worker.terminate()]);
      });

      const up = new Promise<WatchEvent>((resolve) =>
        node.watch("late", (event) => {
          if (event.type === "up") {
            resolve(event);
          }
        }),
      );

      node.join(late.port);

      assert.equal((await up).instance.service, "late");
      assert.equal(await node.call("late", "ping"), 1);
    },
  );

  test(
    "does not echo a worker's own service back",
    { timeout: 5000 },
    async (t) => {
      const node = new Node({ cluster: "hub-echo", callTimeout: 1000 });
      const worker = createWorker("self");
      const ups: unknown[] = [];

      t.after(async () => {
        node.close();
        await Promise.allSettled([worker.worker.terminate()]);
      });

      collect(worker.worker, "self-up", ups);

      node.join(worker.port);

      await waitFor(() => ups.length > 0);
      await new Promise<void>((resolve) => setTimeout(resolve, 100));

      assert.equal(ups.length, 1);
      assert.equal(await node.call("self", "ping"), 1);
    },
  );

  test("forwards the original publisher when relaying topics", async (t) => {
    const hub = new Node({ cluster: "origin-hub" });
    const b = new Node({ cluster: "origin-b" });
    const c = new Node({ cluster: "origin-c" });
    const chanB = new MessageChannel();
    const chanC = new MessageChannel();

    t.after(() => {
      hub.close();
      b.close();
      c.close();
    });

    b.register("b-ready", { ping: () => 1 });
    c.register("c-ready", { ping: () => 1 });

    const bReady = new Promise((resolve) => hub.watch("b-ready", resolve));
    const cReady = new Promise((resolve) => hub.watch("c-ready", resolve));

    hub.join(chanB.port1);
    b.attach(chanB.port2);
    hub.join(chanC.port1);
    c.attach(chanC.port2);

    await bReady;
    await cReady;

    const received = new Promise((resolve) =>
      c.subscribe("news", (data, from) => resolve({ data, from })),
    );
    const hubReceived = new Promise((resolve) =>
      hub.subscribe("news", (data, from) => resolve({ data, from })),
    );
    let bLocal = 0;
    b.subscribe("news", () => {
      bLocal += 1;
    });

    b.publish("news", "hello");

    assert.deepEqual(await received, { data: "hello", from: b.nodeId });
    assert.deepEqual(await hubReceived, { data: "hello", from: b.nodeId });
    assert.equal(bLocal, 1);
  });
});
