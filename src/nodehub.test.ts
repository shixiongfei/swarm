/*
 * nodehub.test.ts
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
import { Worker } from "node:worker_threads";
import type { Transferable } from "node:worker_threads";
import { Node } from "./node.ts";
import type { MusterError } from "./errors.ts";
import type { WatchEvent } from "./registry.ts";

const source = new URL("./index.ts", import.meta.url).href;

type Role = "server" | "watcher" | "client";

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

      await waitFor(() => serverNews.length === 1 && clientNews.length === 1);

      assert.deepEqual(serverNews, ["hello"]);
      assert.deepEqual(clientNews, ["hello"]);

      node.publish("news", "from-main");

      await waitFor(() => serverNews.length === 2 && clientNews.length === 2);

      assert.deepEqual(serverNews, ["hello", "from-main"]);
      assert.deepEqual(clientNews, ["hello", "from-main"]);
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
        (err: MusterError) => err.kind === "not-found",
      );
    },
  );
});
