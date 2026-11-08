/*
 * wsserver.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createServer as createHttpServer } from "node:http";
import { test } from "node:test";
import { WebSocket } from "ws";
import { SwarmError } from "./errors.ts";
import { WsServer } from "./wsserver.ts";
import type { AddressInfo } from "node:net";

const host = "127.0.0.1";

function openSocket(port: number, path = "/") {
  return new Promise<WebSocket>((resolve, reject) => {
    const socket = new WebSocket(`ws://${host}:${port}${path}`);

    socket.once("open", () => resolve(socket));
    socket.once("error", reject);
  });
}

async function waitFor(check: () => boolean, timeout = 2000) {
  const deadline = Date.now() + timeout;

  while (!check()) {
    if (Date.now() > deadline) {
      throw new Error("timed out");
    }

    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
}

test("accept emits online and tracks the connection", async () => {
  const server = new WsServer(host, 0);
  const online = new Promise<WebSocket>((resolve) => {
    server.events.once("online", resolve);
  });

  await server.listen();

  assert.notEqual(server.port, 0);

  const client = await openSocket(server.port);
  const accepted = await online;

  assert.equal(server.size, 1);
  assert.equal(server.connections.length, 1);
  assert.equal(server.connections[0], accepted);

  const offline = new Promise<WebSocket>((resolve) => {
    server.events.once("offline", resolve);
  });

  client.terminate();

  assert.equal(await offline, accepted);
  assert.equal(server.size, 0);

  server.close();
});

test("attaches to an existing http server on a path", async (t) => {
  const http = createHttpServer((_request, response) => response.end("ok"));
  const port = await new Promise<number>((resolve) => {
    http.listen(0, host, () => resolve((http.address() as AddressInfo).port));
  });
  const server = new WsServer(host, 0, { server: http, path: "/swarm" });
  const online = new Promise<WebSocket>((resolve) => {
    server.events.once("online", resolve);
  });

  t.after(() => {
    server.close();
    http.close();
  });

  await server.listen();

  assert.equal(server.port, port);
  assert.equal(server.host, host);

  const client = await openSocket(port, "/swarm");
  const accepted = await online;

  assert.equal(server.size, 1);
  assert.equal(server.connections[0], accepted);

  await assert.rejects(() => openSocket(port, "/other"));

  server.close();
  client.terminate();

  assert.equal(http.listening, true);
});

test("listen rejects when the attached http server is not listening", async () => {
  const http = createHttpServer();
  const server = new WsServer(host, 0, { server: http });

  await assert.rejects(
    () => server.listen(),
    (error: unknown) =>
      error instanceof SwarmError && error.kind === "rejected",
  );

  server.close();
});

test("close stops listening and drops every connection", async () => {
  const server = new WsServer(host, 0);
  const accepted: WebSocket[] = [];

  server.events.on("online", (socket) => accepted.push(socket));

  await server.listen();

  const first = await openSocket(server.port);
  const second = await openSocket(server.port);

  await waitFor(() => server.size === 2);

  const closed = Promise.all([
    new Promise<void>((resolve) => first.once("close", () => resolve())),
    new Promise<void>((resolve) => second.once("close", () => resolve())),
  ]);

  const offline: WebSocket[] = [];
  const dropped = new Promise<void>((resolve) => {
    server.events.on("offline", (socket) => {
      offline.push(socket);

      if (offline.length === 2) {
        resolve();
      }
    });
  });

  server.close();

  assert.equal(server.size, 0);
  assert.equal(accepted.length, 2);

  await dropped;

  assert.equal(offline.length, 2);

  await closed;
});

test("listen rejects when the port is already in use", async () => {
  const first = new WsServer(host, 0);

  await first.listen();

  const second = new WsServer(host, first.port);

  await assert.rejects(
    () => second.listen(),
    (error: unknown) => error instanceof SwarmError && error.kind === "failed",
  );

  first.close();
  second.close();
});

test("a throwing event handler crashes the process", () => {
  const script = `
import { WsServer } from "./wsserver.ts";
import { WsClient } from "./wsclient.ts";

const server = new WsServer("127.0.0.1", 0);

server.events.on("online", () => {
  throw new Error("boom-server");
});

await server.listen();

const client = new WsClient(\`ws://127.0.0.1:\${server.port}\`, { reconnect: false });

await client.connect();

setTimeout(() => console.log("survived"), 100);
`;

  const result = spawnSync(
    process.execPath,
    ["--input-type=module", "-e", script],
    {
      cwd: new URL(".", import.meta.url),
      encoding: "utf8",
      timeout: 10000,
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /boom-server/);
  assert.doesNotMatch(result.stdout, /survived/);
});
