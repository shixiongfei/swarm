/*
 * tcpclient.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { MusterError } from "./errors.ts";
import { TcpClient } from "./tcpclient.ts";
import { TcpServer } from "./tcpserver.ts";
import type { Socket } from "node:net";

const host = "127.0.0.1";

async function waitFor(check: () => boolean, timeout = 2000) {
  const deadline = Date.now() + timeout;

  while (!check()) {
    if (Date.now() > deadline) {
      throw new Error("timed out");
    }

    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
}

test("connect emits online and close emits offline", async () => {
  const server = new TcpServer(host, 0);

  await server.listen();

  const client = new TcpClient(host, server.port, { reconnect: false });
  const online = new Promise<Socket>((resolve) => {
    client.events.once("online", resolve);
  });
  const offline = new Promise<Socket>((resolve) => {
    client.events.once("offline", resolve);
  });

  await client.connect();

  const socket = await online;

  assert.equal(client.socket, socket);
  assert.equal(client.connected, true);

  client.close();

  assert.equal(await offline, socket);
  assert.equal(client.socket, undefined);
  assert.equal(client.connected, false);

  server.close();
});

test("reconnects after an unexpected disconnect", async () => {
  const server = new TcpServer(host, 0);
  const accepted: Socket[] = [];

  server.events.on("online", (socket) => accepted.push(socket));

  await server.listen();

  const client = new TcpClient(host, server.port, {
    retryDelay: 10,
    maxRetryDelay: 20,
  });

  const online: Socket[] = [];
  const retry: Array<{ attempt: number; delay: number }> = [];

  client.events.on("online", (socket) => online.push(socket));
  client.events.on("retry", (attempt, delay) => retry.push({ attempt, delay }));

  await client.connect();
  await waitFor(() => accepted.length === 1);

  const offline = new Promise<void>((resolve) => {
    client.events.once("offline", () => resolve());
  });
  const reconnected = new Promise<Socket>((resolve) => {
    client.events.once("online", resolve);
  });

  accepted[0].destroy();

  await offline;

  const socket = await reconnected;

  assert.equal(online.length, 2);
  assert.equal(client.socket, socket);
  assert.deepEqual(retry[0], { attempt: 1, delay: 10 });

  client.close();
  server.close();
});

test("close stops reconnect", async () => {
  const server = new TcpServer(host, 0);
  const accepted: Socket[] = [];

  server.events.on("online", (socket) => accepted.push(socket));

  await server.listen();

  const client = new TcpClient(host, server.port, { retryDelay: 10 });
  let retries = 0;

  client.events.on("retry", () => {
    retries += 1;
  });

  await client.connect();
  await waitFor(() => accepted.length === 1);

  const offline = new Promise<void>((resolve) => {
    client.events.once("offline", () => resolve());
  });

  accepted[0].destroy();

  await offline;

  client.close();

  await new Promise<void>((resolve) => setTimeout(resolve, 30));

  assert.equal(retries, 1);
  assert.equal(accepted.length, 1);

  server.close();
});

test("connect rejects when the server is unavailable", async () => {
  const server = new TcpServer(host, 0);

  await server.listen();

  const port = server.port;

  server.close();

  const client = new TcpClient(host, port, { reconnect: false });

  await assert.rejects(
    () => client.connect(),
    (error: unknown) =>
      error instanceof MusterError && error.kind === "failed",
  );

  client.close();
});
