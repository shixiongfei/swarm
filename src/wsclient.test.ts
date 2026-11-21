/*
 * wsclient.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import assert from "node:assert/strict";
import { createServer } from "node:net";
import { test } from "node:test";
import { SwarmError } from "./errors.ts";
import { maxEnvelopeSize } from "./codec.ts";
import { WsClient } from "./wsclient.ts";
import { WsServer } from "./wsserver.ts";
import { freePort, waitFor, within } from "./testutil.ts";
import type { AddressInfo, Socket } from "node:net";
import type { WebSocket as ServerSocket } from "ws";

const host = "127.0.0.1";

test("connect emits online and close emits offline", async () => {
  const server = new WsServer(host, 0);

  await server.listen();

  const client = new WsClient(`ws://${host}:${server.port}`, {
    reconnect: false,
  });
  const online = new Promise<WebSocket>((resolve) => {
    client.events.once("online", resolve);
  });
  const offline = new Promise<WebSocket>((resolve) => {
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
  const server = new WsServer(host, 0);
  const accepted: ServerSocket[] = [];

  server.events.on("online", (socket) => accepted.push(socket));

  await server.listen();

  const client = new WsClient(`ws://${host}:${server.port}`, {
    retryDelay: 10,
    maxRetryDelay: 20,
  });

  const online: WebSocket[] = [];

  client.events.on("online", (socket) => online.push(socket));

  await client.connect();
  await waitFor(() => accepted.length === 1);

  const offline = new Promise<void>((resolve) => {
    client.events.once("offline", () => resolve());
  });
  const reconnected = new Promise<WebSocket>((resolve) => {
    client.events.once("online", resolve);
  });

  accepted[0].terminate();

  await offline;

  const socket = await reconnected;

  assert.equal(online.length, 2);
  assert.equal(client.socket, socket);

  client.close();
  server.close();
});

test("close stops reconnect", async () => {
  const server = new WsServer(host, 0);
  const accepted: ServerSocket[] = [];

  server.events.on("online", (socket) => accepted.push(socket));

  await server.listen();

  const client = new WsClient(`ws://${host}:${server.port}`, {
    retryDelay: 10,
  });

  await client.connect();
  await waitFor(() => accepted.length === 1);

  const offline = new Promise<void>((resolve) => {
    client.events.once("offline", () => resolve());
  });

  accepted[0].terminate();

  await offline;

  client.close();

  await new Promise<void>((resolve) => setTimeout(resolve, 30));

  assert.equal(accepted.length, 1);

  server.close();
});

test("close during the tls connect rejects with CLOSED and never emits online", async () => {
  const server = new WsServer(host, 0);

  await server.listen();

  const client = new WsClient(`ws://${host}:${server.port}`, {
    tls: {},
    reconnect: false,
  });
  const online: WebSocket[] = [];

  client.events.on("online", (socket) => online.push(socket));

  const connecting = client.connect();

  client.close();

  await assert.rejects(
    connecting,
    (error: SwarmError) => error.kind === "rejected" && error.code === "CLOSED",
  );

  assert.deepEqual(online, []);
  assert.equal(client.socket, undefined);
  assert.equal(server.size, 0);

  server.close();
});

test("caps the tls payload at the envelope size", async () => {
  const server = new WsServer(host, 0);

  await server.listen();

  const client = new WsClient(`ws://${host}:${server.port}`, {
    tls: {},
    reconnect: false,
  });

  await client.connect();
  await waitFor(() => server.size === 1);

  const accepted = server.connections[0];

  assert.ok(accepted);

  const offline = new Promise<boolean>((resolve) => {
    client.events.on("offline", () => resolve(true));
  });

  accepted.send("x".repeat(maxEnvelopeSize + 1));

  assert.equal(await within(offline, 2000), true);

  client.close();
  server.close();
});

test("connect rejects when the server is unavailable", async () => {
  const server = new WsServer(host, 0);

  await server.listen();

  const port = server.port;

  server.close();

  const client = new WsClient(`ws://${host}:${port}`, { reconnect: false });

  await assert.rejects(
    () => client.connect(),
    (error: unknown) => error instanceof SwarmError && error.kind === "failed",
  );

  client.close();
});

test("abort cancels a pending connect", async (t) => {
  const sockets: Socket[] = [];
  const server = createServer((socket) => sockets.push(socket));

  await new Promise<void>((resolve) => server.listen(0, host, resolve));

  const address = server.address() as AddressInfo;
  const controller = new AbortController();
  const client = new WsClient(`ws://${host}:${address.port}`, {
    reconnect: false,
    signal: controller.signal,
  });

  t.after(() => {
    client.close();

    for (const socket of sockets) {
      socket.destroy();
    }

    server.close();
  });

  const connecting = client.connect();

  await waitFor(() => client.socket !== undefined);

  controller.abort();

  await assert.rejects(
    connecting,
    (error: unknown) =>
      error instanceof SwarmError && error.code === "CANCELLED",
  );

  assert.equal(client.socket, undefined);
});

test("abort stops reconnect after a failed connect", async (t) => {
  const port = await freePort();
  const controller = new AbortController();
  const client = new WsClient(`ws://${host}:${port}`, {
    retryDelay: 10,
    maxRetryDelay: 10,
    signal: controller.signal,
  });

  await assert.rejects(
    () => client.connect(),
    (error: unknown) => error instanceof SwarmError && error.kind === "failed",
  );

  controller.abort();

  const server = new WsServer(host, port);

  t.after(() => {
    client.close();
    server.close();
  });

  await server.listen();

  await new Promise<void>((resolve) => setTimeout(resolve, 100));

  assert.equal(server.size, 0);
});

test("connect rejects immediately when the signal is already aborted", async () => {
  const controller = new AbortController();
  const client = new WsClient(`ws://${host}:1`, {
    reconnect: false,
    signal: controller.signal,
  });

  controller.abort();

  await assert.rejects(
    () => client.connect(),
    (error: unknown) =>
      error instanceof SwarmError && error.code === "CANCELLED",
  );

  client.close();
});

test("abort closes an established connection", async (t) => {
  const server = new WsServer(host, 0);

  await server.listen();

  const controller = new AbortController();
  const client = new WsClient(`ws://${host}:${server.port}`, {
    reconnect: false,
    signal: controller.signal,
  });

  t.after(() => {
    client.close();
    server.close();
  });

  const offline = new Promise<WebSocket>((resolve) => {
    client.events.once("offline", resolve);
  });

  await client.connect();

  controller.abort();

  assert.ok(await within(offline, 2000));
  assert.equal(client.socket, undefined);
});
