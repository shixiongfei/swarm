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
import { test } from "node:test";
import { SwarmError } from "./errors.ts";
import { maxEnvelopeSize } from "./codec.ts";
import { WsClient } from "./wsclient.ts";
import { WsServer } from "./wsserver.ts";
import { waitFor, within } from "./testutil.ts";
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
