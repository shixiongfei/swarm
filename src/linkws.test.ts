/*
 * linkws.test.ts
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
import { WebSocket, WebSocketServer } from "ws";
import { LinkWs } from "./linkws.ts";
import type { AddressInfo } from "node:net";
import type { Envelope } from "./envelope.ts";

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

function createPair() {
  return new Promise<{
    server: WebSocketServer;
    socket: WebSocket;
    peer: WebSocket;
  }>((resolve) => {
    const server = new WebSocketServer({ host, port: 0 });
    const accepted = new Promise<WebSocket>((resolve) =>
      server.once("connection", resolve),
    );

    server.once("listening", () => {
      const port = (server.address() as AddressInfo).port;
      const socket = new WebSocket(`ws://${host}:${port}`);

      socket.once("open", async () => {
        resolve({ server, socket, peer: await accepted });
      });
    });
  });
}

test("sends envelopes over a socket", async (t) => {
  const { server, socket, peer } = await createPair();
  const client = new LinkWs(socket, "client-node");
  const remote = new LinkWs(peer, "peer-node");
  const received: Envelope[] = [];

  remote.events.on("envelope", (message) => received.push(message));

  t.after(() => {
    client.close();
    remote.close();
    server.close();
  });

  client.send({ t: "pub", from: "client-node", topic: "news", data: 1 });
  client.send({ t: "pub", from: "client-node", topic: "news", data: 2 });

  await waitFor(() => received.length === 2);

  assert.deepEqual(received, [
    { t: "pub", from: "client-node", topic: "news", data: 1 },
    { t: "pub", from: "client-node", topic: "news", data: 2 },
  ]);
});

test("emits close once when the peer closes", async (t) => {
  const { server, socket, peer } = await createPair();
  const client = new LinkWs(socket, "client-node");
  let closes = 0;

  client.events.on("close", () => {
    closes += 1;
  });

  t.after(() => {
    client.close();
    peer.terminate();
    server.close();
  });

  peer.terminate();

  await waitFor(() => closes === 1);
  await new Promise<void>((resolve) => setTimeout(resolve, 20));

  assert.equal(closes, 1);
});

test("ignores a send after close", async (t) => {
  const { server, socket, peer } = await createPair();
  const client = new LinkWs(socket, "client-node");

  t.after(() => {
    peer.terminate();
    server.close();
  });

  client.close();

  assert.doesNotThrow(() => {
    client.send({ t: "pub", from: "client-node", topic: "news", data: 1 });
  });
});
