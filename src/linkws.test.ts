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
import { WebSocket as WsWebSocket, WebSocketServer } from "ws";
import { LinkWs } from "./linkws.ts";
import type { AddressInfo } from "node:net";
import type { Envelope } from "./envelope.ts";
import type { SwarmError } from "./errors.ts";

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
    socket: WsWebSocket;
    peer: WsWebSocket;
  }>((resolve) => {
    const server = new WebSocketServer({ host, port: 0 });
    const accepted = new Promise<WsWebSocket>((resolve) =>
      server.once("connection", resolve),
    );

    server.once("listening", () => {
      const port = (server.address() as AddressInfo).port;
      const socket = new WsWebSocket(`ws://${host}:${port}`);

      socket.once("open", async () => {
        resolve({ server, socket, peer: await accepted });
      });
    });
  });
}

test("sends envelopes over a socket", async (t) => {
  const { server, socket, peer } = await createPair();
  const client = new LinkWs(socket as unknown as WebSocket, "client-node");
  const remote = new LinkWs(peer as unknown as WebSocket, "peer-node");
  const received: Envelope[] = [];

  remote.events.on("envelope", (message) => received.push(message));

  t.after(() => {
    client.close();
    remote.close();
    server.close();
  });

  client.send({
    t: "pub",
    id: "pub-1",
    ttl: 8,
    from: "client-node",
    origin: "client-node",
    topic: "news",
    data: 1,
  });
  client.send({
    t: "pub",
    id: "pub-1",
    ttl: 8,
    from: "client-node",
    origin: "client-node",
    topic: "news",
    data: 2,
  });

  await waitFor(() => received.length === 2);

  assert.deepEqual(received, [
    {
      t: "pub",
      id: "pub-1",
      ttl: 8,
      from: "client-node",
      origin: "client-node",
      topic: "news",
      data: 1,
    },
    {
      t: "pub",
      id: "pub-1",
      ttl: 8,
      from: "client-node",
      origin: "client-node",
      topic: "news",
      data: 2,
    },
  ]);
});

test("emits close once when the peer closes", async (t) => {
  const { server, socket, peer } = await createPair();
  const client = new LinkWs(socket as unknown as WebSocket, "client-node");
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

test("rejects a send when the socket is congested", () => {
  const sent: Uint8Array[] = [];
  const socket = {
    bufferedAmount: 17 * 1024 * 1024,
    binaryType: "arraybuffer",
    send(payload: Uint8Array) {
      sent.push(payload);
    },
    addEventListener() {},
    removeEventListener() {},
    close() {},
  } as unknown as WebSocket;

  const link = new LinkWs(socket, "client-node");

  assert.throws(
    () =>
      link.send({
        t: "pub",
        id: "pub-1",
        ttl: 8,
        from: "client-node",
        origin: "client-node",
        topic: "news",
        data: 1,
      }),
    (err: SwarmError) =>
      err.kind === "failed" && err.message === "link is congested",
  );

  assert.equal(sent.length, 0);
});

test("ignores a send after close", async (t) => {
  const { server, socket, peer } = await createPair();
  const client = new LinkWs(socket as unknown as WebSocket, "client-node");

  t.after(() => {
    peer.terminate();
    server.close();
  });

  client.close();

  assert.doesNotThrow(() => {
    client.send({
      t: "pub",
      id: "pub-1",
      ttl: 8,
      from: "client-node",
      origin: "client-node",
      topic: "news",
      data: 1,
    });
  });
});
