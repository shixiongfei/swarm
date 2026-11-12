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
import { encode, maxEnvelopeSize } from "./codec.ts";
import { waitFor } from "./testutil.ts";
import type { AddressInfo } from "node:net";
import type { Envelope } from "./envelope.ts";
import type { SwarmError } from "./errors.ts";

const host = "127.0.0.1";

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

function fakeSocket() {
  const handlers = new Map<string, Set<(event: unknown) => void>>();
  const socket = {
    binaryType: "blob",
    bufferedAmount: 0,
    sent: [] as Uint8Array[],
    closed: false,
    addEventListener(type: string, handler: (event: unknown) => void) {
      const set = handlers.get(type) ?? new Set();

      set.add(handler);
      handlers.set(type, set);
    },
    removeEventListener(type: string, handler: (event: unknown) => void) {
      handlers.get(type)?.delete(handler);
    },
    send(payload: Uint8Array) {
      socket.sent.push(payload);
    },
    close() {
      socket.closed = true;
    },
    emit(type: string, event?: unknown) {
      for (const handler of [...(handlers.get(type) ?? [])]) {
        handler(event);
      }
    },
  };

  return socket;
}

function pubEnvelope(data: unknown): Envelope {
  return {
    t: "pub",
    id: "pub-1",
    ttl: 8,
    from: "client-node",
    origin: "client-node",
    topic: "news",
    data,
  };
}

test("sends envelopes over a socket", async (t) => {
  const { server, socket, peer } = await createPair();
  const client = new LinkWs(
    socket as unknown as WebSocket,
    "client-node",
    true,
  );
  const remote = new LinkWs(peer as unknown as WebSocket, "peer-node", true);
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
  const client = new LinkWs(
    socket as unknown as WebSocket,
    "client-node",
    true,
  );
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

  const link = new LinkWs(socket, "client-node", false);

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
  const client = new LinkWs(
    socket as unknown as WebSocket,
    "client-node",
    true,
  );

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

test("merges fragmented frames", () => {
  const socket = fakeSocket();
  const link = new LinkWs(socket as unknown as WebSocket, "client-node", true);
  const received: Envelope[] = [];
  const bytes = encode(pubEnvelope(1));

  link.events.on("envelope", (message) => received.push(message));

  socket.emit("message", { data: [bytes.slice(0, 2), bytes.slice(2)] });

  assert.deepEqual(received, [pubEnvelope(1)]);
});

test("ignores text and malformed frames", () => {
  const socket = fakeSocket();
  const link = new LinkWs(socket as unknown as WebSocket, "client-node", true);
  const received: Envelope[] = [];

  link.events.on("envelope", (message) => received.push(message));

  socket.emit("message", { data: "text" });
  socket.emit("message", { data: new Uint8Array([0xc1]) });
  socket.emit("message", {});

  assert.deepEqual(received, []);
});

test("ignores frames once closed", () => {
  const socket = fakeSocket();
  const link = new LinkWs(socket as unknown as WebSocket, "client-node", true);
  const received: Envelope[] = [];

  link.events.on("envelope", (message) => received.push(message));

  link.close();
  socket.emit("message", { data: encode(pubEnvelope(1)) });

  assert.deepEqual(received, []);
});

test("closes the link when the socket errors", () => {
  const socket = fakeSocket();
  const link = new LinkWs(socket as unknown as WebSocket, "client-node", true);
  let closes = 0;

  link.events.on("close", () => {
    closes += 1;
  });

  socket.emit("error");

  assert.equal(closes, 1);
  assert.equal(socket.closed, true);

  socket.emit("error");

  assert.equal(closes, 1);
});

test("throws when the envelope is too large", () => {
  const socket = fakeSocket();
  const link = new LinkWs(socket as unknown as WebSocket, "client-node", true);

  assert.throws(
    () => link.send(pubEnvelope("x".repeat(maxEnvelopeSize))),
    (err: SwarmError) =>
      err.kind === "failed" && err.message === "envelope is too large",
  );
  assert.equal(socket.sent.length, 0);
});
