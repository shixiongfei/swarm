/*
 * linktcp.test.ts
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
import { connect, createServer } from "node:net";
import { LinkTcp } from "./linktcp.ts";
import { encode } from "./codec.ts";
import type { AddressInfo, Server, Socket } from "node:net";
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
  return new Promise<{ server: Server; socket: Socket; peer: Socket }>(
    (resolve) => {
      const server = createServer();

      server.listen(0, host, () => {
        const port = (server.address() as AddressInfo).port;
        const accepted = new Promise<Socket>((resolve) =>
          server.once("connection", resolve),
        );
        const socket = connect({ port, host });
        const connected = new Promise<void>((resolve) =>
          socket.once("connect", resolve),
        );

        Promise.all([accepted, connected]).then(([peer]) =>
          resolve({ server, socket, peer }),
        );
      });
    },
  );
}

test("sends envelopes over a socket", async (t) => {
  const { server, socket, peer } = await createPair();
  const client = new LinkTcp(socket, "client-node");
  const remote = new LinkTcp(peer, "peer-node");
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

test("decodes a frame split across chunks", async (t) => {
  const { server, socket, peer } = await createPair();
  const remote = new LinkTcp(peer, "peer-node");
  const received: Envelope[] = [];

  remote.events.on("envelope", (message) => received.push(message));

  t.after(() => {
    socket.destroy();
    remote.close();
    server.close();
  });

  const envelope: Envelope = {
    t: "call",
    id: "call-1",
    from: "client-node",
    to: "peer-node",
    service: "math",
    method: "add",
    args: [1, 2],
  };
  const payload = encode(envelope);
  const header = new Uint8Array(4);

  new DataView(header.buffer).setUint32(0, payload.byteLength, false);

  socket.write(header);
  socket.write(payload.subarray(0, 2));

  await new Promise<void>((resolve) => setTimeout(resolve, 10));

  socket.write(payload.subarray(2));

  await waitFor(() => received.length === 1);

  assert.deepEqual(received[0], envelope);
});

test("emits close once when the peer closes", async (t) => {
  const { server, socket, peer } = await createPair();
  const client = new LinkTcp(socket, "client-node");
  let closes = 0;

  client.events.on("close", () => {
    closes += 1;
  });

  t.after(() => {
    client.close();
    peer.destroy();
    server.close();
  });

  peer.destroy();

  await waitFor(() => closes === 1);
  await new Promise<void>((resolve) => setTimeout(resolve, 20));

  assert.equal(closes, 1);
});

test("ignores a send after close", async (t) => {
  const { server, socket, peer } = await createPair();
  const client = new LinkTcp(socket, "client-node");

  t.after(() => {
    peer.destroy();
    server.close();
  });

  client.close();

  assert.doesNotThrow(() => {
    client.send({ t: "pub", from: "client-node", topic: "news", data: 1 });
  });
});
