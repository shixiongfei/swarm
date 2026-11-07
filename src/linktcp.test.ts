/*
 * linktcp.test.ts
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
import { connect, createServer } from "node:net";
import { LinkTcp } from "./linktcp.ts";
import { encode, maxEnvelopeSize } from "./codec.ts";
import type { AddressInfo, Server, Socket } from "node:net";
import type { Envelope } from "./envelope.ts";

const host = "127.0.0.1";

function frame(envelope: Envelope) {
  const payload = encode(envelope);
  const bytes = new Uint8Array(4 + payload.byteLength);

  new DataView(bytes.buffer).setUint32(0, payload.byteLength, false);
  bytes.set(payload, 4);

  return bytes;
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

  client.send({
    t: "pub",
    from: "client-node",
    origin: "client-node",
    topic: "news",
    data: 1,
  });
  client.send({
    t: "pub",
    from: "client-node",
    origin: "client-node",
    topic: "news",
    data: 2,
  });

  await waitFor(() => received.length === 2);

  assert.deepEqual(received, [
    {
      t: "pub",
      from: "client-node",
      origin: "client-node",
      topic: "news",
      data: 1,
    },
    {
      t: "pub",
      from: "client-node",
      origin: "client-node",
      topic: "news",
      data: 2,
    },
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

test("decodes a whole frame followed by a partial frame", async (t) => {
  const { server, socket, peer } = await createPair();
  const remote = new LinkTcp(peer, "peer-node");
  const received: Envelope[] = [];

  remote.events.on("envelope", (message) => received.push(message));

  t.after(() => {
    socket.destroy();
    remote.close();
    server.close();
  });

  const first: Envelope = {
    t: "pub",
    from: "client-node",
    origin: "client-node",
    topic: "news",
    data: 1,
  };
  const second: Envelope = {
    t: "pub",
    from: "client-node",
    origin: "client-node",
    topic: "news",
    data: 2,
  };
  const firstFrame = frame(first);
  const secondFrame = frame(second);
  const combined = new Uint8Array(
    firstFrame.byteLength + secondFrame.byteLength,
  );

  combined.set(firstFrame, 0);
  combined.set(secondFrame, firstFrame.byteLength);

  socket.write(combined.subarray(0, combined.byteLength - 2));

  await waitFor(() => received.length === 1);

  socket.write(combined.subarray(combined.byteLength - 2));

  await waitFor(() => received.length === 2);

  assert.deepEqual(received, [first, second]);
});

test("closes when a frame header exceeds the envelope limit", async (t) => {
  const { server, socket, peer } = await createPair();
  const remote = new LinkTcp(peer, "peer-node");
  let closes = 0;

  remote.events.on("close", () => {
    closes += 1;
  });

  t.after(() => {
    socket.destroy();
    server.close();
  });

  const header = new Uint8Array(4);

  new DataView(header.buffer).setUint32(0, maxEnvelopeSize + 1, false);

  socket.write(header);

  await waitFor(() => closes === 1);
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
    client.send({
      t: "pub",
      from: "client-node",
      origin: "client-node",
      topic: "news",
      data: 1,
    });
  });
});

test("decodes a small frame after a large payload", async (t) => {
  const { server, socket, peer } = await createPair();
  const remote = new LinkTcp(peer, "peer-node");
  const received: Envelope[] = [];

  remote.events.on("envelope", (message) => received.push(message));

  t.after(() => {
    socket.destroy();
    remote.close();
    server.close();
  });

  const large: Envelope = {
    t: "pub",
    from: "client-node",
    origin: "client-node",
    topic: "news",
    data: "a".repeat(1024 * 1024),
  };
  const small: Envelope = {
    t: "pub",
    from: "client-node",
    origin: "client-node",
    topic: "news",
    data: 2,
  };

  socket.write(frame(large));
  socket.write(frame(small));

  await waitFor(() => received.length === 2);

  assert.deepEqual(received, [large, small]);
});
