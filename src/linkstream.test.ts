/*
 * linkstream.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { connect, createServer } from "node:net";
import { LinkStream } from "./linkstream.ts";
import { decode, encode, maxEnvelopeSize } from "./codec.ts";
import { frame } from "./frame.ts";
import { waitFor } from "./testutil.ts";
import type { AddressInfo, Server, Socket } from "node:net";
import type { Envelope } from "./envelope.ts";

const host = "127.0.0.1";

function packet(envelope: Envelope) {
  const payload = encode(envelope);
  const bytes = new Uint8Array(4 + payload.byteLength);

  new DataView(bytes.buffer).setUint32(0, payload.byteLength, false);
  bytes.set(payload, 4);

  return bytes;
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

const pubs = (data: unknown, from = "client-node") => ({
  t: "pubs" as const,
  items: [
    { t: "pub" as const, id: "pub-1", ttl: 8, from, origin: from, topic: "news", data },
  ],
});

describe("LinkStream", () => {
  test("sends envelopes over a socket", async (t) => {
    const { server, socket, peer } = await createPair();
    const client = new LinkStream(socket, "client-node");
    const remote = new LinkStream(peer, "peer-node");
    const received: Envelope[] = [];

    remote.events.on("envelope", (message) => received.push(message));

    t.after(() => {
      client.close();
      remote.close();
      server.close();
    });

    client.send(frame(pubs(1)));
    client.send(frame(pubs(2)));

    await waitFor(() => received.length === 2);

    assert.deepEqual(received, [pubs(1), pubs(2)]);
  });

  test("sends a payload that spans every header byte", async (t) => {
    const { server, socket, peer } = await createPair();
    const client = new LinkStream(socket, "client-node");
    const remote = new LinkStream(peer, "peer-node");
    const received: Envelope[] = [];
    const envelope = pubs("a".repeat(70000));

    remote.events.on("envelope", (message) => received.push(message));

    t.after(() => {
      client.close();
      remote.close();
      server.close();
    });

    client.send(frame(envelope));

    await waitFor(() => received.length === 1);

    assert.deepEqual(received, [envelope]);
  });

  test("decodes a frame split across chunks", async (t) => {
    const { server, socket, peer } = await createPair();
    const remote = new LinkStream(peer, "peer-node");
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
    const remote = new LinkStream(peer, "peer-node");
    const received: Envelope[] = [];

    remote.events.on("envelope", (message) => received.push(message));

    t.after(() => {
      socket.destroy();
      remote.close();
      server.close();
    });

    const first: Envelope = pubs(1);
    const second: Envelope = pubs(2);
    const firstFrame = packet(first);
    const secondFrame = packet(second);
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
    const remote = new LinkStream(peer, "peer-node");
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

  test("closes when a frame header has the high bit set", async (t) => {
    const { server, socket, peer } = await createPair();
    const remote = new LinkStream(peer, "peer-node");
    let closes = 0;

    remote.events.on("close", () => {
      closes += 1;
    });

    t.after(() => {
      socket.destroy();
      server.close();
    });

    socket.write(new Uint8Array([0xff, 0xff, 0xff, 0xff]));

    await waitFor(() => closes === 1);
  });

  test("emits close once when the peer closes", async (t) => {
    const { server, socket, peer } = await createPair();
    const client = new LinkStream(socket, "client-node");
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
    const client = new LinkStream(socket, "client-node");

    t.after(() => {
      peer.destroy();
      server.close();
    });

    client.close();

    assert.doesNotThrow(() => {
      client.send(frame(pubs(1)));
    });
  });

  test("decodes a small frame after a large payload", async (t) => {
    const { server, socket, peer } = await createPair();
    const remote = new LinkStream(peer, "peer-node");
    const received: Envelope[] = [];

    remote.events.on("envelope", (message) => received.push(message));

    t.after(() => {
      socket.destroy();
      remote.close();
      server.close();
    });

    const large = pubs("a".repeat(1024 * 1024));
    const small = pubs(2);

    socket.write(packet(large));
    socket.write(packet(small));

    await waitFor(() => received.length === 2);

    assert.deepEqual(received, [large, small]);
  });

  test("decodes a trailing frame after a near-maximum frame", async (t) => {
    const { server, socket, peer } = await createPair();
    const remote = new LinkStream(peer, "peer-node");
    const received: string[] = [];
    let closes = 0;

    remote.events.on("envelope", (message) => {
      received.push(
        message.t === "pubs" && message.items[0].data === 2 ? "small" : "large",
      );
    });
    remote.events.on("close", () => {
      closes += 1;
    });

    t.after(() => {
      socket.destroy();
      remote.close();
      server.close();
    });

    const large = pubs("");
    const small = pubs(2);

    let size = maxEnvelopeSize - 1024;

    for (let i = 0; i < 8; i += 1) {
      large.items[0].data = "a".repeat(size);

      const length = encode(large).byteLength;

      if (length === maxEnvelopeSize) {
        break;
      }

      size += maxEnvelopeSize - length;
    }

    assert.equal(encode(large).byteLength, maxEnvelopeSize);

    const largeFrame = packet(large);
    const smallFrame = packet(small);
    const tail = new Uint8Array(1 + smallFrame.byteLength);

    tail.set(largeFrame.subarray(largeFrame.byteLength - 1));
    tail.set(smallFrame, 1);

    assert.ok(tail.byteLength > 4);

    socket.write(largeFrame.subarray(0, largeFrame.byteLength - 1));

    await new Promise<void>((resolve) => setTimeout(resolve, 50));

    socket.write(tail);

    await waitFor(() => received.length === 2);

    assert.deepEqual(received, ["large", "small"]);
    assert.equal(closes, 0);
  });

  test("close drains queued frames and ends the socket", async (t) => {
    const written: Uint8Array[] = [];
    let ended = 0;
    let destroyed = 0;

    const socket = {
      remoteAddress: "127.0.0.1",
      setNoDelay() {},
      on() {},
      once() {},
      off() {},
      write(frame: Uint8Array) {
        written.push(frame);
        return false;
      },
      end() {
        ended += 1;
      },
      destroy() {
        destroyed += 1;
      },
    } as unknown as Socket;

    const link = new LinkStream(socket, "client-node");

    t.after(() => link.close());

    link.send(frame({ t: "bye", from: "client-node" }));
    link.send(frame({ t: "pong", from: "client-node" }));

    assert.equal(written.length, 1);

    link.close();

    assert.equal(written.length, 2);
    assert.equal(ended, 1);
    assert.equal(destroyed, 0);

    await new Promise<void>((resolve) => setTimeout(resolve, 150));

    assert.equal(destroyed, 1);
  });

  test("flushes control frames ahead of queued bulk frames", (t) => {
    const written: Envelope[] = [];
    let onDrain: (() => void) | undefined;
    let writable = true;

    const socket = {
      remoteAddress: "127.0.0.1",
      setNoDelay() {},
      on(event: string, handler: () => void) {
        if (event === "drain") {
          onDrain = handler;
        }
      },
      once() {},
      off() {},
      write(bytes: Uint8Array) {
        written.push(decode(bytes.subarray(4))!);
        return writable;
      },
      end() {},
      destroy() {},
    } as unknown as Socket;

    const link = new LinkStream(socket, "client-node");

    t.after(() => link.close());

    writable = false;
    link.send(frame({ t: "ping", from: "client-node" }));
    link.send(
      frame({
        t: "changes",
        items: [{ t: "up", from: "peer", service: "svc" }],
      }),
    );
    link.send(frame({ t: "pong", from: "client-node" }));

    assert.equal(written.length, 1);
    assert.equal(written[0].t, "ping");

    writable = true;
    onDrain?.();

    assert.deepEqual(
      written.map((message) => message.t),
      ["ping", "pong", "changes"],
    );
  });

  test("drains a large backlog in order", (t) => {
    const written: Uint8Array[] = [];
    let onDrain: (() => void) | undefined;
    let accepting = false;

    const socket = {
      remoteAddress: "127.0.0.1",
      setNoDelay() {},
      on(event: string, handler: () => void) {
        if (event === "drain") {
          onDrain = handler;
        }
      },
      once() {},
      off() {},
      write(bytes: Uint8Array) {
        written.push(bytes);
        return accepting;
      },
      end() {},
      destroy() {},
    } as unknown as Socket;

    const link = new LinkStream(socket, "client-node");

    t.after(() => link.close());

    const count = 100000;

    for (let i = 0; i < count; i += 1) {
      link.send(frame({ t: "pong", from: `n${i}` }));
    }

    assert.equal(written.length, 1);

    const begun = performance.now();

    accepting = true;
    onDrain?.();

    const spent = performance.now() - begun;

    assert.equal(written.length, count);

    const mismatch = written.findIndex(
      (bytes, index) =>
        (decode(bytes.subarray(4)) as { from?: string } | undefined)?.from !==
        `n${index}`,
    );

    assert.equal(mismatch, -1, `packet ${mismatch} is out of order`);
    assert.ok(
      spent < 100,
      `draining ${count} packets took ${spent.toFixed(1)}ms`,
    );
  });

  test("sends bye after queued bulk frames on close", (t) => {
    const written: Envelope[] = [];
    const socket = {
      remoteAddress: "127.0.0.1",
      setNoDelay() {},
      on() {},
      once() {},
      off() {},
      write(bytes: Uint8Array) {
        written.push(decode(bytes.subarray(4))!);
        return false;
      },
      end() {},
      destroy() {},
      destroyed: false,
    } as unknown as Socket;

    const link = new LinkStream(socket, "client-node");
    t.after(() => link.close());

    link.send(frame({ t: "ping", from: "client-node" }));
    link.send(
      frame({
        t: "changes",
        items: [{ t: "up", from: "peer", service: "svc" }],
      }),
    );
    link.send(frame({ t: "bye", from: "client-node" }));
    link.close();

    assert.deepEqual(
      written.map((message) => message.t),
      ["ping", "changes", "bye"],
    );
  });

  test("treats unlisted frames as control frames", (t) => {
    const written: Envelope[] = [];
    let onDrain: (() => void) | undefined;
    let writable = true;

    const socket = {
      remoteAddress: "127.0.0.1",
      setNoDelay() {},
      on(event: string, handler: () => void) {
        if (event === "drain") {
          onDrain = handler;
        }
      },
      once() {},
      off() {},
      write(bytes: Uint8Array) {
        written.push(decode(bytes.subarray(4))!);
        return writable;
      },
      end() {},
      destroy() {},
    } as unknown as Socket;

    const link = new LinkStream(socket, "client-node");

    t.after(() => link.close());

    writable = false;
    link.send(frame({ t: "ping", from: "client-node" }));
    link.send(
      frame({
        t: "changes",
        items: [{ t: "up", from: "peer", service: "svc" }],
      }),
    );
    link.send(
      frame({
        t: "call",
        id: "1",
        from: "client-node",
        to: "peer",
        service: "svc",
        method: "run",
        args: [],
      }),
    );

    writable = true;
    onDrain?.();

    assert.deepEqual(
      written.map((message) => message.t),
      ["ping", "call", "changes"],
    );
  });
});
