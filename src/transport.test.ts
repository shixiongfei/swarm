/*
 * transport.test.ts
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
import { createConnection, createServer } from "node:net";
import { WebSocket } from "ws";
import { Transport } from "./transport.ts";
import { waitFor, within } from "./testutil.ts";
import type { SwarmError } from "./errors.ts";
import type { Link } from "./link.ts";
import type { Socket, AddressInfo } from "node:net";

type Attach = {
  link: Link;
  secret: string | undefined;
  expected: string | undefined;
  addr: string | undefined;
  auto: boolean | undefined;
};

function makeTransport() {
  const joins: Link[] = [];
  const attaches: Attach[] = [];
  const errors: Error[] = [];

  const transport = new Transport({
    nodeId: "node-1",
    join: (link) => joins.push(link),
    attach: (link, secret, expected, addr, auto) => {
      attaches.push({ link, secret, expected, addr, auto });
    },
    onError: (error) => errors.push(error),
  });

  return { transport, joins, attaches, errors };
}

function closeLinks(...arrays: Link[][]) {
  for (const links of arrays) {
    for (const link of links) {
      link.close();
    }
  }
}

describe("Transport", () => {
  test("open listens on tcp and dispose closes the server", async () => {
    const { transport } = makeTransport();
    const server = await transport.open("tcp://127.0.0.1:0");

    assert.equal(server.port > 0, true);

    transport.dispose();

    await assert.rejects(server.listen());
  });

  test("close closes a server and forgets it", async (t) => {
    const { transport } = makeTransport();
    const server = await transport.open("tcp://127.0.0.1:0");
    const port = server.port;

    t.after(() => transport.dispose());

    transport.close(server);
    transport.close(server);

    await assert.rejects(server.listen());

    const again = await transport.open(`tcp://127.0.0.1:${port}`);

    assert.equal(again.port, port);
  });

  test("a server error is reported without closing the listener", async (t) => {
    const { transport, joins, errors } = makeTransport();
    const tcpSockets: Socket[] = [];
    const wsSockets: WebSocket[] = [];

    t.after(() => {
      for (const socket of tcpSockets) {
        socket.destroy();
      }

      for (const socket of wsSockets) {
        socket.terminate();
      }

      closeLinks(joins);
      transport.dispose();
    });

    const tcp = await transport.open("tcp://127.0.0.1:0");
    const ws = await transport.open("ws://127.0.0.1:0");

    for (const server of [tcp, ws]) {
      server.events.emit("error", new Error("boom"));
    }

    assert.deepEqual(
      errors.map((error) => error.message),
      ["boom", "boom"],
    );

    await tcp.listen();
    await ws.listen();

    tcpSockets.push(createConnection({ port: tcp.port, host: "127.0.0.1" }));
    wsSockets.push(new WebSocket(`ws://127.0.0.1:${ws.port}`));

    await waitFor(() => joins.length === 2);
  });

  test("open rejects web socket options on a tcp url", async (t) => {
    const { transport } = makeTransport();

    t.after(() => transport.dispose());

    await assert.rejects(
      transport.open("tcp://127.0.0.1:0", { ws: {} }),
      (err: SwarmError) =>
        err.kind === "rejected" &&
        err.message === "tcp listen does not support http",
    );
  });

  test("open rejects wss without tls options", async (t) => {
    const { transport } = makeTransport();

    t.after(() => transport.dispose());

    await assert.rejects(
      transport.open("wss://127.0.0.1:0"),
      (err: SwarmError) =>
        err.kind === "rejected" &&
        err.message === "wss listen requires tls options",
    );
  });

  test("open rejects a port that is already in use", async (t) => {
    const blocker = createServer();

    await new Promise<void>((resolve) =>
      blocker.listen(0, "127.0.0.1", resolve),
    );

    const { port } = blocker.address() as AddressInfo;

    const { transport } = makeTransport();

    t.after(() => {
      transport.dispose();
      blocker.close();
    });

    await assert.rejects(transport.open(`tcp://127.0.0.1:${port}`));
  });

  test("connect tracks a client and attaches it with the url", async (t) => {
    const hub = makeTransport();
    const peer = makeTransport();
    const server = await hub.transport.open("tcp://127.0.0.1:0");

    t.after(() => {
      closeLinks(
        hub.joins,
        peer.attaches.map((entry) => entry.link),
      );
      peer.transport.dispose();
      hub.transport.dispose();
    });

    await peer.transport.connect(`tcp://127.0.0.1:${server.port}`);

    assert.equal(hub.joins.length, 1);
    assert.equal(peer.attaches.length, 1);
    assert.equal(peer.attaches[0].expected, undefined);
    assert.equal(peer.attaches[0].addr, `tcp://127.0.0.1:${server.port}`);
  });

  test("a client that was never adopted does not block connect", async (t) => {
    const hub = makeTransport();
    const peer = makeTransport();
    const server = await hub.transport.open("tcp://127.0.0.1:0");
    const url = `tcp://127.0.0.1:${server.port}`;
    const abandoned = await peer.transport.createClient(url);

    t.after(() => {
      closeLinks(
        hub.joins,
        peer.attaches.map((entry) => entry.link),
      );
      peer.transport.dispose();
      hub.transport.dispose();
    });

    abandoned.close();

    await peer.transport.connect(url);

    assert.equal(peer.attaches.length, 1);
    await waitFor(() => hub.joins.length === 1);
  });

  test("concurrent connect calls share one client", async (t) => {
    const hub = makeTransport();
    const peer = makeTransport();
    const server = await hub.transport.open("tcp://127.0.0.1:0");
    const url = `tcp://127.0.0.1:${server.port}`;

    t.after(() => {
      closeLinks(
        hub.joins,
        peer.attaches.map((entry) => entry.link),
      );
      peer.transport.dispose();
      hub.transport.dispose();
    });

    await Promise.all([
      peer.transport.connect(url),
      peer.transport.connect(url),
    ]);

    assert.equal(peer.attaches.length, 1);
    await waitFor(() => hub.joins.length === 1);
  });

  test("a later caller can abort its wait without closing the shared connect", async (t) => {
    const peer = makeTransport();
    const sockets: Socket[] = [];
    const server = createServer((socket) => sockets.push(socket));

    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );

    const port = (server.address() as { port: number }).port;
    const url = `ws://127.0.0.1:${port}`;

    t.after(() => {
      peer.transport.dispose();
      for (const socket of sockets) {
        socket.destroy();
      }
      server.close();
    });

    const first = peer.transport.connect(url).then(
      () => "connected",
      (error: SwarmError) => error.kind,
    );

    const controller = new AbortController();
    const second = peer.transport
      .connect(url, { signal: controller.signal })
      .then(
        () => "connected",
        (error: SwarmError) => error.kind,
      );

    await waitFor(() => sockets.length === 1);
    controller.abort();

    assert.equal(await second, "cancelled");
    assert.equal(await within(first, 50), undefined);
  });

  test("createClient attaches with the expected peer id", async (t) => {
    const hub = makeTransport();
    const peer = makeTransport();
    const server = await hub.transport.open("tcp://127.0.0.1:0");
    const url = `tcp://127.0.0.1:${server.port}`;
    const client = await peer.transport.createClient(url, undefined, "peer-7");

    t.after(() => {
      client.close();
      closeLinks(
        hub.joins,
        peer.attaches.map((entry) => entry.link),
      );
      peer.transport.dispose();
      hub.transport.dispose();
    });

    await client.connect();

    assert.equal(peer.attaches.length, 1);
    assert.equal(peer.attaches[0].expected, "peer-7");
  });
});
