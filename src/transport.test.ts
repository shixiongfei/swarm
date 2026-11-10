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
import { test } from "node:test";
import { createServer } from "node:net";
import { Transport } from "./transport.ts";
import type { SwarmError } from "./errors.ts";
import type { Link } from "./link.ts";
import type { AddressInfo } from "node:net";

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
    attach: (link, secret, expected, addr, auto) =>
      attaches.push({ link, secret, expected, addr, auto }),
    onError: (error) => errors.push(error),
  });

  return { transport, joins, attaches, errors };
}

async function freePort() {
  const server = createServer();

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));

  const port = (server.address() as AddressInfo).port;

  await new Promise<void>((resolve) => server.close(() => resolve()));

  return port;
}

function closeLinks(...arrays: Link[][]) {
  for (const links of arrays) {
    for (const link of links) {
      link.close();
    }
  }
}

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

test("open forwards a server error and detaches the server", async (t) => {
  const { transport, errors } = makeTransport();
  const server = await transport.open("tcp://127.0.0.1:0");

  t.after(() => transport.dispose());

  server.events.emit("error", new Error("boom"));

  assert.equal(errors.length, 1);
  assert.equal(errors[0].message, "boom");
  await assert.rejects(server.listen());
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
  const port = await freePort();
  const blocker = createServer();

  await new Promise<void>((resolve) =>
    blocker.listen(port, "127.0.0.1", resolve),
  );

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
