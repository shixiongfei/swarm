/*
 * dialer.test.ts
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
import { Dialer } from "./dialer.ts";
import type { Client } from "./client.ts";

type FakeClient = Client & { connectCount: number; closeCount: number };

function makeClient(): FakeClient {
  const client = {
    connectCount: 0,
    closeCount: 0,
    connect() {
      client.connectCount += 1;
      return Promise.resolve(client);
    },
    close() {
      client.closeCount += 1;
    },
  };

  return client as unknown as FakeClient;
}

function makeDialer(seeds: Array<string | URL> = []) {
  const created: Array<{ addr: string; id?: string }> = [];
  const clients: FakeClient[] = [];
  const tracked: FakeClient[] = [];
  let closed = false;

  const dialer = new Dialer({
    seeds,
    create: (addr, id) => {
      created.push({ addr, id });
      const client = makeClient();

      clients.push(client);

      return Promise.resolve(client);
    },
    track: (client) => tracked.push(client as unknown as FakeClient),
    untrack: (client) => {
      const index = tracked.indexOf(client as unknown as FakeClient);

      if (index >= 0) {
        tracked.splice(index, 1);
      }
    },
    isClosed: () => closed,
  });

  return {
    dialer,
    created,
    clients,
    tracked,
    close() {
      closed = true;
    },
  };
}

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

test("add ignores a duplicate seed and dials the new one", async (t) => {
  const { dialer, created } = makeDialer();
  const seed = "tcp://10.0.0.9:9";

  t.after(() => dialer.close());

  dialer.add(seed);
  dialer.add(seed);

  assert.deepEqual(dialer.list(), [seed]);
  assert.deepEqual(created, [{ addr: seed, id: undefined }]);
});

test("remove forgets a seed without dialing", (t) => {
  const { dialer, created } = makeDialer();

  t.after(() => dialer.close());

  dialer.add("tcp://10.0.0.9:9");
  dialer.remove("tcp://10.0.0.9:9");
  dialer.remove("tcp://10.0.0.9:10");

  assert.deepEqual(dialer.list(), []);
  assert.equal(created.length, 1);
});

test("discover dials every seed once", async (t) => {
  const seeds = ["tcp://10.0.0.9:9", "tcp://10.0.0.9:10"];
  const { dialer, created } = makeDialer(seeds);

  t.after(() => dialer.close());

  dialer.discover();
  dialer.discover();

  await tick();

  assert.deepEqual(
    created.map((entry) => entry.addr),
    seeds,
  );
});

test("dial ignores a failed create and retries later", async () => {
  const created: string[] = [];
  let failed = false;

  // A failing create must clear the in-flight marker so the next dial retries.
  const dialer = new Dialer({
    seeds: [],
    create: (addr) => {
      created.push(addr);

      if (!failed) {
        failed = true;
        return Promise.reject(new Error("boom"));
      }

      return Promise.resolve(makeClient() as unknown as Client);
    },
    track: () => {},
    untrack: () => {},
    isClosed: () => false,
  });

  await dialer.dial("tcp://10.0.0.9:9");

  assert.equal(dialer.has("tcp://10.0.0.9:9"), false);

  await dialer.dial("tcp://10.0.0.9:9");

  assert.equal(dialer.has("tcp://10.0.0.9:9"), true);
  assert.equal(created.length, 2);

  dialer.close();
});

test("drop closes and untracks the client", async (t) => {
  const { dialer, clients, tracked } = makeDialer();

  t.after(() => dialer.close());

  await dialer.dial("tcp://10.0.0.9:9");

  assert.equal(tracked.length, 1);

  dialer.drop("tcp://10.0.0.9:9");

  assert.equal(clients[0].closeCount, 1);
  assert.equal(tracked.length, 0);
  assert.equal(dialer.has("tcp://10.0.0.9:9"), false);
});

test("close drops every dial", async (t) => {
  const { dialer, clients, tracked } = makeDialer();

  t.after(() => dialer.close());

  await dialer.dial("tcp://10.0.0.9:9");
  await dialer.dial("tcp://10.0.0.9:10");

  dialer.close();

  assert.deepEqual(
    clients.map((client) => client.closeCount),
    [1, 1],
  );
  assert.equal(tracked.length, 0);
});

test("dial closes the client when the dialer is already closed", async (t) => {
  const { dialer, clients, tracked, close } = makeDialer();

  t.after(() => dialer.close());

  close();

  await dialer.dial("tcp://10.0.0.9:9");

  assert.equal(clients.length, 1);
  assert.equal(clients[0].closeCount, 1);
  assert.equal(tracked.length, 0);
});

test("add and remove accept url seeds", (t) => {
  const { dialer, created } = makeDialer();
  const seed = new URL("tcp://10.0.0.9:9");

  t.after(() => dialer.close());

  dialer.add(seed);

  assert.deepEqual(dialer.list(), ["tcp://10.0.0.9:9"]);
  assert.deepEqual(created, [{ addr: "tcp://10.0.0.9:9", id: undefined }]);

  dialer.remove(seed);

  assert.deepEqual(dialer.list(), []);
});
