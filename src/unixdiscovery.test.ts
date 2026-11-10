/*
 * unixdiscovery.test.ts
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
import { mkdtemp, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { UnixDiscovery } from "./unixdiscovery.ts";
import type { Socket } from "node:net";

async function tempDir() {
  return mkdtemp(join(tmpdir(), "swarm-unix-"));
}

async function exists(path: string) {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

async function isEmpty(dir: string) {
  try {
    return (await readdir(dir)).length === 0;
  } catch {
    return true;
  }
}

async function waitFor(check: () => Promise<boolean>, timeout = 2000) {
  const deadline = Date.now() + timeout;

  while (Date.now() < deadline) {
    if (await check()) {
      return;
    }

    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }

  throw new Error("timed out");
}

test("links a pair of nodes exactly once", async (t) => {
  const dir = await tempDir();
  const a = new UnixDiscovery({
    nodeId: "0".repeat(16),
    cluster: "pair",
    dir,
    interval: 20,
  });
  const b = new UnixDiscovery({
    nodeId: "1".repeat(16),
    cluster: "pair",
    dir,
    interval: 20,
  });
  const sockets: Socket[] = [];

  let accepts = 0;
  let connects = 0;

  t.after(() => {
    a.close();
    b.close();

    for (const socket of sockets) {
      socket.destroy();
    }

    return rm(dir, { recursive: true, force: true });
  });

  for (const discovery of [a, b]) {
    discovery.events.on("accept", (socket) => {
      accepts += 1;
      sockets.push(socket);
    });
    discovery.events.on("connect", (socket) => {
      connects += 1;
      sockets.push(socket);
    });
  }

  await a.open();
  await b.open();

  await waitFor(async () => accepts === 1 && connects === 1);
  await new Promise<void>((resolve) => setTimeout(resolve, 100));

  assert.equal(accepts, 1);
  assert.equal(connects, 1);
});

test("removes its socket file on close but keeps a given directory", async (t) => {
  const root = await tempDir();
  const dir = join(root, "cluster");
  const discovery = new UnixDiscovery({
    nodeId: "0".repeat(16),
    cluster: "close",
    dir,
    interval: 1000,
  });

  t.after(() => rm(root, { recursive: true, force: true }));

  await discovery.open();

  assert.equal(discovery.active, true);
  assert.equal((await stat(dir)).mode & 0o777, 0o700);

  const names = await readdir(dir);

  assert.equal(names.length, 1);
  assert.equal((await stat(join(dir, names[0]))).mode & 0o777, 0o600);

  discovery.close();

  assert.equal(discovery.active, false);
  await waitFor(() => isEmpty(dir));
  assert.equal(await exists(dir), true);
});

test("removes a stale socket left by a dead peer", async (t) => {
  const dir = await tempDir();
  const stale = join(dir, `${"f".repeat(16)}.sock`);
  const discovery = new UnixDiscovery({
    nodeId: "0".repeat(16),
    cluster: "stale",
    dir,
    interval: 20,
  });

  t.after(() => {
    discovery.close();
    return rm(dir, { recursive: true, force: true });
  });

  await writeFile(stale, "");

  assert.equal(await exists(stale), true);

  await discovery.open();

  await waitFor(async () => !(await exists(stale)));
});

test("derives a private directory from the cluster", async (t) => {
  const discovery = new UnixDiscovery({
    nodeId: "0".repeat(16),
    cluster: `dir-${Date.now()}`,
    interval: 1000,
  });

  t.after(() => discovery.close());

  await discovery.open();

  assert.match(discovery.dir, /swarm[\\/][0-9a-f]{12}$/);
  assert.equal((await stat(discovery.dir)).mode & 0o777, 0o700);

  const dir = discovery.dir;

  discovery.close();

  await waitFor(async () => !(await exists(dir)));
});
