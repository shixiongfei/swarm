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
import {
  mkdtemp,
  readdir,
  rename,
  rm,
  stat,
  utimes,
  writeFile,
} from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { UnixDiscovery } from "./unixdiscovery.ts";
import { waitFor } from "./testutil.ts";
import type { Socket } from "node:net";

const isWindows = process.platform === "win32";

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

test("links a pair of nodes exactly once", async (t) => {
  const dir = await tempDir();
  const a = new UnixDiscovery({
    nodeId: "0".repeat(16),
    cluster: "pair",
    secret: "",
    dir,
    interval: 20,
  });
  const b = new UnixDiscovery({
    nodeId: "1".repeat(16),
    cluster: "pair",
    secret: "",
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
    secret: "",
    dir,
    interval: 1000,
  });

  t.after(() => rm(root, { recursive: true, force: true }));

  await discovery.open();

  assert.equal(discovery.active, true);

  if (!isWindows) {
    assert.equal((await stat(dir)).mode & 0o777, 0o700);
  }

  const names = await readdir(dir);

  assert.equal(names.length, 1);

  if (!isWindows) {
    assert.equal((await stat(join(dir, names[0]))).mode & 0o777, 0o600);
  }

  discovery.close();

  assert.equal(discovery.active, false);
  await waitFor(() => isEmpty(dir));
  assert.equal(await exists(dir), true);
});

test("keeps a fresh endpoint that has no listener yet", async (t) => {
  const dir = await tempDir();
  const stale = join(dir, `${"f".repeat(16)}.sock`);
  const discovery = new UnixDiscovery({
    nodeId: "0".repeat(16),
    cluster: "stale",
    secret: "",
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

  await new Promise<void>((resolve) => setTimeout(resolve, 100));

  assert.equal(await exists(stale), true);
});

test("keeps an endpoint through a single failed connection", async (t) => {
  const dir = await tempDir();
  const peer = join(dir, `${"f".repeat(16)}.sock`);
  const discovery = new UnixDiscovery({
    nodeId: "0".repeat(16),
    cluster: "single-failure",
    secret: "",
    dir,
    interval: 60000,
  });

  t.after(() => {
    discovery.close();
    return rm(dir, { recursive: true, force: true });
  });

  await writeFile(peer, "");

  const past = new Date(Date.now() - 60000);

  await utimes(peer, past, past);
  await discovery.open();
  await delay(200);

  assert.equal(await exists(peer), true);
});

test("reaps an endpoint left by a dead peer", async (t) => {
  const dir = await tempDir();
  const stale = join(dir, `${"f".repeat(16)}.sock`);
  const discovery = new UnixDiscovery({
    nodeId: "0".repeat(16),
    cluster: "reap",
    secret: "",
    dir,
    interval: 20,
  });

  t.after(() => {
    discovery.close();
    return rm(dir, { recursive: true, force: true });
  });

  await writeFile(stale, "");

  const past = new Date(Date.now() - 60000);

  await utimes(stale, past, past);
  await discovery.open();
  await waitFor(async () => !(await exists(stale)));

  assert.equal(await exists(stale), false);
});

test(
  "reclaims its own socket file left by a crashed instance",
  { skip: isWindows },
  async (t) => {
    const dir = await tempDir();
    const marker = join(dir, `${"0".repeat(16)}.sock`);
    const parked = join(dir, "parked");
    const server = createServer();

    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(marker, () => {
        server.off("error", reject);
        resolve();
      });
    });

    await rename(marker, parked);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rename(parked, marker);

    assert.equal(await exists(marker), true);

    const discovery = new UnixDiscovery({
      nodeId: "0".repeat(16),
      cluster: "reclaim",
      secret: "",
      dir,
      interval: 1000,
    });

    t.after(() => {
      discovery.close();
      return rm(dir, { recursive: true, force: true });
    });

    await discovery.open();

    assert.equal(discovery.active, true);
  },
);

test("derives a private directory from the cluster", async (t) => {
  const discovery = new UnixDiscovery({
    nodeId: "0".repeat(16),
    cluster: `dir-${Date.now()}`,
    secret: "",
    interval: 1000,
  });

  t.after(() => discovery.close());

  await discovery.open();

  assert.match(discovery.dir, /swarm[\\/][0-9a-f]{12}$/);

  if (!isWindows) {
    assert.equal((await stat(discovery.dir)).mode & 0o777, 0o700);
  }

  const dir = discovery.dir;

  discovery.close();

  await waitFor(async () => !(await exists(dir)));
});

test(
  "publishes a marker file next to the named pipe on windows",
  { skip: !isWindows },
  async (t) => {
    const dir = await tempDir();
    const discovery = new UnixDiscovery({
      nodeId: "0".repeat(16),
      cluster: "pipe",
      secret: "",
      dir,
      interval: 1000,
    });

    t.after(() => {
      discovery.close();
      return rm(dir, { recursive: true, force: true });
    });

    await discovery.open();

    assert.equal(discovery.active, true);

    const names = await readdir(dir);

    assert.equal(names.length, 1);
    assert.equal((await stat(join(dir, names[0]))).isFile(), true);
  },
);

test("close during open leaves no scan timer", async (t) => {
  const dir = await tempDir();
  const discovery = new UnixDiscovery({
    nodeId: "f".repeat(16),
    cluster: "race",
    secret: "",
    dir,
    interval: 20,
  });
  const probe = discovery as unknown as {
    scan: () => Promise<void>;
    timer?: ReturnType<typeof setInterval>;
  };

  t.after(async () => {
    if (probe.timer !== undefined) {
      clearInterval(probe.timer);
    }

    discovery.close();
    await rm(dir, { recursive: true, force: true });
  });

  let release!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => (release = resolve));
  const started = new Promise<void>((resolve) => (entered = resolve));

  probe.scan = async () => {
    entered();
    await gate;
  };

  const opening = discovery.open();

  await started;
  discovery.close();
  release();
  await opening;

  assert.equal(probe.timer, undefined);
});
