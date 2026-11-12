/*
 * discovery.test.ts
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
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Discovery } from "./discovery.ts";
import type { DiscoveryOptions } from "./discovery.ts";

function makeDiscovery(discovery?: boolean | DiscoveryOptions) {
  const errors: Error[] = [];
  const ready: string[] = [];

  const instance = new Discovery({
    nodeId: "self",
    cluster: `discovery-${randomUUID()}`,
    discovery,
    learn: () => {},
    self: () => [],
    join: () => {},
    attach: () => {},
    onError: (error) => errors.push(error),
    onReady: (directory) => ready.push(directory),
  });

  return { discovery: instance, errors, ready };
}

async function tempDir() {
  return mkdtemp(join(tmpdir(), "swarm-discovery-"));
}

describe("Discovery", () => {
  test("starts nothing without configuration", async () => {
    const { discovery, errors, ready } = makeDiscovery();

    await discovery.startUnix();

    assert.deepEqual(errors, []);
    assert.deepEqual(ready, []);
    assert.equal(discovery.unixActive, false);
    assert.equal(discovery.multicastActive, false);

    discovery.close();
  });

  test("reports the directory once the unix socket is listening", async (t) => {
    const root = await tempDir();
    const { discovery, errors, ready } = makeDiscovery({ dir: root });

    t.after(async () => {
      discovery.close();
      await rm(root, { recursive: true, force: true });
    });

    await discovery.startUnix();

    assert.deepEqual(errors, []);
    assert.equal(ready.length, 1);
    assert.equal(typeof ready[0], "string");
    assert.equal(discovery.unixActive, true);
  });

  test("starts only once", async (t) => {
    const root = await tempDir();
    const { discovery, ready } = makeDiscovery({ dir: root });

    t.after(async () => {
      discovery.close();
      await rm(root, { recursive: true, force: true });
    });

    await discovery.startUnix();
    await discovery.startUnix();

    assert.equal(ready.length, 1);
  });

  test("stops listening after close", async (t) => {
    const root = await tempDir();
    const { discovery } = makeDiscovery({ dir: root });

    t.after(() => rm(root, { recursive: true, force: true }));

    await discovery.startUnix();
    assert.equal(discovery.unixActive, true);

    discovery.close();
    assert.equal(discovery.unixActive, false);
  });

  test("reports an error when the directory is unusable", async (t) => {
    const root = await tempDir();
    const dir = join(root, "not-a-directory");

    await writeFile(dir, "");

    const { discovery, errors, ready } = makeDiscovery({ dir });

    t.after(async () => {
      discovery.close();
      await rm(root, { recursive: true, force: true });
    });

    await discovery.startUnix();

    assert.equal(errors.length, 1);
    assert.equal(errors[0] instanceof Error, true);
    assert.deepEqual(ready, []);
    assert.equal(discovery.unixActive, false);
  });

  test("does not start multicast after close", async () => {
    const errors: Error[] = [];
    const discovery = new Discovery({
      nodeId: "self",
      cluster: `discovery-${randomUUID()}`,
      multicast: { port: 0 },
      learn: () => {},
      self: () => [],
      join: () => {},
      attach: () => {},
      onError: (error) => errors.push(error),
      onReady: () => {},
    });

    discovery.close();

    await discovery.startMulticast();

    assert.deepEqual(errors, []);
    assert.equal(discovery.multicastActive, false);
  });

  test("does not start after close", async (t) => {
    const root = await tempDir();
    const { discovery, ready } = makeDiscovery({ dir: root });

    t.after(() => rm(root, { recursive: true, force: true }));

    discovery.close();

    await discovery.startUnix();

    assert.deepEqual(ready, []);
    assert.equal(discovery.unixActive, false);
  });
});
