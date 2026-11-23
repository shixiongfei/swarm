/*
 * app.test.ts
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
import { createServer } from "node:net";
import type { AddressInfo } from "node:net";
import { createApp } from "./app.ts";
import { createService } from "./service.ts";
import { Node } from "./node.ts";
import { freePort, waitFor } from "./testutil.ts";
import type { SwarmError } from "./errors.ts";

describe("App", () => {
  test("start listens and makes services callable", async (t) => {
    const port = await freePort();
    const math = createService({
      name: "math",
      setup: () => ({
        add: (_ctx, a: number, b: number) => a + b,
      }),
    });

    const app = createApp({
      node: { cluster: "app" },
      listen: [`tcp://127.0.0.1:${port}`],
      services: [math],
    });

    t.after(() => app.stop());

    assert.equal(await app.start(), app);

    await waitFor(() => math.state === "ready");

    assert.equal(await app.node.call("math", "add", [1, 2]), 3);
  });

  test("start is idempotent", async (t) => {
    const app = createApp({ node: { cluster: "app" } });

    t.after(() => app.stop());

    assert.equal(await app.start(), app);
    assert.equal(await app.start(), app);
  });

  test("concurrent start waits for the same boot", async (t) => {
    const port = await freePort();
    const app = createApp({
      node: { cluster: "app" },
      listen: [`tcp://127.0.0.1:${port}`],
    });
    const peer = new Node({ cluster: "app" });

    t.after(() => {
      app.stop();
      peer.close();
    });

    const first = app.start();
    const second = app.start();

    assert.equal(first, second);
    assert.equal(await first, app);
    assert.equal(await second, app);

    assert.equal(await peer.connect(`tcp://127.0.0.1:${port}`), peer);
  });

  test("stop detaches services and closes the node", async (t) => {
    const math = createService({
      name: "math",
      setup: () => ({
        add: (_ctx, a: number, b: number) => a + b,
      }),
    });

    const app = createApp({
      node: { cluster: "app" },
      services: [math],
    });

    t.after(() => app.stop());

    await app.start();
    await waitFor(() => math.state === "ready");

    await app.stop();

    assert.equal(math.state, "idle");
    assert.equal(app.node.snapshot().services.length, 0);
  });

  test("start after stop is rejected", async (t) => {
    const app = createApp({ node: { cluster: "app" } });

    t.after(() => app.stop());

    await app.start();
    await app.stop();

    await assert.rejects(app.start(), (err: SwarmError) => {
      return err.code === "APP_CLOSED";
    });
  });

  test("closing the node directly stops the app", async (t) => {
    const math = createService({
      name: "math",
      setup: () => ({
        add: (_ctx, a: number, b: number) => a + b,
      }),
    });

    const app = createApp({ node: { cluster: "app" }, services: [math] });

    t.after(() => app.stop());

    await app.start();
    await waitFor(() => math.state === "ready");

    app.node.close();

    assert.equal(math.state, "idle");

    await assert.rejects(app.start(), (err: SwarmError) => {
      return err.code === "APP_CLOSED";
    });
  });

  test("a failed start closes the node and stays stopped", async (t) => {
    const blocker = createServer();

    await new Promise<void>((resolve) =>
      blocker.listen(0, "127.0.0.1", resolve),
    );

    const taken = (blocker.address() as AddressInfo).port;

    const open = await freePort();
    const math = createService({
      name: "math",
      setup: () => ({
        add: (_ctx, a: number, b: number) => a + b,
      }),
    });

    const app = createApp({
      node: { cluster: "app" },
      listen: [`tcp://127.0.0.1:${open}`, `tcp://127.0.0.1:${taken}`],
      services: [math],
    });

    t.after(() => {
      blocker.close();
      return app.stop();
    });

    await assert.rejects(() => app.start());

    assert.equal(math.state, "idle");
    assert.equal(app.node.isClosed, true);

    await assert.rejects(app.start(), (err: SwarmError) => {
      return err.code === "APP_CLOSED";
    });

    await waitFor(async () => {
      const probe = createServer();

      try {
        await new Promise<void>((resolve, reject) => {
          probe.once("error", reject);
          probe.listen(open, "127.0.0.1", resolve);
        });

        return true;
      } catch {
        return false;
      } finally {
        await new Promise<void>((resolve) => probe.close(() => resolve()));
      }
    });
  });

  test("stop during start rejects the start and leaves services idle", async (t) => {
    const math = createService({
      name: "math",
      setup: () => ({
        add: (_ctx, a: number, b: number) => a + b,
      }),
    });

    const app = createApp({
      node: { cluster: "app" },
      listen: ["tcp://127.0.0.1:0"],
      services: [math],
    });

    t.after(() => app.stop());

    const starting = app.start();

    app.stop();

    await assert.rejects(starting, (err: SwarmError) => {
      return err.kind === "rejected" && err.message === "node is closed";
    });

    assert.equal(math.state, "idle");
    assert.equal(app.node.isClosed, true);
  });
});
