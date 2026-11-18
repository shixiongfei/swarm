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
import { createApp } from "./app.ts";
import { createService } from "./service.ts";
import { freePort, waitFor } from "./testutil.ts";
import type { SwarmError } from "./errors.ts";

describe("App", () => {
  test("start listens and makes services callable", async (t) => {
    const port = await freePort();
    const math = createService({
      name: "math",
      setup: () => ({
        add: (a: number, b: number) => a + b,
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

  test("stop detaches services and closes the node", async (t) => {
    const math = createService({
      name: "math",
      setup: () => ({
        add: (a: number, b: number) => a + b,
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
        add: (a: number, b: number) => a + b,
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
});
