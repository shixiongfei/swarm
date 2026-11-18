/*
 * service.test.ts
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
import { Node } from "./node.ts";
import { createService } from "./service.ts";
import { freePort, waitFor } from "./testutil.ts";
import type { SwarmError } from "./errors.ts";

function registered(node: Node, service: string) {
  return node.registrations().some((instance) => instance.service === service);
}

describe("Service", () => {
  test("registers when it has no dependency", async (t) => {
    const node = new Node({ cluster: "service" });
    const db = createService({
      name: "db",
      setup: ({ node }) => ({
        self: () => node.nodeId,
      }),
    });

    t.after(() => node.close());

    db.attach(node);

    await waitFor(() => db.state === "ready");
    assert.equal(registered(node, "db"), true);
  });

  test("waits for its dependency before registering", async (t) => {
    const node = new Node({ cluster: "service" });
    const api = createService({
      name: "api",
      requires: ["db"],
      setup: () => ({ ping: () => "pong" }),
    });

    t.after(() => node.close());

    api.attach(node);

    assert.equal(api.state, "waiting");
    assert.equal(registered(node, "api"), false);

    const db = createService({ name: "db", setup: () => ({ get: () => 1 }) });

    db.attach(node);

    await waitFor(() => api.state === "ready");
    assert.equal(registered(node, "api"), true);
  });

  test("unregisters when its dependency disappears and returns", async (t) => {
    const node = new Node({ cluster: "service" });
    const db = createService({ name: "db", setup: () => ({ get: () => 1 }) });
    const api = createService({
      name: "api",
      requires: ["db"],
      setup: () => ({ ping: () => "pong" }),
    });

    t.after(() => node.close());

    db.attach(node);
    api.attach(node);

    await waitFor(() => api.state === "ready");

    db.detach();

    await waitFor(() => api.state === "waiting");
    assert.equal(registered(node, "api"), false);

    db.attach(node);

    await waitFor(() => api.state === "ready");
    assert.equal(registered(node, "api"), true);
  });

  test("runs setup once across dependency flaps", async (t) => {
    const node = new Node({ cluster: "service" });
    let calls = 0;

    const db = createService({ name: "db", setup: () => ({ get: () => 1 }) });
    const api = createService({
      name: "api",
      requires: ["db"],
      setup: () => {
        calls++;

        return { ping: () => "pong" };
      },
    });

    t.after(() => node.close());

    db.attach(node);
    api.attach(node);

    await waitFor(() => api.state === "ready");

    db.detach();
    await waitFor(() => api.state === "waiting");

    db.attach(node);
    await waitFor(() => api.state === "ready");

    assert.equal(calls, 1);
  });

  test("follows a dependency provided by another node", async (t) => {
    const port = await freePort();
    const hub = new Node({ cluster: "service-remote" });
    const peer = new Node({ cluster: "service-remote" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    await hub.listen(`tcp://127.0.0.1:${port}`);
    await peer.connect(`tcp://127.0.0.1:${port}`);

    const db = createService({ name: "db", setup: () => ({ get: () => 1 }) });
    const api = createService({
      name: "api",
      requires: ["db"],
      setup: () => ({ ping: () => "pong" }),
    });

    db.attach(hub);
    api.attach(peer);

    await waitFor(() => api.state === "ready");

    db.detach();

    await waitFor(() => api.state === "waiting");
  });

  test("detach unregisters and resets the state", async (t) => {
    const node = new Node({ cluster: "service" });
    const db = createService({ name: "db", setup: () => ({ get: () => 1 }) });

    t.after(() => node.close());

    db.attach(node);

    await waitFor(() => db.state === "ready");

    db.detach();

    assert.equal(db.state, "idle");
    assert.equal(registered(node, "db"), false);
  });

  test("reattaching does not reuse providers from the previous node", async (t) => {
    const source = new Node({ cluster: "service" });
    const target = new Node({ cluster: "service" });
    const db = createService({ name: "db", setup: () => ({ get: () => 1 }) });
    const api = createService({
      name: "api",
      requires: ["db"],
      setup: () => ({ ping: () => "pong" }),
    });

    t.after(() => {
      source.close();
      target.close();
    });

    db.attach(source);
    api.attach(source);

    await waitFor(() => api.state === "ready");

    api.detach();
    db.detach();
    api.attach(target);

    await Promise.resolve();

    assert.equal(api.state, "waiting");
    assert.equal(registered(target, "api"), false);
  });

  test("closing during setup leaves the service idle", async (t) => {
    const node = new Node({ cluster: "service" });
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const started = new Promise<void>((resolve) => (entered = resolve));
    const service = createService({
      name: "math",
      setup: async () => {
        entered();
        await gate;

        return {
          add: (a: number, b: number) => a + b,
        };
      },
    });

    t.after(() => node.close());

    service.attach(node);

    await started;

    node.close();
    release();

    await Promise.resolve();

    assert.equal(service.state, "idle");
  });

  test("attach twice is rejected", (t) => {
    const node = new Node({ cluster: "service" });
    const db = createService({ name: "db", setup: () => ({ get: () => 1 }) });

    t.after(() => node.close());

    db.attach(node);

    assert.throws(
      () => db.attach(node),
      (err: SwarmError) => err.code === "SERVICE_ATTACHED",
    );
  });

  test("reports a failing setup through the node error event", async (t) => {
    const node = new Node({ cluster: "service" });
    const broken = createService({
      name: "broken",
      setup: () => {
        throw new Error("boom");
      },
    });

    t.after(() => node.close());

    const raised = new Promise<Error>((resolve) =>
      node.events.once("error", resolve),
    );

    broken.attach(node);

    const err = (await raised) as SwarmError;

    assert.equal(err.code, "SETUP_FAILED");
    assert.equal(err.message, "boom");
    assert.equal(broken.state, "idle");
    assert.equal(registered(node, "broken"), false);
  });

  test("closing the node resets the service", async (t) => {
    const node = new Node({ cluster: "service" });
    const db = createService({ name: "db", setup: () => ({ get: () => 1 }) });

    db.attach(node);

    await waitFor(() => db.state === "ready");

    node.close();

    assert.equal(db.state, "idle");
  });
});
