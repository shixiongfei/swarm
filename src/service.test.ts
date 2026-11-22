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
import { spawnSync } from "node:child_process";
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

  test("runs setup again after detaching and reattaching", async (t) => {
    const node = new Node({ cluster: "service" });
    let calls = 0;
    const db = createService({
      name: "db",
      setup: () => {
        calls += 1;

        return { get: () => 1 };
      },
    });

    t.after(() => node.close());

    db.attach(node);

    await waitFor(() => db.state === "ready");

    db.detach();
    db.attach(node);

    await waitFor(() => db.state === "ready");

    assert.equal(calls, 2);
  });

  test("does not cache setup that finishes after detach", async (t) => {
    const source = new Node({ cluster: "service" });
    const target = new Node({ cluster: "service" });
    let calls = 0;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const db = createService({
      name: "db",
      setup: async ({ node }) => {
        calls += 1;

        if (calls === 1) {
          await gate;
        }

        return { self: () => node.nodeId };
      },
    });

    t.after(() => {
      source.close();
      target.close();
    });

    db.attach(source);

    await waitFor(() => calls === 1);

    db.detach();
    db.attach(target);
    release();

    await waitFor(() => db.state === "ready");

    assert.equal(calls, 2);
    assert.equal(registered(target, "db"), true);
    assert.equal(registered(source, "db"), false);
  });

  test("reruns setup when the same node reattaches during setup", async (t) => {
    const node = new Node({ cluster: "service" });
    let calls = 0;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const db = createService({
      name: "db",
      setup: async () => {
        calls += 1;

        const generation = calls;

        if (generation === 1) {
          await gate;
        }

        return { generation: () => generation };
      },
    });

    t.after(() => node.close());

    db.attach(node);

    await waitFor(() => calls === 1);

    db.detach();
    db.attach(node);
    release();

    await waitFor(() => db.state === "ready");

    assert.equal(calls, 2);
    assert.equal(await node.call("db", "generation"), 2);
  });

  test("a stale setup failure does not tear down the next attach", async (t) => {
    const source = new Node({ cluster: "service" });
    const target = new Node({ cluster: "service" });
    const errors: Error[] = [];
    let calls = 0;
    let reject!: (error: Error) => void;
    let entered!: () => void;
    const gate = new Promise<never>((_, fail) => (reject = fail));
    const started = new Promise<void>((resolve) => (entered = resolve));
    const db = createService({
      name: "db",
      setup: async ({ node }) => {
        calls += 1;

        if (calls === 1) {
          entered();
          await gate;
        }

        return { self: () => node.nodeId };
      },
    });

    t.after(() => {
      source.close();
      target.close();
    });

    source.events.on("error", (error) => errors.push(error));
    target.events.on("error", (error) => errors.push(error));

    db.attach(source);

    await started;

    db.detach();
    db.attach(target);
    reject(new Error("stale"));

    await waitFor(() => db.state === "ready");

    assert.equal(calls, 2);
    assert.deepEqual(errors, []);
    assert.equal(registered(target, "db"), true);
    assert.equal(await target.call("db", "self"), target.nodeId);
  });

  test("a setup failure crashes the process when nobody listens", () => {
    const script = `
import { Node } from "./node.ts";
import { createService } from "./service.ts";

const node = new Node({ cluster: "test" });

createService({
  name: "db",
  setup: () => {
    throw new Error("boom-setup");
  },
}).attach(node);

setTimeout(() => console.log("survived"), 500);
`;

    const result = spawnSync(
      process.execPath,
      ["--input-type=module", "-e", script],
      {
        cwd: new URL(".", import.meta.url),
        encoding: "utf8",
        timeout: 10000,
      },
    );

    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /boom-setup/);
    assert.doesNotMatch(result.stdout, /survived/);
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
          add: (_ctx, a: number, b: number) => a + b,
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
