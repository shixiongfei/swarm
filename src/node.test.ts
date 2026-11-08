/*
 * node.test.ts
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
import { createSocket } from "node:dgram";
import { createServer } from "node:net";
import { createServer as createHttpServer } from "node:http";
import { Node } from "./node.ts";
import type { AddressInfo } from "node:net";
import type { SwarmError } from "./errors.ts";
import type { MetaData, WatchEvent } from "./registry.ts";

function makeNode(callTimeout = 50) {
  return new Node({ cluster: "test", callTimeout });
}

async function freePort() {
  const server = createServer();

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));

  const port = (server.address() as AddressInfo).port;

  await new Promise<void>((resolve) => server.close(() => resolve()));

  return port;
}

async function freeUdpPort() {
  const socket = createSocket({ type: "udp4", reuseAddr: true });

  await new Promise<void>((resolve) => socket.bind(0, "0.0.0.0", resolve));

  const port = (socket.address() as AddressInfo).port;

  await new Promise<void>((resolve) => socket.close(resolve));

  return port;
}

async function within<T>(promise: Promise<T>, ms: number) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<undefined>((resolve) => {
    timer = setTimeout(() => resolve(undefined), ms);
  });
  const value = await Promise.race([promise, timeout]);

  if (timer !== undefined) {
    clearTimeout(timer);
  }

  return value;
}

async function connectPair(protocol: "tcp" | "ws", hub: Node, peer: Node) {
  const port = await freePort();
  const url = `${protocol}://127.0.0.1:${port}`;

  await hub.listen(url);
  assert.equal(await peer.connect(url), peer);

  return port;
}

async function attachGhost(node: Node, service: string) {
  const { port1, port2 } = new MessageChannel();
  const ready = new Promise<WatchEvent>((resolve) =>
    node.watch(service, resolve),
  );

  node.attach(port1);
  port2.start();
  port2.postMessage({ t: "hello", from: "ghost" });
  port2.postMessage({ t: "up", from: "ghost", service });

  await ready;

  return {
    detach() {
      port2.close();
    },
    close() {
      node.close();
      port1.close();
      port2.close();
    },
  };
}

describe("Node", () => {
  test("register and unregister return the node", () => {
    const node = makeNode();

    assert.equal(node.register("math", { add: () => 0 }), node);
    assert.equal(node.unregister("math"), node);
  });

  test("call passes an empty argument list by default", async () => {
    const node = makeNode();
    let received: unknown[] | undefined;

    node.register("echo", {
      count: (...args: unknown[]) => {
        received = args;
        return args.length;
      },
    });

    assert.equal(await node.call("echo", "count"), 0);
    assert.deepEqual(received, []);
  });

  test("call uses the node call timeout", async (t) => {
    const node = makeNode(20);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    await assert.rejects(
      node.call("math", "add"),
      (err: SwarmError) => err.kind === "timeout",
    );
  });

  test("call accepts a per-call timeout", async (t) => {
    const node = makeNode(5000);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    await assert.rejects(
      node.call("math", "add", [], { timeout: 20 }),
      (err: SwarmError) => err.kind === "timeout",
    );
  });

  test("call uses the node call timeout for a local service", async () => {
    const node = makeNode(20);

    node.register("slow", {
      wait: () => new Promise<void>((resolve) => setTimeout(resolve, 40)),
    });

    await assert.rejects(
      node.call("slow", "wait"),
      (err: SwarmError) => err.kind === "timeout",
    );
  });

  test("register rejects metadata that cannot be cloned", async () => {
    const node = makeNode();

    assert.throws(
      () => node.register("math", { add: () => 0 }, { build: () => {} }),
      (err: SwarmError) =>
        err.kind === "rejected" && err.message === "meta is not cloneable",
    );

    await assert.rejects(
      node.call("math", "add"),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("register rejects metadata that is not a plain object", async () => {
    const node = makeNode();

    assert.throws(
      () =>
        node.register(
          "math",
          { add: () => 0 },
          new Date() as unknown as MetaData,
        ),
      (err: SwarmError) =>
        err.kind === "rejected" && err.message === "meta is not a plain object",
    );

    await assert.rejects(
      node.call("math", "add"),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("register stores a snapshot of meta", () => {
    const node = makeNode();
    const meta = { build: 1 };

    node.register("math", { add: () => 0 }, meta);
    meta.build = 2;

    let stored: unknown;

    node.watch("math", (event) => {
      stored = event.instance.meta;
    });

    assert.deepEqual(stored, { build: 1 });
  });

  test("call rejects a service no link serves", async () => {
    const node = makeNode();

    await assert.rejects(
      node.call("nope", "add"),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("routes calls to a local service before a link", async (t) => {
    const node = makeNode(20);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    node.register("math", { add: () => 10 });

    assert.equal(await node.call("math", "add"), 10);
  });

  test("get returns a proxy that calls the service", async () => {
    type MathApi = { add(a: number, b: number): number };

    const node = makeNode();
    node.register("math", { add: (a: number, b: number) => a + b });
    const math = node.get<MathApi>("math");

    assert.equal(await math.add(2, 3), 5);
    assert.equal(await math, math);
    assert.equal(Reflect.get(math, Symbol.toStringTag), undefined);
  });

  test("close is idempotent", () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });

    node.close();

    assert.doesNotThrow(() => node.close());
  });

  test("close emits down for local services", () => {
    const node = makeNode();
    const events: WatchEvent[] = [];

    node.watch("math", (event) => events.push(event));
    node.register("math", { add: () => 0 });

    node.close();

    assert.deepEqual(
      events.map((event) => event.type),
      ["up", "down"],
    );
  });

  test("close rejects pending calls", async (t) => {
    const node = makeNode(5000);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    const call = node.call("math", "add");

    node.close();

    await assert.rejects(
      call,
      (err: SwarmError) =>
        err.kind === "failed" && err.message === "node is closed",
    );
  });

  test("operations fail after close", (t) => {
    const node = makeNode();
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      port1.close();
      port2.close();
    });

    node.register("math", { add: () => 0 });
    node.close();

    assert.throws(
      () => node.register("math", {}),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.unregister("math"),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.watch("math", () => {}),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.subscribe("news", () => {}),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.publish("news", 1),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.get("math"),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.attach(port1),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.throws(
      () => node.call("math", "add"),
      (err: SwarmError) => err.kind === "rejected",
    );
  });

  test("passes the publisher nodeId to subscribers", async (t) => {
    const a = new Node({ cluster: "from-a" });
    const b = new Node({ cluster: "from-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const received = new Promise((resolve) =>
      b.subscribe("news", (data, from) => resolve({ data, from })),
    );

    a.attach(port1);
    b.attach(port2);
    a.publish("news", { ok: true });

    assert.deepEqual(await received, { data: { ok: true }, from: a.nodeId });
  });

  test("publishes nothing when the payload cannot be cloned", async (t) => {
    const a = new Node({ cluster: "publish-a" });
    const b = new Node({ cluster: "publish-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.register("math", { add: () => 1 });

    let local = 0;
    let remote = 0;

    a.subscribe("news", () => {
      local += 1;
    });
    b.subscribe("news", () => {
      remote += 1;
    });

    const ready = new Promise<void>((resolve) =>
      b.watch("math", () => resolve()),
    );

    a.attach(port1);
    b.attach(port2);

    await ready;

    assert.throws(
      () => a.publish("news", () => {}),
      (err: SwarmError) =>
        err.kind === "failed" && err.message === "data is not cloneable",
    );

    await new Promise<void>((resolve) => setTimeout(resolve, 20));

    assert.equal(local, 0);
    assert.equal(remote, 0);
  });

  test("routes to another link after the first link closes", async (t) => {
    const a = new Node({ cluster: "failover-a" });
    const b = new Node({ cluster: "failover-b" });
    const c = new Node({ cluster: "failover-c" });
    const first = new MessageChannel();
    const second = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      c.close();
      first.port1.close();
      first.port2.close();
      second.port1.close();
      second.port2.close();
    });

    b.register("math", { add: () => 1 });
    c.register("math", { add: () => 2 });

    const ready = new Promise<void>((resolve) => {
      let count = 0;

      a.watch("math", () => {
        count += 1;

        if (count === 2) {
          resolve();
        }
      });
    });

    a.attach(first.port1);
    b.attach(first.port2);
    a.attach(second.port1);
    c.attach(second.port2);

    await ready;

    assert.equal(await a.call("math", "add"), 1);

    const closed = new Promise<void>((resolve) =>
      first.port1.addEventListener("close", () => resolve()),
    );

    b.close();

    await closed;

    assert.equal(await a.call("math", "add"), 2);
  });

  test("balances calls across links serving the same service", async (t) => {
    const a = new Node({ cluster: "balance-a" });
    const b = new Node({ cluster: "balance-b" });
    const c = new Node({ cluster: "balance-c" });
    const first = new MessageChannel();
    const second = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      c.close();
      first.port1.close();
      first.port2.close();
      second.port1.close();
      second.port2.close();
    });

    let bCalls = 0;
    let cCalls = 0;

    b.register("math", {
      add: () => {
        bCalls += 1;
        return 1;
      },
    });
    c.register("math", {
      add: () => {
        cCalls += 1;
        return 2;
      },
    });

    const ready = new Promise<void>((resolve) => {
      let count = 0;

      a.watch("math", () => {
        count += 1;

        if (count === 2) {
          resolve();
        }
      });
    });

    a.attach(first.port1);
    b.attach(first.port2);
    a.attach(second.port1);
    c.attach(second.port2);

    await ready;

    assert.deepEqual(
      [
        await a.call("math", "add"),
        await a.call("math", "add"),
        await a.call("math", "add"),
        await a.call("math", "add"),
      ],
      [1, 2, 1, 2],
    );
    assert.equal(bCalls, 2);
    assert.equal(cCalls, 2);
  });

  test("retries the next link when a call fails in transport", async (t) => {
    const a = new Node({ cluster: "retry-a" });
    const b = new Node({ cluster: "retry-b" });
    const c = new Node({ cluster: "retry-c" });
    const first = new MessageChannel();
    const second = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      c.close();
      first.port1.close();
      first.port2.close();
      second.port1.close();
      second.port2.close();
    });

    b.register("math", { add: () => new Promise(() => {}) });
    c.register("math", { add: () => 2 });

    const ready = new Promise<void>((resolve) => {
      let count = 0;

      a.watch("math", () => {
        count += 1;

        if (count === 2) {
          resolve();
        }
      });
    });

    a.attach(first.port1);
    b.attach(first.port2);
    a.attach(second.port1);
    c.attach(second.port2);

    await ready;

    const result = a.call("math", "add");

    b.close();

    assert.equal(await result, 2);
  });

  test("does not retry on a service error", async (t) => {
    const a = new Node({ cluster: "no-retry-a" });
    const b = new Node({ cluster: "no-retry-b" });
    const c = new Node({ cluster: "no-retry-c" });
    const first = new MessageChannel();
    const second = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      c.close();
      first.port1.close();
      first.port2.close();
      second.port1.close();
      second.port2.close();
    });

    let cCalls = 0;

    b.register("math", {
      add: () => {
        throw new Error("boom");
      },
    });
    c.register("math", {
      add: () => {
        cCalls += 1;
        return 2;
      },
    });

    const ready = new Promise<void>((resolve) => {
      let count = 0;

      a.watch("math", () => {
        count += 1;

        if (count === 2) {
          resolve();
        }
      });
    });

    a.attach(first.port1);
    b.attach(first.port2);
    a.attach(second.port1);
    c.attach(second.port2);

    await ready;

    await assert.rejects(
      a.call("math", "add"),
      (err: SwarmError) => err.kind === "failed" && err.message === "boom",
    );
    assert.equal(cCalls, 0);
  });

  test("keeps an instance while another link to the same node is open", async (t) => {
    const a = new Node({ cluster: "union-a" });
    const b = new Node({ cluster: "union-b" });
    const first = new MessageChannel();
    const second = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      first.port1.close();
      first.port2.close();
      second.port1.close();
      second.port2.close();
    });

    b.register("math", { add: () => 1 });

    const up = new Promise<WatchEvent>((resolve) => a.watch("math", resolve));

    a.attach(first.port1);
    b.attach(first.port2);

    await up;

    a.attach(second.port1);
    b.attach(second.port2);

    const closed = new Promise<void>((resolve) =>
      first.port1.addEventListener("close", () => resolve()),
    );

    first.port2.close();

    await closed;

    const echo = new Promise<WatchEvent>((resolve) => a.watch("echo", resolve));

    b.register("echo", { ping: () => "pong" });

    assert.equal((await echo).instance.nodeId, b.nodeId);
    assert.equal(await a.call("echo", "ping"), "pong");
    assert.equal(await a.call("math", "add"), 1);
  });

  test("watch unsubscribe stops delivery", async (t) => {
    const a = new Node({ cluster: "watch-unsubscribe-a" });
    const b = new Node({ cluster: "watch-unsubscribe-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.register("math", { add: () => 1 });

    const ready = new Promise<void>((resolve) =>
      b.watch("math", () => resolve()),
    );

    a.attach(port1);
    b.attach(port2);

    await ready;

    let observed = 0;

    const unsubscribe = b.watch("echo", () => {
      observed += 1;
    });

    unsubscribe();

    const up = new Promise<void>((resolve) => b.watch("echo", () => resolve()));

    a.register("echo", { ping: () => "pong" });

    await up;

    assert.equal(await b.call("echo", "ping"), "pong");
    assert.equal(observed, 0);
  });
  test("registers a service after attach", { timeout: 1000 }, async (t) => {
    const a = new Node({ cluster: "late-register-a" });
    const b = new Node({ cluster: "late-register-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const up = new Promise<WatchEvent>((resolve) => b.watch("late", resolve));

    a.attach(port1);
    b.attach(port2);

    a.register("late", { ping: () => 1 });

    assert.equal((await up).instance.service, "late");
    assert.equal(await b.call("late", "ping"), 1);
  });

  test("publishes after attach", { timeout: 1000 }, async (t) => {
    const a = new Node({ cluster: "late-publish-a" });
    const b = new Node({ cluster: "late-publish-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const received = new Promise<unknown>((resolve) =>
      b.subscribe("news", resolve),
    );

    a.attach(port1);
    b.attach(port2);

    a.publish("news", 1);

    assert.equal(await received, 1);
  });
});

for (const protocol of ["tcp", "ws"] as const) {
  describe(`Node ${protocol.toUpperCase()}`, () => {
    test("listen and connect return the node", async (t) => {
      const hub = new Node({ cluster: `${protocol}-return-hub` });
      const peer = new Node({ cluster: `${protocol}-return-peer` });

      t.after(() => {
        hub.close();
        peer.close();
      });

      await connectPair(protocol, hub, peer);
    });

    test(
      "calls a service registered on the connected peer",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({ cluster: `${protocol}-call-hub` });
        const peer = new Node({ cluster: `${protocol}-call-peer` });

        t.after(() => {
          hub.close();
          peer.close();
        });

        const up = new Promise<WatchEvent>((resolve) =>
          hub.watch("math", resolve),
        );

        await connectPair(protocol, hub, peer);

        peer.register("math", { add: (a: number, b: number) => a + b });

        assert.equal((await up).instance.nodeId, peer.nodeId);
        assert.equal(await hub.call("math", "add", [2, 3]), 5);
      },
    );

    test(
      "calls a service registered on the listening node",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({
          cluster: `${protocol}-host-hub`,
          callTimeout: 500,
        });
        const peer = new Node({
          cluster: `${protocol}-host-peer`,
          callTimeout: 500,
        });

        t.after(() => {
          hub.close();
          peer.close();
        });

        hub.register("core", { ping: () => "core" });

        const core = new Promise<WatchEvent>((resolve) =>
          peer.watch("core", resolve),
        );

        await connectPair(protocol, hub, peer);
        await core;

        assert.equal(await peer.call("core", "ping"), "core");

        const late = new Promise<WatchEvent>((resolve) =>
          peer.watch("late", resolve),
        );

        hub.register("late", { ping: () => "late" });
        await late;

        assert.equal(await peer.call("late", "ping"), "late");
      },
    );

    test(
      "discovers a service registered before listen when secret is set",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({
          cluster: `${protocol}-secret-hub`,
          secret: "s3cret",
        });
        const peer = new Node({
          cluster: `${protocol}-secret-peer`,
          secret: "s3cret",
        });

        t.after(() => {
          hub.close();
          peer.close();
        });

        hub.register("core", { ping: () => "core" });

        const up = new Promise<WatchEvent>((resolve) =>
          peer.watch("core", resolve),
        );

        const port = await freePort();
        const url = `${protocol}://127.0.0.1:${port}`;

        await hub.listen(url);
        await peer.connect(url);
        await up;

        assert.equal(await peer.call("core", "ping"), "core");
      },
    );

    test("publishes from the peer to the hub", { timeout: 3000 }, async (t) => {
      const hub = new Node({ cluster: `${protocol}-publish-hub` });
      const peer = new Node({ cluster: `${protocol}-publish-peer` });

      t.after(() => {
        hub.close();
        peer.close();
      });

      await connectPair(protocol, hub, peer);

      const ready = new Promise<WatchEvent>((resolve) =>
        hub.watch("math", resolve),
      );
      const received = new Promise<unknown>((resolve) =>
        hub.subscribe("news", resolve),
      );

      peer.register("math", { add: () => 0 });

      await ready;
      peer.publish("news", { id: 1 });

      assert.deepEqual(await received, { id: 1 });
    });

    test(
      "publishes nothing when the payload is not encodable",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({ cluster: `${protocol}-circular-hub` });
        const peer = new Node({ cluster: `${protocol}-circular-peer` });

        t.after(() => {
          hub.close();
          peer.close();
        });

        await connectPair(protocol, hub, peer);

        let local = 0;
        let remote = 0;

        peer.subscribe("news", () => {
          local += 1;
        });
        hub.subscribe("news", () => {
          remote += 1;
        });

        const circular: { self?: unknown } = {};
        circular.self = circular;

        assert.throws(
          () => peer.publish("news", circular),
          (err: SwarmError) =>
            err.kind === "failed" &&
            err.message === "envelope is not encodable",
        );

        assert.equal(local, 0);
        assert.equal(remote, 0);
      },
    );

    test(
      "publishes nothing when the payload exceeds the envelope limit",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({ cluster: `${protocol}-oversize-hub` });
        const peer = new Node({ cluster: `${protocol}-oversize-peer` });

        t.after(() => {
          hub.close();
          peer.close();
        });

        await connectPair(protocol, hub, peer);

        let local = 0;
        let remote = 0;

        peer.subscribe("news", () => {
          local += 1;
        });
        hub.subscribe("news", () => {
          remote += 1;
        });

        assert.throws(
          () => peer.publish("news", "x".repeat(9 * 1024 * 1024)),
          (err: SwarmError) =>
            err.kind === "failed" && err.message === "envelope is too large",
        );

        assert.equal(local, 0);
        assert.equal(remote, 0);
      },
    );

    test(
      "returns the bound address when listening on port 0",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({ cluster: `${protocol}-ephemeral` });
        const peer = new Node({ cluster: `${protocol}-ephemeral-peer` });

        t.after(() => {
          hub.close();
          peer.close();
        });

        hub.register("core", { ping: () => "core" });

        const address = await hub.listen(`${protocol}://127.0.0.1:0`);

        assert.equal(address.host, "127.0.0.1");
        assert.ok(address.port > 0);

        const up = new Promise<WatchEvent>((resolve) =>
          peer.watch("core", resolve),
        );

        await peer.connect(`${protocol}://127.0.0.1:${address.port}`);
        await up;

        assert.equal(await peer.call("core", "ping"), "core");
      },
    );

    test(
      "accepts a url object and a url secret",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({ cluster: `${protocol}-secret-hub` });
        const peer = new Node({ cluster: `${protocol}-secret-peer` });
        const port = await freePort();
        const url = `${protocol}://:s3cret@127.0.0.1:${port}`;

        t.after(() => {
          hub.close();
          peer.close();
        });

        const up = new Promise<WatchEvent>((resolve) =>
          hub.watch("math", resolve),
        );

        await hub.listen(new URL(url));
        await peer.connect(url);

        peer.register("math", { add: () => 7 });

        await up;
        assert.equal(await hub.call("math", "add"), 7);
      },
    );

    test("listen rejects a malformed url", async (t) => {
      const node = new Node({ cluster: `${protocol}-invalid-listen` });

      t.after(() => node.close());

      await assert.rejects(
        node.listen("http://127.0.0.1:1234"),
        (err: SwarmError) =>
          err.kind === "rejected" &&
          err.message === "invalid url: http://127.0.0.1:1234",
      );
    });

    test(
      "connect rejects when nothing listens",
      { timeout: 3000 },
      async (t) => {
        const node = new Node({ cluster: `${protocol}-refused` });
        const port = await freePort();

        t.after(() => node.close());

        await assert.rejects(
          node.connect(`${protocol}://127.0.0.1:${port}`),
          (err: SwarmError) => err.kind === "failed",
        );
      },
    );

    test(
      "close stops listening and rejects later calls",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({ cluster: `${protocol}-close-hub` });
        const peer = new Node({ cluster: `${protocol}-close-peer` });

        t.after(() => peer.close());

        const up = new Promise<WatchEvent>((resolve) =>
          hub.watch("math", resolve),
        );

        await connectPair(protocol, hub, peer);

        peer.register("math", { add: () => 1 });

        await up;
        assert.equal(await hub.call("math", "add"), 1);

        hub.close();

        assert.throws(
          () => hub.call("math", "add"),
          (err: SwarmError) => err.kind === "rejected",
        );
      },
    );

    test(
      "falls back to a remote method when the local one is missing",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({ cluster: `${protocol}-fallback-hub` });
        const peer = new Node({ cluster: `${protocol}-fallback-peer` });

        t.after(() => peer.close());
        t.after(() => hub.close());

        const up = new Promise<WatchEvent>((resolve) =>
          hub.watch("math", (event) => {
            if (event.type === "up" && event.instance.nodeId === peer.nodeId) {
              resolve(event);
            }
          }),
        );

        hub.register("math", { add: (a: number, b: number) => a + b });

        await connectPair(protocol, hub, peer);

        peer.register("math", { mul: (a: number, b: number) => a * b });

        await up;

        assert.equal(await hub.call("math", "add", [2, 3]), 5);
        assert.equal(await hub.call("math", "mul", [2, 3]), 6);
      },
    );
  });
}

test(
  "connect turns a wss url into a websocket client",
  { timeout: 3000 },
  async (t) => {
    const node = new Node({ cluster: "wss-client" });

    t.after(() => node.close());

    await assert.rejects(
      () => node.connect("wss://127.0.0.1:1", { reconnect: false }),
      (err: SwarmError) => err.kind === "failed",
    );
  },
);

test("listen rejects a wss url", async (t) => {
  const node = new Node({ cluster: "wss-listen" });

  t.after(() => node.close());

  await assert.rejects(
    () => node.listen("wss://127.0.0.1:0"),
    (err: SwarmError) =>
      err.kind === "rejected" && err.message === "wss listen is not supported",
  );
});

test("tcp listen rejects paths and attached http servers", async (t) => {
  const node = new Node({ cluster: "tcp-reject" });
  const http = createHttpServer();

  t.after(() => node.close());

  await assert.rejects(
    () => node.listen("tcp://127.0.0.1:1/swarm"),
    (err: SwarmError) =>
      err.kind === "rejected" &&
      err.message === "tcp url does not support a path",
  );

  await assert.rejects(
    () => node.listen("tcp://127.0.0.1:1", { ws: { server: http } }),
    (err: SwarmError) =>
      err.kind === "rejected" &&
      err.message === "tcp listen does not support http",
  );
});

test("ws and wss default to ports 80 and 443", async (t) => {
  const node = new Node({ cluster: "default-ports" });

  t.after(() => node.close());

  await assert.rejects(
    () => node.connect("ws://127.0.0.1", { reconnect: false }),
    (err: SwarmError) => err.kind === "failed",
  );
  await assert.rejects(
    () => node.connect("wss://127.0.0.1", { reconnect: false }),
    (err: SwarmError) => err.kind === "failed",
  );
  await assert.rejects(
    () => node.connect("tcp://127.0.0.1"),
    (err: SwarmError) =>
      err.kind === "rejected" && err.message === "invalid url: tcp://127.0.0.1",
  );
});

test("collect invokes local and remote instances and dedupes by peer", async (t) => {
  const local = new Node({ cluster: "collect-local" });
  const remote = new Node({ cluster: "collect-remote" });

  t.after(() => {
    local.close();
    remote.close();
  });

  local.register("math", {
    add: async (a: number, b: number) => a + b,
  });
  remote.register("math", {
    add: async (a: number, b: number) => a + b + 2,
  });

  const tcpPort = await freePort();
  const wsPort = await freePort();

  await remote.listen(`tcp://127.0.0.1:${tcpPort}`);
  await remote.listen(`ws://127.0.0.1:${wsPort}`);

  const up = new Promise<void>((resolve) =>
    local.watch("math", (event) => {
      if (event.type === "up" && event.instance.nodeId === remote.nodeId) {
        resolve();
      }
    }),
  );

  await local.connect(`tcp://127.0.0.1:${tcpPort}`);
  await local.connect(`ws://127.0.0.1:${wsPort}`);
  await up;

  const results = await local.collect("math", "add", [1, 2]);

  assert.equal(results.length, 2);
  assert.deepEqual(results[0], { nodeId: local.nodeId, ok: true, value: 3 });
  assert.deepEqual(results[1], { nodeId: remote.nodeId, ok: true, value: 5 });
});

test("listens on a websocket path and advertises it", async (t) => {
  const provider = new Node({ cluster: "ws-path-provider" });
  const consumer = new Node({ cluster: "ws-path-consumer" });
  const http = createHttpServer((_request, response) => response.end("ok"));
  const port = await freePort();

  await new Promise<void>((resolve) => http.listen(port, "127.0.0.1", resolve));

  t.after(() => {
    consumer.close();
    provider.close();
    http.close();
  });

  provider.register("math", {
    add: async (a: number, b: number) => a + b,
  });

  const listening = await provider.listen(`ws://127.0.0.1:${port}/swarm`, {
    advertise: `ws://127.0.0.1:${port}`,
    ws: { server: http },
  });

  assert.equal(listening.port, port);
  assert.equal(
    (provider as unknown as NodeInternals).members[0].addr,
    `ws://127.0.0.1:${port}/swarm`,
  );

  const up = new Promise<void>((resolve) =>
    consumer.watch("math", (event) => {
      if (event.type === "up" && event.instance.nodeId === provider.nodeId) {
        resolve();
      }
    }),
  );

  await assert.rejects(
    () => consumer.connect(`ws://127.0.0.1:${port}`, { reconnect: false }),
    (err: SwarmError) => err.kind === "failed",
  );

  await consumer.connect(`ws://127.0.0.1:${port}/swarm`);
  await up;

  assert.equal(await consumer.call("math", "add", [1, 2]), 3);

  consumer.close();
  provider.close();

  assert.equal(http.listening, true);
});

test("collect isolates a failing local instance", async (t) => {
  const local = new Node({ cluster: "collect-iso-local" });
  const remote = new Node({ cluster: "collect-iso-remote" });

  t.after(() => {
    local.close();
    remote.close();
  });

  const failure = new Error("boom");

  local.register("math", {
    add: () => {
      throw failure;
    },
  });
  remote.register("math", {
    add: async (a: number, b: number) => a + b,
  });

  const port = await freePort();

  await remote.listen(`tcp://127.0.0.1:${port}`);

  const up = new Promise<void>((resolve) =>
    local.watch("math", (event) => {
      if (event.type === "up" && event.instance.nodeId === remote.nodeId) {
        resolve();
      }
    }),
  );

  await local.connect(`tcp://127.0.0.1:${port}`);
  await up;

  const results = await local.collect("math", "add", [1, 2]);

  assert.equal(results.length, 2);
  assert.deepEqual(results[0], {
    nodeId: local.nodeId,
    ok: false,
    error: failure,
  });
  assert.deepEqual(results[1], { nodeId: remote.nodeId, ok: true, value: 3 });
});

test("collect rejects when no provider exists", async (t) => {
  const node = new Node({ cluster: "collect-none" });

  t.after(() => node.close());

  await assert.rejects(
    () => node.collect("missing", "add", [1, 2]),
    (error: SwarmError) => error.kind === "not-found",
  );
});

test("collect rejects after close", async () => {
  const node = new Node({ cluster: "collect-closed" });

  node.close();

  await assert.rejects(
    () => node.collect("math", "add", [1, 2]),
    (error: SwarmError) => error.kind === "rejected",
  );
});

test("seeds dial peers without an explicit connect", async (t) => {
  const port = await freePort();
  const seed = new Node({ cluster: "seed-server" });
  const node = new Node({
    cluster: "seed-client",
    seeds: [`tcp://127.0.0.1:${port}`],
  });

  t.after(() => {
    node.close();
    seed.close();
  });

  seed.register("math", { add: async (a: number, b: number) => a + b });

  const up = new Promise<void>((resolve) =>
    node.watch("math", (event) => {
      if (event.type === "up" && event.instance.nodeId === seed.nodeId) {
        resolve();
      }
    }),
  );

  await seed.listen(`tcp://127.0.0.1:${port}`);
  await node.listen("tcp://127.0.0.1:0");
  await up;

  assert.equal(await node.call("math", "add", [1, 2]), 3);
});

test("peers converge through a shared seed", async (t) => {
  const seedPort = await freePort();
  const leftPort = await freePort();
  const rightPort = await freePort();
  const seed = new Node({ cluster: "mesh-seed" });
  const left = new Node({
    cluster: "mesh-left",
    seeds: [`tcp://127.0.0.1:${seedPort}`],
  });
  const right = new Node({
    cluster: "mesh-right",
    seeds: [`tcp://127.0.0.1:${seedPort}`],
  });

  t.after(() => {
    left.close();
    right.close();
    seed.close();
  });

  right.register("math", { add: async (a: number, b: number) => a + b });

  const up = new Promise<void>((resolve) =>
    left.watch("math", (event) => {
      if (event.type === "up" && event.instance.nodeId === right.nodeId) {
        resolve();
      }
    }),
  );

  await seed.listen(`tcp://0.0.0.0:${seedPort}`);
  await left.listen(`tcp://0.0.0.0:${leftPort}`, {
    advertise: `tcp://127.0.0.1:${leftPort}`,
  });
  await right.listen(`tcp://0.0.0.0:${rightPort}`, {
    advertise: `tcp://127.0.0.1:${rightPort}`,
  });
  await up;

  assert.equal(await left.call("math", "add", [1, 2]), 3);
});

test("discovers a peer over multicast without seeds", async (t) => {
  const providerPort = await freePort();
  const multicastPort = await freeUdpPort();
  const options = {
    multicast: {
      port: multicastPort,
      interval: 100,
      interface: "127.0.0.1",
    },
  };
  const consumer = new Node({ cluster: "multicast", ...options });
  const provider = new Node({ cluster: "multicast", ...options });

  t.after(() => {
    consumer.close();
    provider.close();
  });

  provider.register("math", { add: (a: number, b: number) => a + b });

  const up = new Promise<boolean>((resolve) =>
    consumer.watch("math", (event) => {
      if (event.type === "up" && event.instance.nodeId === provider.nodeId) {
        resolve(true);
      }
    }),
  );

  await consumer.listen("tcp://127.0.0.1:0");
  await provider.listen(`tcp://127.0.0.1:${providerPort}`, {
    advertise: `tcp://127.0.0.1:${providerPort}`,
  });

  if ((await within(up, 2000)) === undefined) {
    t.skip("multicast packets are not delivered");
    return;
  }

  assert.equal(await consumer.call("math", "add", [1, 2]), 3);
});

test("listen advertises the configured address", async (t) => {
  const port = await freePort();
  const node = new Node({ cluster: "advertise-on" });
  const { port1, port2 } = new MessageChannel();

  t.after(() => {
    node.close();
    port1.close();
    port2.close();
  });

  await node.listen(`tcp://0.0.0.0:${port}`, {
    backlog: 10,
    advertise: `tcp://127.0.0.1:${port}`,
  });

  const learned = new Promise<Array<{ id: string; addr: string }>>(
    (resolve) => {
      port2.addEventListener("message", (event) => {
        const message = event.data as { t?: string; members?: unknown };

        if (
          message.t === "peers" &&
          Array.isArray(message.members) &&
          message.members.length > 0
        ) {
          resolve(message.members as Array<{ id: string; addr: string }>);
        }
      });
    },
  );

  node.join(port1);
  port2.start();
  port2.postMessage({ t: "hello", from: "ghost" });

  assert.deepEqual(await learned, [
    { id: node.nodeId, addr: `tcp://127.0.0.1:${port}` },
  ]);
});

test("listen rejects an undialable advertise address", async (t) => {
  const node = new Node({ cluster: "advertise-invalid" });

  t.after(() => node.close());

  for (const advertise of [
    "tcp://0.0.0.0:1",
    "tcp://[::]:1",
    "tcp://[::ffff:0.0.0.0]:1",
  ]) {
    await assert.rejects(
      node.listen("tcp://127.0.0.1:0", { advertise }),
      (err: SwarmError) =>
        err.kind === "rejected" && err.message.startsWith("invalid advertise"),
    );
  }
});

test("listen fills the bound port when advertise uses port 0", async (t) => {
  const node = new Node({ cluster: "advertise-port-zero" });
  const { port1, port2 } = new MessageChannel();

  t.after(() => {
    node.close();
    port1.close();
    port2.close();
  });

  const bound = await node.listen("tcp://0.0.0.0:0", {
    advertise: "tcp://127.0.0.1:0",
  });

  const learned = new Promise<Array<{ id: string; addr: string }>>(
    (resolve) => {
      port2.addEventListener("message", (event) => {
        const message = event.data as { t?: string; members?: unknown };

        if (
          message.t === "peers" &&
          Array.isArray(message.members) &&
          message.members.length > 0
        ) {
          resolve(message.members as Array<{ id: string; addr: string }>);
        }
      });
    },
  );

  node.join(port1);
  port2.start();
  port2.postMessage({ t: "hello", from: "ghost" });

  assert.deepEqual(await learned, [
    { id: node.nodeId, addr: `tcp://127.0.0.1:${bound.port}` },
  ]);
});

test("listen does not advertise without options", async (t) => {
  const node = new Node({ cluster: "advertise-off" });
  const { port1, port2 } = new MessageChannel();

  t.after(() => {
    node.close();
    port1.close();
    port2.close();
  });

  await node.listen("tcp://0.0.0.0:0");

  const learned = new Promise<unknown[]>((resolve) => {
    port2.addEventListener("message", (event) => {
      const message = event.data as { t?: string; members?: unknown };

      if (message.t === "peers" && Array.isArray(message.members)) {
        resolve(message.members);
      }
    });
  });

  node.join(port1);
  port2.start();
  port2.postMessage({ t: "hello", from: "ghost" });

  assert.deepEqual(await learned, []);
});

test("peers ignores invalid or wildcard entries", async (t) => {
  const node = new Node({ cluster: "peers-invalid" });
  const { port1, port2 } = new MessageChannel();

  t.after(() => {
    node.close();
    port1.close();
    port2.close();
  });

  node.join(port1);
  port2.start();

  const learned = new Promise<Array<{ id: string; addr: string }>>(
    (resolve) => {
      port2.addEventListener("message", (event) => {
        const message = event.data as { t?: string; members?: unknown };

        if (
          message.t === "peers" &&
          Array.isArray(message.members) &&
          message.members.length > 0
        ) {
          resolve(message.members as Array<{ id: string; addr: string }>);
        }
      });
    },
  );

  port2.postMessage({ t: "hello", from: "ghost" });
  port2.postMessage({
    t: "peers",
    from: "ghost",
    members: [
      { id: "wild", addr: "tcp://0.0.0.0:1" },
      { id: "bad", addr: "not a url" },
      { id: "0", addr: "tcp://127.0.0.1:1" },
      { id: "00", addr: "tcp://192.0.2.1:1" },
    ],
  });

  assert.deepEqual(await learned, [
    { id: "0", addr: "tcp://127.0.0.1:1" },
    { id: "00", addr: "tcp://192.0.2.1:1" },
  ]);
});

test("routes a call through a client-only relay", async (t) => {
  const consumerPort = await freePort();
  const providerPort = await freePort();
  const consumer = new Node({ cluster: "relay-consumer" });
  const relay = new Node({ cluster: "relay-middle" });
  const provider = new Node({ cluster: "relay-provider" });

  t.after(() => {
    consumer.close();
    relay.close();
    provider.close();
  });

  provider.register("math", { add: async (a: number, b: number) => a + b });

  const up = new Promise<void>((resolve) =>
    consumer.watch("math", (event) => {
      if (event.type === "up" && event.instance.nodeId === provider.nodeId) {
        resolve();
      }
    }),
  );

  await consumer.listen(`tcp://127.0.0.1:${consumerPort}`);
  await provider.listen(`tcp://127.0.0.1:${providerPort}`);
  await relay.connect(`tcp://127.0.0.1:${consumerPort}`);
  await relay.connect(`tcp://127.0.0.1:${providerPort}`);
  await up;

  assert.equal(await consumer.call("math", "add", [1, 2]), 3);

  const down = new Promise<void>((resolve) =>
    consumer.watch("math", (event) => {
      if (event.type === "down" && event.instance.nodeId === provider.nodeId) {
        resolve();
      }
    }),
  );

  provider.unregister("math");
  await down;
});

test("rejects a forwarded call whose hop budget is exhausted", async (t) => {
  const node = new Node({ cluster: "hop-budget" });
  const { port1, port2 } = new MessageChannel();
  const up = new Promise<void>((resolve) =>
    node.watch("math", () => resolve()),
  );
  const failed = new Promise<{ t?: string; kind?: string }>((resolve) => {
    port2.addEventListener("message", (event) => {
      const message = event.data as { t?: string; kind?: string };

      if (message.t === "error") {
        resolve(message);
      }
    });
  });

  t.after(() => {
    node.close();
    port1.close();
    port2.close();
  });

  node.join(port1);
  port2.start();
  port2.postMessage({ t: "hello", from: "peer" });
  port2.postMessage({ t: "up", from: "peer", service: "math" });
  await up;

  port2.postMessage({
    t: "call",
    id: "hop",
    from: "peer",
    to: node.nodeId,
    service: "math",
    method: "add",
    args: [],
    ttl: 0,
  });

  assert.equal((await failed).kind, "not-found");
});

test("dial rejects a peer that does not match the advertised id", async (t) => {
  const target = new Node({ cluster: "dial-identity-target" });
  const node = new Node({ cluster: "dial-identity" });
  const { port1, port2 } = new MessageChannel();

  t.after(() => {
    node.close();
    target.close();
    port1.close();
    port2.close();
  });

  target.register("math", { add: (x: number, y: number) => x + y });
  node.register("pingpong", { ping: () => "pong" });

  const bound = await target.listen("tcp://127.0.0.1:0");

  const up = new Promise<WatchEvent>((resolve) =>
    target.watch("pingpong", resolve),
  );

  node.join(port1);
  port2.start();
  port2.postMessage({ t: "hello", from: "ghost" });
  port2.postMessage({
    t: "peers",
    from: "ghost",
    members: [{ id: "expected", addr: `tcp://127.0.0.1:${bound.port}` }],
  });

  const event = await within(up, 2000);

  assert.notEqual(event, undefined);
  assert.equal(event?.type, "up");

  let rejected = false;

  for (let attempt = 0; attempt < 20 && !rejected; attempt += 1) {
    try {
      await node.call("math", "add", [1, 2]);
    } catch {
      rejected = true;
      break;
    }

    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  assert.equal(rejected, true);
});

test("peers drops loopback members from a non-local link", (t) => {
  const node = new Node({ cluster: "peers-loopback" });
  const internals = node as unknown as NodeInternals;
  const self = { id: node.nodeId, addr: "tcp://127.0.0.1:9" };
  const remote = {
    id: "00000000000000000000000000000000",
    addr: "tcp://192.168.0.9:9",
  };

  t.after(() => node.close());

  internals.members.push(self);
  internals.learn([{ id: "local", addr: "tcp://127.0.0.1:9" }], {
    local: false,
  });
  internals.learn([remote], { local: false });

  assert.deepEqual(
    internals.members.map((member) => member.id),
    [self.id, remote.id],
  );
});

test("expires members whose advertisements stopped", async (t) => {
  const port = await freePort();
  const node = new Node({ cluster: "members-expire", memberTtl: 1000 });
  const internals = node as unknown as NodeInternals;
  const remote = {
    id: "00000000000000000000000000000000",
    addr: `tcp://127.0.0.1:${port}`,
  };

  t.after(() => node.close());

  internals.learn([remote], { local: true });

  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.equal(internals.members.length, 1);

  internals.seen.set(remote.id, Date.now() - 2000);
  internals.learn([], { local: true });

  assert.equal(internals.members.length, 0);
  assert.equal(internals.seen.has(remote.id), false);
  assert.equal(internals.dials.has(remote.addr), true);
  assert.equal(internals.expiries.has(remote.addr), true);
  assert.equal(internals.clients.length, 1);
});

test("drops a dial after the eviction window", async (t) => {
  const port = await freePort();
  const node = new Node({
    cluster: "members-dial-ttl",
    memberTtl: 1000,
    dialTtl: 1000,
  });
  const internals = node as unknown as NodeInternals;
  const remote = {
    id: "00000000000000000000000000000000",
    addr: `tcp://127.0.0.1:${port}`,
  };

  t.after(() => node.close());

  internals.learn([remote], { local: true });

  await new Promise((resolve) => setTimeout(resolve, 10));

  internals.seen.set(remote.id, Date.now() - 2000);
  internals.learn([], { local: true });

  assert.equal(internals.dials.has(remote.addr), true);
  assert.equal(internals.expiries.has(remote.addr), true);

  internals.expiries.set(remote.addr, Date.now() - 1);
  internals.learn([], { local: true });

  assert.equal(internals.dials.has(remote.addr), false);
  assert.equal(internals.clients.length, 0);
  assert.equal(internals.expiries.has(remote.addr), false);
});

test("restores a member when the dial link reconnects", async (t) => {
  const a = new Node({ cluster: "members-restore" });
  const b = new Node({ cluster: "members-restore" });
  const internals = b as unknown as NodeInternals & {
    nodeLinks: unknown[];
  };

  t.after(() => {
    a.close();
    b.close();
  });

  const address = await a.listen("tcp://127.0.0.1:0");

  internals.learn([{ id: a.nodeId, addr: `tcp://127.0.0.1:${address.port}` }], {
    local: true,
  });

  const linked = await within(
    new Promise<boolean>((resolve) => {
      const tick = () => {
        if (internals.nodeLinks.length > 0) {
          resolve(true);
          return;
        }

        setTimeout(tick, 20);
      };

      tick();
    }),
    2000,
  );

  assert.equal(linked, true);

  (a as unknown as { servers: Array<{ close(): void }> }).servers[0].close();

  internals.members.length = 0;
  internals.seen.clear();

  await new Promise((resolve) => setTimeout(resolve, 50));

  await a.listen(`tcp://127.0.0.1:${address.port}`);

  const restored = await within(
    new Promise<boolean>((resolve) => {
      const tick = () => {
        if (internals.members.some((member) => member.id === a.nodeId)) {
          resolve(true);
          return;
        }

        setTimeout(tick, 20);
      };

      tick();
    }),
    4000,
  );

  assert.equal(restored, true);
});

test("refreshes member freshness when the link closes", async (t) => {
  const node = new Node({ cluster: "members-detach", memberTtl: 1000 });
  const { port1, port2 } = new MessageChannel();
  const internals = node as unknown as NodeInternals & {
    nodeLinks: Array<{ peerId?: string; close(): void }>;
  };

  t.after(() => {
    node.close();
    port1.close();
    port2.close();
  });

  node.attach(port1);
  port2.start();
  port2.postMessage({ t: "hello", from: "peer" });

  const opened = await within(
    new Promise<boolean>((resolve) => {
      const tick = () => {
        const nodeLink = internals.nodeLinks[0];

        if (nodeLink !== undefined && nodeLink.peerId === "peer") {
          resolve(true);
          return;
        }

        setTimeout(tick, 10);
      };

      tick();
    }),
    2000,
  );

  assert.equal(opened, true);

  internals.learn([{ id: "peer", addr: "tcp://10.0.0.9:9" }], { local: true });
  internals.seen.set("peer", Date.now() - 5000);

  const before = Date.now();

  internals.nodeLinks[0].close();

  assert.equal(internals.members.length, 1);
  assert.equal((internals.seen.get("peer") as number) >= before, true);

  internals.learn([], { local: true });

  assert.equal(internals.members.length, 1);
});

type NodeInternals = {
  members: Array<{ id: string; addr: string }>;
  seen: Map<string, number>;
  dials: Map<string, unknown>;
  expiries: Map<string, number>;
  clients: unknown[];
  learn(
    entries: Array<{ id: string; addr: string }>,
    link: { local: boolean },
  ): void;
};
