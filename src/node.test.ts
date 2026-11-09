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
import { WebSocket } from "ws";
import { Node } from "./node.ts";
import type { AddressInfo } from "node:net";
import type { SwarmError } from "./errors.ts";
import type { MetaData, WatchEvent } from "./registry.ts";

const tlsKey = `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCbnk+vu+HrTf2i
0sRCnjDXiRCarl558kOM7xS0phgWwdpaJ6VRZnwAtI9Z90zrhs26G//mRDDv9ogi
ZZdSeRNEnMeM/3JY6ztvETIHLkGYBo8lnd/X7tknaZjNemyp4HAdixXX8s/3d8v9
WovE2LhXcDY4kdzEchJpflllMRGbF5F9MNyT8OWpi9c+1+QUTuK/wCjgukhJPQwN
ynbJn7GZLX+gOGw3eTgnZ421PZSjffPRTphGiGfpfq4l94/zEnhNwM7trfSU55tj
nyo9A6xp+rcebWb9MoThALOAvV0zVSATBx50INkHsLu4rGqDinEjvLD3uJdRY4BY
y/iIYq5hAgMBAAECggEAD8zm/gqqDshYQIrSNt9f9H/BX1XlWCVjJXFK8r1LLy2s
nBXFa8/njEdzp1NRjI0fNfkiJkD15Q9s1W084Y393la6zilLn2qhmg6EEVrpebma
GYbU6P732SVtcy4tz/Zupm6Y9QL9iadbX2nXoiYiWsJqcZuhxOe+Jdbt1bVB5TNl
S5aXWhJoXBgQ0EapQ+Ex7O+OzKOan7cEk8xZ+Bsuhx1KSeOgZnDfKaNeVuElsyg1
NVdQuC83b+NMUPYt9LBUTPDfe3zjWZh92X3aPgiGzrk0PRH1Ymjf5wl31buu3Nbz
1wTXHq+UMTHDqAJ+wuBHJS6S+aKUnDe10lo4X3w4TwKBgQDUhFk+U8OQLizWI/39
pvhG1jBuDG1L+/hhAMEDASMV5G1jm0MN5eWfwSMA812mosHOUfuTen/31h1tC6vM
KOG5nFD5wDfonFKv+6FliSfFNidnb1ev9vtyNDl4kRztgX3eA4nUlEsEHuLIJGQI
76oHhPaD0ox4nDuQ2NAUKjtyVwKBgQC7dZycUiCOsJ6R+/WCOZAReMSojcqbL5aD
Rx6ozKRw41P0hvjOlYvdZhzY8sEL3WHyg3Plst5EhW8vqjN998to4ilazzqGFq+T
kY16ezylvsyWJd3KgK5jgHnksWkALQkmRZs6NXKG5y1x79Pov38AFHIOsWAf9ZdF
u5nwkyUiBwKBgQC+BBBfoDnN4tuIJOpnMDHx9HWdEFNxizwMT8ZET3H1kycT1rFf
7d0guGn54aAs7H3pYnhU50UgbjwlUjgr9anOciwdA3d57rwoSg+HX9lFhiQ5Yd6/
oVA2TufMG8QHwK6wIe3NO0LPzmXrmk4vh0s6KfLY7xtWhCrmWlXCkfhRRQKBgCvF
p78csjt/ZFeENOymkkxyHcGZ8lBQSV34EmGp/0/Y1TbI+XoLk3AsMto5vPgPRprB
NSPSo3HukmrX3FQys02XWb65mtnKnE7h8t5BvoSZS95XkzWA3LehPVU4U+NVoMhO
ApQR/N8hRgxPyvSTGo2jOIrTwjVUPHFs/+IcCdSrAoGAMHq83pTop2kJfo7oyyTR
79YmZcNmIp417EDpHZKY34imr32Npx3WkQsopv+wnfv17BmfuQobISD7CSamIKu+
EZG9dtehFadtGWzeFHGAMs5sbBynPmCvJeuYelK/hOwXVBywgZc2wcdVishkPlbL
efxovqRrI7cysMEuqDP5l2g=
-----END PRIVATE KEY-----
`;

const tlsCert = `-----BEGIN CERTIFICATE-----
MIIC+TCCAeGgAwIBAgIUOxlZh+y8DiAL9Oe1TxkGM9J2adMwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJMTI3LjAuMC4xMB4XDTI2MTEwODEzNTg1NloXDTM2MTEw
NTEzNTg1NlowFDESMBAGA1UEAwwJMTI3LjAuMC4xMIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAm55Pr7vh6039otLEQp4w14kQmq5eefJDjO8UtKYYFsHa
WielUWZ8ALSPWfdM64bNuhv/5kQw7/aIImWXUnkTRJzHjP9yWOs7bxEyBy5BmAaP
JZ3f1+7ZJ2mYzXpsqeBwHYsV1/LP93fL/VqLxNi4V3A2OJHcxHISaX5ZZTERmxeR
fTDck/DlqYvXPtfkFE7iv8Ao4LpIST0MDcp2yZ+xmS1/oDhsN3k4J2eNtT2Uo33z
0U6YRohn6X6uJfeP8xJ4TcDO7a30lOebY58qPQOsafq3Hm1m/TKE4QCzgL1dM1Ug
EwcedCDZB7C7uKxqg4pxI7yw97iXUWOAWMv4iGKuYQIDAQABo0MwQTAdBgNVHQ4E
FgQU/upo5ULGXNigxu7UqnwNZUqLPXowDwYDVR0TAQH/BAUwAwEB/zAPBgNVHREE
CDAGhwR/AAABMA0GCSqGSIb3DQEBCwUAA4IBAQB/CqD0dHE2hPsrEsAVc4TZgyPH
nA1EpRIh9I9lXKn4H/2LDh9x/ZOiPEXli5XcnMY4UcK9wrE5F7mW+KQJdCiSzBoN
TQUCjhU2Lx2EYbupBchBLkmVn8DzxFZvjB5iDb2ejP0xb+MjGU2ealZKB+goZftU
GxHNG5GzXjOMVmM43v8ekyDruwyr4hKlqhMzC2/RqQYtkVWfYzpysvQN9mc5zZjK
kReN6WwvUPN4NJ2QndOu8RP/XXzGLEEoyBcbS5QvNHMTcY0VySLBvXSwvn4mGsdK
mJTnKt8LKtGnmhK7vIGP5kth9i5duTSYnhdEA/AG9D0D3+NnsVH58Pj09mTg
-----END CERTIFICATE-----
`;

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

async function waitFor(check: () => boolean, timeout = 2000) {
  const deadline = Date.now() + timeout;

  while (Date.now() < deadline) {
    if (check()) {
      return true;
    }

    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }

  return false;
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
  port2.postMessage({ t: "hello", from: "ghost", version: 1 });
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

  test("call returns a promise for a local service", async (t) => {
    const node = makeNode();

    t.after(() => node.close());

    node.register("math", { add: () => 3 });

    const call = node.call("math", "add");

    assert.equal(call instanceof Promise, true);
    assert.equal(await call, 3);

    const targeted = node.callTo(node.nodeId, "math", "add");

    assert.equal(targeted instanceof Promise, true);
    assert.equal(await targeted, 3);
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

  test("re-registering updates the service metadata", async (t) => {
    const a = new Node({ cluster: "meta-a" });
    const b = new Node({ cluster: "meta-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const first = new Promise<unknown>((resolve) =>
      b.watch("math", (event) => {
        if (event.instance.nodeId === a.nodeId) {
          resolve(event.instance.meta);
        }
      }),
    );

    a.attach(port1);
    b.attach(port2);

    a.register("math", { add: () => 1 }, { build: 1 });

    assert.deepEqual(await within(first, 2000), { build: 1 });

    const second = new Promise<unknown>((resolve) =>
      b.watch("math", (event) => {
        const meta = event.instance.meta as { build?: number } | undefined;

        if (event.instance.nodeId === a.nodeId && meta?.build === 2) {
          resolve(meta);
        }
      }),
    );

    a.register("math", { add: () => 2 }, { build: 2 });

    assert.deepEqual(await within(second, 2000), { build: 2 });
  });

  test("unregister propagates through a triangle where peers know the owner", async (t) => {
    const a = new Node({ cluster: "echo-tri-a" });
    const b = new Node({ cluster: "echo-tri-b" });
    const c = new Node({ cluster: "echo-tri-c" });
    const ac = new MessageChannel();
    const cb = new MessageChannel();
    const ba = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      c.close();
    });

    a.register("math", { add: () => 1 }, { build: 1 });

    a.attach(ac.port1);
    c.attach(ac.port2);
    assert.equal(await waitFor(() => c.snapshot().services.length === 1), true);

    c.attach(cb.port1);
    b.attach(cb.port2);
    assert.equal(await waitFor(() => b.snapshot().services.length === 1), true);

    b.attach(ba.port1);
    a.attach(ba.port2);
    assert.equal(await waitFor(() => a.snapshot().links.length === 2), true);

    a.register("math", { add: () => 2 }, { build: 2 });

    assert.equal(
      await waitFor(() => {
        const meta = b.snapshot().services[0]?.meta as
          { build?: number } | undefined;

        return meta?.build === 2;
      }),
      true,
    );

    a.unregister("math");

    assert.equal(await waitFor(() => a.snapshot().services.length === 0), true);
    assert.equal(await waitFor(() => b.snapshot().services.length === 0), true);
    assert.equal(await waitFor(() => c.snapshot().services.length === 0), true);
  });

  test("unregister propagates across duplicate links to the same peer", async (t) => {
    const a = new Node({ cluster: "echo-dup-a" });
    const b = new Node({ cluster: "echo-dup-b" });
    const first = new MessageChannel();
    const second = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
    });

    a.register("math", { add: () => 1 }, { build: 1 });

    a.attach(first.port1);
    b.attach(first.port2);
    assert.equal(await waitFor(() => b.snapshot().services.length === 1), true);

    a.attach(second.port1);
    b.attach(second.port2);
    assert.equal(await waitFor(() => b.snapshot().links.length === 2), true);

    a.unregister("math");

    assert.equal(await waitFor(() => a.snapshot().services.length === 0), true);
    assert.equal(await waitFor(() => b.snapshot().services.length === 0), true);
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

  test("operations fail after close", async (t) => {
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
    await assert.rejects(
      node.call("math", "add"),
      (err: SwarmError) => err.kind === "rejected",
    );
  });

  test("snapshot reports closed and empty after close", () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });

    assert.equal(node.snapshot().closed, false);
    assert.equal(node.snapshot().services.length, 1);

    node.close();

    const snap = node.snapshot();
    assert.equal(snap.closed, true);
    assert.deepEqual(snap.services, []);
    assert.deepEqual(snap.members, []);
    assert.deepEqual(snap.links, []);
  });

  test("snapshot exposes seeds, multicast state, and health", (t) => {
    const node = new Node({
      cluster: "obs-snapshot",
      seeds: ["tcp://127.0.0.1:1234", new URL("ws://127.0.0.1:5678")],
    });

    t.after(() => node.close());

    const snap = node.snapshot();

    assert.deepEqual(snap.seeds, [
      "tcp://127.0.0.1:1234",
      "ws://127.0.0.1:5678/",
    ]);
    assert.equal(snap.multicast, false);
    assert.equal(snap.health, "isolated");
  });

  test("snapshot link info includes setup duration and queue depth", async (t) => {
    const hub = new Node({ cluster: "obs-link-info" });
    const peer = new Node({ cluster: "obs-link-peer" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    const address = await hub.listen("tcp://127.0.0.1:0");
    await peer.connect(`tcp://127.0.0.1:${address.port}`);

    await waitFor(
      () =>
        hub.snapshot().links.length === 1 &&
        hub.snapshot().links[0].setup !== undefined,
    );

    const link = hub.snapshot().links[0];

    assert.equal(typeof link.setup, "number");
    assert.equal(link.setup !== undefined && link.setup >= 0, true);
    assert.equal(link.queue, 0);
  });

  test("emits link-up and link-down events", async (t) => {
    const node = new Node({ cluster: "obs-link-events" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      node.close();
      port2.close();
    });

    const ups: unknown[] = [];
    const downs: unknown[] = [];

    node.events.on("link-up", (link) => ups.push(link));
    node.events.on("link-down", (link) => downs.push(link));

    node.join(port1);
    port2.start();
    port2.postMessage({ t: "hello", from: "ghost", version: 1 });

    await waitFor(() => ups.length === 1);

    port2.close();

    await waitFor(() => downs.length === 1);
  });

  test("emits member-up and member-down events", async (t) => {
    const hub = new Node({ cluster: "obs-member-events" });
    const peer = new Node({ cluster: "obs-member-peer" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    const ups: unknown[] = [];
    const downs: unknown[] = [];

    hub.events.on("member-up", (member) => ups.push(member));
    hub.events.on("member-down", (member) => downs.push(member));

    const address = await hub.listen("tcp://127.0.0.1:0");
    await peer.connect(`tcp://127.0.0.1:${address.port}`);

    await waitFor(() =>
      ups.some((member) => (member as { id?: string }).id === peer.nodeId),
    );

    peer.close();

    await waitFor(() =>
      downs.some((member) => (member as { id?: string }).id === peer.nodeId),
    );
  });

  test("accepts an error event listener", () => {
    const node = makeNode();
    let received: Error | undefined;

    node.events.on("error", (error) => {
      received = error;
    });

    node.close();

    assert.equal(received, undefined);
  });

  test("call rejects with cancelled when the signal aborts", async (t) => {
    const node = new Node({ cluster: "cancel-call" });

    t.after(() => node.close());

    node.register("slow", {
      work: () => new Promise(() => {}),
    });

    const controller = new AbortController();
    const promise = node.call("slow", "work", [], {
      signal: controller.signal,
    });

    controller.abort();

    await assert.rejects(
      promise,
      (err: SwarmError) =>
        err.kind === "cancelled" && err.message.includes("cancelled"),
    );
  });

  test("leave removes a member and closes its link", async (t) => {
    const hub = new Node({ cluster: "leave-hub" });
    const peer = new Node({ cluster: "leave-peer" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    const address = await hub.listen("tcp://127.0.0.1:0");
    await peer.connect(`tcp://127.0.0.1:${address.port}`);

    await waitFor(
      () =>
        hub.snapshot().members.some((m) => m.id === peer.nodeId) &&
        hub.snapshot().links.length > 0,
    );

    hub.leave(peer.nodeId);

    await waitFor(() => hub.snapshot().links.length === 0);
    assert.equal(
      hub.snapshot().members.some((m) => m.id === peer.nodeId),
      false,
    );
  });

  test("rejects calls that exceed the per-link in-flight limit", async (t) => {
    const a = new Node({ cluster: "bp-a", maxInflight: 1 });
    const b = new Node({ cluster: "bp-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    let release: (value?: unknown) => void;
    const started = new Promise<void>((resolve) => {
      b.register("slow", {
        work: () => {
          resolve();
          return new Promise((r) => {
            release = r;
          });
        },
      });
    });

    const up = new Promise<WatchEvent>((resolve) => a.watch("slow", resolve));

    a.attach(port1);
    b.attach(port2);

    await up;

    const first = a.call("slow", "work", [], { timeout: 60000 });
    await started;

    await assert.rejects(
      a.call("slow", "work", [], { timeout: 60000 }),
      (err: SwarmError) =>
        err.kind === "rejected" && err.message.includes("in-flight"),
    );

    release!();
    await first;
  });

  test("addSeed dials the new seed and removeSeed drops it", async (t) => {
    const hub = new Node({ cluster: "seed-hub" });
    const node = new Node({ cluster: "seed-node" });

    t.after(() => {
      hub.close();
      node.close();
    });

    const address = await hub.listen("tcp://127.0.0.1:0");

    assert.deepEqual(node.snapshot().seeds, []);

    node.addSeed(`tcp://127.0.0.1:${address.port}`);

    assert.equal(node.snapshot().seeds.length, 1);

    await waitFor(
      () =>
        node.snapshot().members.some((m) => m.id === hub.nodeId) ||
        node.snapshot().links.length > 0,
    );

    node.removeSeed(`tcp://127.0.0.1:${address.port}`);
    assert.deepEqual(node.snapshot().seeds, []);
  });

  test("keeps redundant explicit links to the same peer", async (t) => {
    const a = new Node({ cluster: "redun-a" });
    const b = new Node({ cluster: "redun-b" });
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
    a.attach(second.port1);
    b.attach(second.port2);

    await waitFor(() => a.snapshot().links.length === 2);
    await up;
    assert.equal(await a.call("math", "add"), 1);
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

  test("does not retry a sent call when the serving link closes", async (t) => {
    const a = new Node({ cluster: "retry-a" });
    const b = new Node({ cluster: "retry-b" });
    const c = new Node({ cluster: "retry-c" });
    const first = new MessageChannel();
    const second = new MessageChannel();
    let cCalls = 0;

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

    const result = a.call("math", "add");

    b.close();

    await assert.rejects(
      result,
      (err: SwarmError) => err.kind === "failed" && err.retriable === false,
    );

    assert.equal(cCalls, 0);
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

  test("attach rejects a port that is already linked", (t) => {
    const a = new Node({ cluster: "dup-attach-a" });
    const b = new Node({ cluster: "dup-attach-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.attach(port1);

    assert.throws(
      () => b.attach(port1),
      (err: SwarmError) =>
        err.kind === "rejected" && /already linked/.test(err.message),
    );
  });

  test("join rejects a port that is already attached", (t) => {
    const a = new Node({ cluster: "misuse-join-a" });
    const b = new Node({ cluster: "misuse-join-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.attach(port1);

    assert.throws(
      () => b.join(port1),
      (err: SwarmError) =>
        err.kind === "rejected" && /already linked/.test(err.message),
    );
  });

  test("attach rejects a port that is already joined", (t) => {
    const a = new Node({ cluster: "misuse-attach-a" });
    const b = new Node({ cluster: "misuse-attach-b" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.join(port1);

    assert.throws(
      () => b.attach(port1),
      (err: SwarmError) =>
        err.kind === "rejected" && /already linked/.test(err.message),
    );
  });

  test(
    "attach cleans up the link when the port is closed",
    {
      timeout: 1000,
    },
    async (t) => {
      const node = new Node({ cluster: "closed-port-cleanup" });
      const { port1, port2 } = new MessageChannel();

      t.after(() => {
        node.close();
        port2.close();
      });

      port1.close();
      node.attach(port1);

      await new Promise((resolve) => setTimeout(resolve, 100));

      assert.equal(node.snapshot().links.length, 0);
    },
  );

  test("attach cleans up when the initial hello fails", (t) => {
    const node = new Node({ cluster: "hello-fail-cleanup" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      node.close();
      port2.close();
    });

    const failingPort = new Proxy(port1, {
      get(target, prop, receiver) {
        if (prop === "postMessage") {
          return (message: unknown) => {
            if ((message as { t?: string })?.t === "hello") {
              throw new Error("simulated send failure");
            }
            return target.postMessage(message);
          };
        }
        const value = Reflect.get(target, prop, receiver);
        if (typeof value === "function") {
          return value.bind(target);
        }
        return value;
      },
    });

    assert.throws(
      () => node.attach(failingPort as unknown as MessagePort),
      (err: SwarmError) => err.kind === "failed",
    );

    assert.equal(node.snapshot().links.length, 0);
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

        await assert.rejects(
          hub.call("math", "add"),
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

test("listen failure detaches from an attached http server", async (t) => {
  const node = new Node({ cluster: "attach-failure" });
  const http = createHttpServer();

  t.after(() => node.close());

  await assert.rejects(
    () => node.listen("ws://127.0.0.1:0", { ws: { server: http } }),
    (err: SwarmError) =>
      err.kind === "rejected" && err.message === "http server is not listening",
  );

  assert.equal(http.listenerCount("upgrade"), 0);
});

test("repeated advertise keeps a single self member", async (t) => {
  const node = new Node({ cluster: "advertise-dedupe" });

  t.after(() => node.close());

  const first = await node.listen("tcp://127.0.0.1:0", {
    advertise: "tcp://127.0.0.1:0",
  });
  const second = await node.listen("tcp://127.0.0.1:0", {
    advertise: "tcp://127.0.0.1:0",
  });

  const self = node
    .snapshot()
    .members.filter((member) => member.id === node.nodeId);

  assert.equal(self.length, 1);
  assert.equal(self[0].addr, `tcp://127.0.0.1:${second.port}`);
  assert.notEqual(first.port, second.port);
});

test("snapshot reports links and services", async (t) => {
  const hub = new Node({ cluster: "snapshot-hub" });
  const peer = new Node({ cluster: "snapshot-peer" });

  t.after(() => {
    hub.close();
    peer.close();
  });

  const ready = new Promise<void>((resolve) =>
    hub.watch("math", () => resolve()),
  );

  hub.register("echo", { ping: () => 2 }, { build: 2 });
  peer.register("math", { add: () => 1 }, { build: 1 });

  const address = await hub.listen("tcp://127.0.0.1:0");

  await peer.connect(`tcp://127.0.0.1:${address.port}`);
  await ready;

  const links = hub.snapshot().links;

  assert.equal(links.length, 1);
  assert.equal(links[0].peerId, peer.nodeId);
  assert.equal(links[0].transport, "socket");
  assert.equal(links[0].local, true);
  assert.deepEqual(links[0].services, [
    { service: "math", nodeId: peer.nodeId, meta: { build: 1 } },
  ]);

  const services = hub.snapshot().services;

  assert.equal(services.length, 2);
  assert.deepEqual(
    services.find((service) => service.service === "echo"),
    { service: "echo", nodeId: hub.nodeId, meta: { build: 2 } },
  );
  assert.deepEqual(
    services.find((service) => service.service === "math"),
    { service: "math", nodeId: peer.nodeId, meta: { build: 1 } },
  );
});

test("snapshot distinguishes websocket links", async (t) => {
  const hub = new Node({ cluster: "snapshot-ws-hub" });
  const peer = new Node({ cluster: "snapshot-ws-peer" });

  t.after(() => {
    hub.close();
    peer.close();
  });

  const ready = new Promise<void>((resolve) =>
    hub.watch("math", () => resolve()),
  );

  peer.register("math", { add: () => 1 });

  const address = await hub.listen("ws://127.0.0.1:0");

  await peer.connect(`ws://127.0.0.1:${address.port}`);
  await ready;

  const links = hub.snapshot().links;

  assert.equal(links.length, 1);
  assert.equal(links[0].peerId, peer.nodeId);
  assert.equal(links[0].transport, "websocket");
  assert.equal(links[0].local, true);
});

test("rejects wss listen without tls options", async () => {
  const node = new Node({ cluster: "wss-no-tls" });

  await assert.rejects(
    node.listen("wss://127.0.0.1:0"),
    (err: SwarmError) =>
      err.kind === "rejected" &&
      err.message === "wss listen requires tls options",
  );

  node.close();
});

test("listens on wss with tls options", async (t) => {
  const node = new Node({ cluster: "wss-tls" });

  t.after(() => node.close());

  const { port } = await node.listen("wss://127.0.0.1:0", {
    advertise: "wss://127.0.0.1:0",
    ws: { tls: { key: tlsKey, cert: tlsCert } },
  });

  const socket = new WebSocket(`wss://127.0.0.1:${port}`, {
    rejectUnauthorized: false,
  });

  t.after(() => socket.close());

  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });

  assert.equal(await waitFor(() => node.snapshot().links.length === 1), true);
  assert.equal(node.snapshot().members[0].addr.startsWith("wss://"), true);
});

test("connects to wss with a custom ca", async (t) => {
  const hub = new Node({ cluster: "wss-ca-hub" });
  const peer = new Node({ cluster: "wss-ca-peer" });

  t.after(() => {
    hub.close();
    peer.close();
  });

  hub.register("math", { add: (a: number, b: number) => a + b });

  const ready = new Promise<void>((resolve) =>
    peer.watch("math", () => resolve()),
  );

  const { port } = await hub.listen("wss://127.0.0.1:0", {
    advertise: "wss://127.0.0.1:0",
    ws: { tls: { key: tlsKey, cert: tlsCert } },
  });

  await peer.connect(`wss://127.0.0.1:${port}`, { tls: { ca: tlsCert } });
  await ready;

  assert.equal(await peer.call("math", "add", [1, 2]), 3);
  assert.equal(hub.snapshot().links[0].transport, "websocket");
});

test("delivers a publish once across a mesh", async (t) => {
  const a = new Node({ cluster: "mesh" });
  const b = new Node({ cluster: "mesh" });
  const c = new Node({ cluster: "mesh" });
  const ab = new MessageChannel();
  const bc = new MessageChannel();
  const ca = new MessageChannel();
  const counts = { a: 0, b: 0, c: 0 };

  t.after(() => {
    a.close();
    b.close();
    c.close();
    ab.port1.close();
    ab.port2.close();
    bc.port1.close();
    bc.port2.close();
    ca.port1.close();
    ca.port2.close();
  });

  a.subscribe("news", () => {
    counts.a += 1;
  });
  b.subscribe("news", () => {
    counts.b += 1;
  });
  c.subscribe("news", () => {
    counts.c += 1;
  });

  a.attach(ab.port1);
  b.attach(ab.port2);
  b.attach(bc.port1);
  c.attach(bc.port2);
  c.attach(ca.port1);
  a.attach(ca.port2);

  const linked = () =>
    [a, b, c].every(
      (node) =>
        node.snapshot().links.length === 2 &&
        node.snapshot().links.every((link) => link.peerId !== undefined),
    );

  assert.equal(await waitFor(linked), true);

  a.publish("news", { n: 1 });

  assert.equal(
    await waitFor(() => counts.a === 1 && counts.b === 1 && counts.c === 1),
    true,
  );

  await new Promise<void>((resolve) => setTimeout(resolve, 50));

  assert.deepEqual(counts, { a: 1, b: 1, c: 1 });
});

test("drops a member when the peer leaves", async (t) => {
  const host = new Node({ cluster: "bye-host" });
  const peer = new Node({ cluster: "bye-peer" });

  t.after(() => {
    host.close();
    peer.close();
  });

  const address = await host.listen("ws://127.0.0.1:0", {
    advertise: "ws://127.0.0.1:0",
  });

  await peer.connect(`ws://127.0.0.1:${address.port}`);

  assert.equal(
    await waitFor(() =>
      peer.snapshot().members.some((member) => member.id === host.nodeId),
    ),
    true,
  );

  host.close();

  assert.equal(
    await waitFor(
      () =>
        !peer.snapshot().members.some((member) => member.id === host.nodeId),
    ),
    true,
  );

  assert.deepEqual(peer.snapshot().members, []);
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

test("honors a configured hop limit", async (t) => {
  const a = new Node({ cluster: "hops", maxHops: 1 });
  const b = new Node({ cluster: "hops" });
  const c = new Node({ cluster: "hops" });
  const ab = new MessageChannel();
  const bc = new MessageChannel();

  t.after(() => {
    a.close();
    b.close();
    c.close();
    ab.port1.close();
    ab.port2.close();
    bc.port1.close();
    bc.port2.close();
  });

  c.register("math", { add: () => 1 });

  const known = new Promise<void>((resolve) =>
    a.watch("math", () => resolve()),
  );

  a.attach(ab.port1);
  b.attach(ab.port2);
  b.attach(bc.port1);
  c.attach(bc.port2);

  await known;

  await assert.rejects(
    a.call("math", "add"),
    (err: SwarmError) => err.kind === "not-found",
  );
});

test("calls and collects a node behind an intermediate node", async (t) => {
  const a = new Node({ cluster: "chain" });
  const b = new Node({ cluster: "chain" });
  const c = new Node({ cluster: "chain" });
  const ab = new MessageChannel();
  const bc = new MessageChannel();
  let calls = 0;

  t.after(() => {
    a.close();
    b.close();
    c.close();
    ab.port1.close();
    ab.port2.close();
    bc.port1.close();
    bc.port2.close();
  });

  a.register("math", { add: (x: number, y: number) => x * y });
  c.register("math", {
    add: (x: number, y: number) => {
      calls += 1;
      return x + y;
    },
  });

  const known = new Promise<void>((resolve) =>
    a.watch("math", (event) => {
      if (event.type === "up" && event.instance.nodeId === c.nodeId) {
        resolve();
      }
    }),
  );

  a.attach(ab.port1);
  b.attach(ab.port2);
  b.attach(bc.port1);
  c.attach(bc.port2);

  await known;

  assert.equal(await a.callTo(c.nodeId, "math", "add", [1, 2]), 3);
  assert.equal(await a.callTo(a.nodeId, "math", "add", [1, 2]), 2);

  const results = await a.collect("math", "add", [1, 2]);
  const local = results.find((result) => result.nodeId === a.nodeId);
  const remote = results.find((result) => result.nodeId === c.nodeId);

  assert.equal(calls, 2);
  assert.equal(results.length, 2);
  assert.deepEqual(local, { nodeId: a.nodeId, ok: true, value: 2 });
  assert.deepEqual(remote, { nodeId: c.nodeId, ok: true, value: 3 });
});

test("rejects a call to a node that serves nothing", async (t) => {
  const a = new Node({ cluster: "chain-missing" });
  const b = new Node({ cluster: "chain-missing" });
  const channel = new MessageChannel();

  t.after(() => {
    a.close();
    b.close();
    channel.port1.close();
    channel.port2.close();
  });

  b.register("math", { add: (x: number, y: number) => x + y });

  const known = new Promise<void>((resolve) =>
    a.watch("math", () => resolve()),
  );

  a.attach(channel.port1);
  b.attach(channel.port2);

  await known;

  await assert.rejects(
    a.callTo("ffffffffffffffffffffffffffffffff", "math", "add", [1, 2]),
    (err: SwarmError) => err.kind === "not-found",
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
  port2.postMessage({ t: "hello", from: "ghost", version: 1 });

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
  port2.postMessage({ t: "hello", from: "ghost", version: 1 });

  assert.deepEqual(await learned, [
    { id: node.nodeId, addr: `tcp://127.0.0.1:${bound.port}` },
  ]);
});

test("listen fills the bound port when advertise omits the port", async (t) => {
  const node = new Node({ cluster: "advertise-port-omitted" });
  const { port1, port2 } = new MessageChannel();

  t.after(() => {
    node.close();
    port1.close();
    port2.close();
  });

  const bound = await node.listen("ws://0.0.0.0:0", {
    advertise: "ws://127.0.0.1/",
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
  port2.postMessage({ t: "hello", from: "ghost", version: 1 });

  assert.deepEqual(await learned, [
    { id: node.nodeId, addr: `ws://127.0.0.1:${bound.port}` },
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
  port2.postMessage({ t: "hello", from: "ghost", version: 1 });

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

  port2.postMessage({ t: "hello", from: "ghost", version: 1 });
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
  port2.postMessage({ t: "hello", from: "peer", version: 1 });
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
  port2.postMessage({ t: "hello", from: "ghost", version: 1 });
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

  await new Promise((resolve) => setTimeout(resolve, 10));

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

  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.equal(internals.dials.has(remote.addr), true);
  assert.equal(internals.expiries.has(remote.addr), true);

  internals.expiries.set(remote.addr, Date.now() - 1);
  internals.learn([], { local: true });

  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.equal(internals.dials.has(remote.addr), false);
  assert.equal(internals.clients.length, 0);
  assert.equal(internals.expiries.has(remote.addr), false);
});

test("updates a member address and its dial", async (t) => {
  const firstPort = await freePort();
  let secondPort = await freePort();

  while (secondPort === firstPort) {
    secondPort = await freePort();
  }

  const node = new Node({ cluster: "members-relocate", memberTtl: 1000 });
  const internals = node as unknown as NodeInternals;
  const remoteId = "ffffffffffffffffffffffffffffffff";

  t.after(() => node.close());

  internals.members.push({ id: node.nodeId, addr: "tcp://127.0.0.1:9" });

  const first = {
    id: remoteId,
    addr: `tcp://127.0.0.1:${firstPort}`,
  };
  const second = {
    id: remoteId,
    addr: `tcp://127.0.0.1:${secondPort}`,
  };
  const firstAddr = first.addr;

  internals.learn([first], { local: true });

  assert.equal(internals.dials.has(firstAddr), true);

  internals.learn([second], { local: true });

  const members = node
    .snapshot()
    .members.filter((member) => member.id === remoteId);

  assert.equal(members.length, 1);
  assert.equal(members[0].addr, first.addr);
  assert.equal(internals.dials.has(firstAddr), true);
  assert.equal(internals.dials.has(second.addr), true);
});

test("expires members without further traffic", async (t) => {
  const port = await freePort();
  const node = new Node({ cluster: "members-timer", memberTtl: 50 });
  const internals = node as unknown as NodeInternals;
  const remote = {
    id: "00000000000000000000000000000000",
    addr: `tcp://127.0.0.1:${port}`,
  };

  t.after(() => node.close());

  internals.members.push({ id: node.nodeId, addr: "tcp://127.0.0.1:9" });
  internals.learn([remote], { local: true });

  assert.equal(internals.dials.size, 0);
  assert.equal(internals.members.length, 2);

  await new Promise((resolve) => setTimeout(resolve, 120));

  assert.equal(internals.members.length, 1);
  assert.equal(
    internals.members.some((member) => member.id === remote.id),
    false,
  );
  assert.equal(internals.seen.has(remote.id), false);
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
  port2.postMessage({ t: "hello", from: "peer", version: 1 });

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

test("closes a listening server when it errors", async (t) => {
  const node = new Node({ cluster: "server-error" });

  t.after(() => node.close());

  await node.listen("tcp://127.0.0.1:0");

  const internals = node as unknown as {
    servers: Array<{
      close(): void;
      listen(): Promise<unknown>;
      events: { emit(event: string, error: Error): void };
    }>;
  };
  const server = internals.servers[0];

  assert.notEqual(server, undefined);

  if (server === undefined) {
    return;
  }

  server.events.emit("error", new Error("boom"));

  assert.equal(internals.servers.length, 0);
  await assert.rejects(server.listen());
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
