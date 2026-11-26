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
import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, createServer } from "node:net";
import { createServer as createHttpServer } from "node:http";
import { WebSocket } from "ws";
import { Node } from "./node.ts";
import { NodeLink } from "./nodelink.ts";
import { LinkPort } from "./linkport.ts";
import { Link } from "./link.ts";
import { protocolVersion } from "./envelope.ts";
import { signToken } from "./token.ts";
import { freePort, freeUdpPort, waitFor, within } from "./testutil.ts";
import { SwarmError } from "./errors.ts";
import { maxEnvelopeSize } from "./codec.ts";
import type { Frame } from "./frame.ts";
import type { CallContext } from "./local.ts";
import type { LinkTransport } from "./link.ts";
import type { NodeLinkDown } from "./node.ts";
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

class FrameLink extends Link {
  readonly local = true;
  readonly frames: Frame[] = [];
  readonly transport: LinkTransport;
  closed = false;

  constructor(nodeId: string, transport: LinkTransport = "port") {
    super(nodeId);

    this.transport = transport;
  }

  send(frame: Frame) {
    this.frames.push(frame);
  }

  close() {
    this.closed = true;
  }
}

function runRelay() {
  return new Promise<void>((resolve) => setImmediate(resolve));
}

function makeNode(callTimeout = 50) {
  return new Node({ cluster: "test", callTimeout });
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
  port2.postMessage({
    t: "hello",
    cluster: node.cluster,
    from: "ghost",
    version: protocolVersion,
    startup: 1,
    token: signToken("", "ghost"),
  });
  port2.postMessage({
    t: "changes",
    items: [{ t: "up", from: "ghost", service }],
  });

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
  test("rejects a peer that announces another cluster", async (t) => {
    const node = new Node({ cluster: "local-cluster" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      node.close();
      port2.close();
    });

    node.attach(port1);
    port2.start();
    port2.postMessage({
      t: "hello",
      cluster: "ghost-cluster",
      from: "ghost",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "ghost"),
    });

    await new Promise((resolve) => setTimeout(resolve, 120));

    assert.equal(node.cluster, "local-cluster");
    assert.equal(
      node.snapshot().links.some((link) => link.peerId === "ghost"),
      false,
    );
  });

  test("rejects a cluster mismatch on the join path", async (t) => {
    const node = new Node({ cluster: "join-cluster" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      node.close();
      port2.close();
    });

    node.join(port1);
    port2.start();
    port2.postMessage({
      t: "hello",
      cluster: "other-cluster",
      from: "ghost",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "ghost"),
    });

    await new Promise((resolve) => setTimeout(resolve, 120));

    assert.equal(node.snapshot().links.length, 0);
  });

  test("rejects a cluster mismatch when dialing", async (t) => {
    const hub = new Node({ cluster: "dial-hub" });
    const peer = new Node({ cluster: "dial-peer" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    const { port } = await hub.listen("tcp://127.0.0.1:0");

    await assert.rejects(peer.connect(`tcp://127.0.0.1:${port}`));
    assert.equal(hub.snapshot().links.length, 0);
  });

  test("authenticates peers by the node secret and by url credentials", async (t) => {
    const hub = new Node({ cluster: "secret-hub", secret: "s3cr3t" });
    const peer = new Node({ cluster: "secret-hub", secret: "s3cr3t" });
    const impostor = new Node({ cluster: "secret-hub", secret: "wrong" });
    const credentialed = new Node({ cluster: "secret-hub", secret: "wrong" });

    t.after(() => {
      hub.close();
      peer.close();
      impostor.close();
      credentialed.close();
    });

    hub.register("math", { add: async (_ctx, a: number, b: number) => a + b });

    const { port } = await hub.listen("tcp://127.0.0.1:0");

    await assert.rejects(impostor.connect(`tcp://127.0.0.1:${port}`));

    await peer.connect(`tcp://127.0.0.1:${port}`);

    await waitFor(() =>
      peer.snapshot().services.some((instance) => instance.service === "math"),
    );

    assert.equal(await peer.call("math", "add", [2, 3]), 5);

    await credentialed.connect(`tcp://ig:s3cr3t@127.0.0.1:${port}`);

    assert.equal(credentialed.snapshot().links.length, 1);
  });

  test("snapshot reports in-flight calls and per-link counters", async (t) => {
    const { port1, port2 } = new MessageChannel();
    const a = new Node({ cluster: "health", callTimeout: 5000 });
    const b = new Node({ cluster: "health", callTimeout: 5000 });
    const gate = Promise.withResolvers<void>();

    t.after(() => {
      a.close();
      b.close();
    });

    b.register("slow", { wait: () => gate.promise });

    a.attach(port1);
    b.attach(port2);

    assert.equal(
      await waitFor(() => a.snapshot().links[0]?.services.length === 1),
      true,
    );

    const call = a.call("slow", "wait", []);

    assert.equal(
      await waitFor(() => b.snapshot().links[0]?.inbound === 1),
      true,
    );
    assert.equal(a.snapshot().health.inflight, 1);
    assert.equal(a.snapshot().links[0]?.pending, 1);
    assert.equal(b.snapshot().health.inflight, 1);

    gate.resolve();

    assert.equal(await call, undefined);
    assert.equal(
      await waitFor(() => a.snapshot().links[0]?.pending === 0),
      true,
    );
    assert.equal(a.snapshot().health.inflight, 0);
    assert.equal(b.snapshot().health.inflight, 0);
  });

  test("rejects inbound calls above maxInbound", async (t) => {
    const { port1, port2 } = new MessageChannel();
    const a = new Node({ cluster: "inbound", callTimeout: 5000 });
    const b = new Node({
      cluster: "inbound",
      callTimeout: 5000,
      maxInbound: 1,
    });
    const gate = Promise.withResolvers<void>();

    t.after(() => {
      a.close();
      b.close();
    });

    b.register("slow", { wait: () => gate.promise });

    a.attach(port1);
    b.attach(port2);

    assert.equal(
      await waitFor(() => a.snapshot().links[0]?.services.length === 1),
      true,
    );

    const first = a.call("slow", "wait", []);

    assert.equal(
      await waitFor(() => b.snapshot().links[0]?.inbound === 1),
      true,
    );

    await assert.rejects(
      a.call("slow", "wait", []),
      (err: SwarmError) => err.code === "CONGESTED" && err.retriable === true,
    );

    gate.resolve();

    assert.equal(await first, undefined);
    assert.equal(
      await waitFor(() => b.snapshot().links[0]?.inbound === 0),
      true,
    );
  });

  test("snapshot counts dropped frames by code", async (t) => {
    const node = new Node({ cluster: "malformed" });
    const { port } = await node.listen("tcp://127.0.0.1:0");
    const socket = connect(port, "127.0.0.1");

    t.after(() => {
      socket.destroy();
      node.close();
    });

    await new Promise<void>((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });

    socket.write(Buffer.from([0, 0, 0, 1, 0xc1]));

    assert.equal(
      await waitFor(
        () => node.snapshot().health.drops.FRAME_DROPPED !== undefined,
      ),
      true,
    );
    assert.equal(node.snapshot().health.drops.FRAME_DROPPED, 1);
  });

  test("local calls hand each handler its own cancel signal", async () => {
    const node = makeNode(5000);
    const signals: AbortSignal[] = [];

    node.register("probe", {
      take: (ctx) => {
        signals.push(ctx.signal);
        return signals.length;
      },
    });

    await node.call("probe", "take", []);
    await node.call("probe", "take", []);

    assert.notEqual(signals[0], signals[1]);
    assert.equal(signals[0]?.aborted, false);
  });

  test("local calls reject with cancelled when the signal aborts", async (t) => {
    const node = makeNode(5000);
    const controller = new AbortController();

    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });

    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });

    let seen: AbortSignal | undefined;

    node.register("slow", {
      work: async (ctx) => {
        seen = ctx.signal;
        entered();
        await gate;
        return "done";
      },
    });

    t.after(() => {
      release();
      node.close();
    });

    const call = node.call("slow", "work", [], {
      signal: controller.signal,
      timeout: 5000,
    });

    await started;
    controller.abort();

    await assert.rejects(call, (err: SwarmError) => err.code === "CANCELLED");
    assert.equal(seen?.aborted, true);
  });

  test("register and unregister return the node", () => {
    const node = makeNode();

    assert.equal(node.register("math", { add: () => 0 }), node);
    assert.equal(node.unregister("math"), node);
  });

  test("secret defaults to an empty string", (t) => {
    const node = makeNode();
    const secured = new Node({ cluster: "test", secret: "s3cret" });

    t.after(() => {
      node.close();
      secured.close();
    });

    assert.equal(node.secret, "");
    assert.equal(secured.secret, "s3cret");
  });

  test("call passes an empty argument list by default", async () => {
    const node = makeNode();
    let received: unknown[] | undefined;

    node.register("echo", {
      count: (_ctx, ...args: unknown[]) => {
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

  test("call rejects a timeout that is not a positive finite number", async (t) => {
    const node = makeNode();

    t.after(() => node.close());

    node.register("math", { add: (_ctx, a: number, b: number) => a + b });

    const rejected = (err: SwarmError) =>
      err.kind === "rejected" && err.code === "INVALID_TIMEOUT";

    for (const timeout of [Infinity, NaN, 0, -1]) {
      await assert.rejects(
        node.call("math", "add", [1, 2], { timeout }),
        rejected,
      );
      await assert.rejects(
        node.callTo(node.nodeId, "math", "add", [1, 2], { timeout }),
        rejected,
      );
      await assert.rejects(
        node.callAll("math", "add", [1, 2], { timeout }),
        rejected,
      );
    }

    assert.equal(await node.call("math", "add", [1, 2]), 3);
  });

  test("call rejects args that are not an array", async (t) => {
    const node = makeNode();

    t.after(() => node.close());

    let calls = 0;

    node.register("math", {
      add: (_ctx, ...values: number[]) => {
        calls += 1;
        return values.reduce((sum, value) => sum + value, 0);
      },
    });

    const rejected = (err: SwarmError) =>
      err.kind === "rejected" && err.code === "REJECTED";

    const malformed = "oops" as unknown as number[];

    await assert.rejects(node.call("math", "add", malformed), rejected);
    await assert.rejects(
      node.callTo(node.nodeId, "math", "add", malformed),
      rejected,
    );
    await assert.rejects(node.callAll("math", "add", malformed), rejected);

    assert.equal(calls, 0);
    assert.equal(await node.call("math", "add", [1, 2]), 3);
  });

  test("call rejects a service or method that is not a string", async (t) => {
    const node = makeNode();

    t.after(() => node.close());

    let calls = 0;

    node.register("math", {
      add: () => {
        calls += 1;
        return 0;
      },
    });

    const rejected = (err: SwarmError) =>
      err.kind === "rejected" && err.code === "REJECTED";

    const bad = 7 as unknown as string;

    await assert.rejects(node.call(bad, "add", []), rejected);
    await assert.rejects(node.call("math", bad, []), rejected);
    await assert.rejects(node.callTo(node.nodeId, bad, "add", []), rejected);
    await assert.rejects(node.callTo(node.nodeId, "math", bad, []), rejected);
    await assert.rejects(node.callAll(bad, "add", []), rejected);
    await assert.rejects(node.callAll("math", bad, []), rejected);

    assert.equal(calls, 0);
  });

  test("does not send traffic to a link that has not authenticated", async (t) => {
    const node = new Node({ cluster: "unauthenticated", secret: "s3cr3t" });
    const { port } = await node.listen("tcp://127.0.0.1:0");
    const socket = connect(port, "127.0.0.1");
    const chunks: Buffer[] = [];

    socket.on("data", (chunk: Buffer) => chunks.push(chunk));

    t.after(() => {
      socket.destroy();
      node.close();
    });

    await new Promise<void>((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });

    node.register("billing", { charge: () => 1 }, { apiKey: "sk-live-999" });
    node.publish("orders", { orderId: 42 });

    await new Promise((resolve) => setTimeout(resolve, 150));

    assert.equal(Buffer.concat(chunks).length, 0);
  });

  test("does not publish to an outbound link that has not authenticated", async (t) => {
    const node = new Node({ cluster: "unauthenticated", secret: "s3cr3t" });
    const chunks: Buffer[] = [];
    const dropped: SwarmError[] = [];
    const server = createServer((socket) =>
      socket.on("data", (chunk: Buffer) => chunks.push(chunk)),
    );
    const port = await freePort();

    node.events.on("error", (error) => dropped.push(error as SwarmError));

    await new Promise<void>((resolve) =>
      server.listen(port, "127.0.0.1", resolve),
    );

    t.after(() => {
      node.close();
      server.close();
    });

    node.addSeed(`tcp://127.0.0.1:${port}`);
    await waitFor(() => chunks.length > 0);

    const before = Buffer.concat(chunks).length;

    node.publish("orders", { orderId: 42 });

    await new Promise((resolve) => setTimeout(resolve, 150));

    assert.equal(Buffer.concat(chunks).length, before);
    assert.ok(
      dropped.some((error) => error.code === "PUB_DROPPED"),
      `dropped ${dropped.map((error) => error.code).join(",")}`,
    );
  });

  test("does not say goodbye to a link that has not authenticated", async (t) => {
    const node = new Node({ cluster: "unauthenticated", secret: "s3cr3t" });
    const { port } = await node.listen("tcp://127.0.0.1:0");
    const socket = connect(port, "127.0.0.1");
    const chunks: Buffer[] = [];

    socket.on("data", (chunk: Buffer) => chunks.push(chunk));

    t.after(() => socket.destroy());

    await new Promise<void>((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });

    await new Promise((resolve) => setTimeout(resolve, 100));

    node.close();

    await new Promise((resolve) => setTimeout(resolve, 150));

    assert.equal(Buffer.concat(chunks).length, 0);
  });

  test("reports a frame that fails to decode", async (t) => {
    const node = new Node({ cluster: "malformed" });
    const { port } = await node.listen("tcp://127.0.0.1:0");
    const socket = connect(port, "127.0.0.1");
    const dropped: SwarmError[] = [];

    node.events.on("error", (error) => dropped.push(error as SwarmError));

    t.after(() => {
      socket.destroy();
      node.close();
    });

    await new Promise<void>((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });

    const packet = Buffer.alloc(5);

    packet.writeUInt32BE(1, 0);
    packet[4] = 0xc1;
    socket.write(packet);

    await waitFor(
      () => dropped.some((error) => error.code === "FRAME_DROPPED"),
      1000,
    );
  });

  test("a node rejects an invalid default call timeout", () => {
    for (const callTimeout of [Infinity, NaN, 0, -1]) {
      assert.throws(
        () => new Node({ cluster: "test", callTimeout }),
        (err: SwarmError) =>
          err.kind === "rejected" && err.code === "INVALID_TIMEOUT",
      );
    }
  });

  test("a closed node reports the CLOSED code", async () => {
    const node = makeNode();

    node.close();

    await assert.rejects(
      node.call("math", "add"),
      (err: SwarmError) => err.code === "CLOSED",
    );
  });

  test("a throwing dispose does not abort the shutdown", () => {
    const node = makeNode();
    const errors: unknown[] = [];
    let disposed = 0;

    node.events.on("error", (error) => errors.push(error));

    node.register("bad", {
      [Symbol.dispose]: () => {
        throw new Error("boom");
      },
    });
    node.register("good", {
      [Symbol.dispose]: () => {
        disposed += 1;
      },
    });

    node.close();

    assert.equal(disposed, 1);
    assert.equal(errors.length, 1);
  });

  test("a throwing event handler crashes the process", () => {
    const script = `
import { Node } from "./node.ts";
import { protocolVersion } from "./envelope.ts";
import { signToken } from "./token.ts";

const node = new Node({ cluster: "test" });
const { port1, port2 } = new MessageChannel();

node.events.on("link-up", () => {
  throw new Error("boom-node");
});

node.attach(port1);
port2.start();
port2.postMessage({
  t: "hello",
  cluster: "test",
  from: "ghost",
  version: protocolVersion,
  startup: 1,
  token: signToken("", "ghost"),
});

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
    assert.match(result.stderr, /boom-node/);
    assert.doesNotMatch(result.stdout, /survived/);
  });

  test("reports a throwing topic subscriber through the error event", (t) => {
    const node = makeNode();
    const errors: Error[] = [];
    let delivered = 0;

    t.after(() => node.close());

    node.events.on("error", (error) => errors.push(error));
    node.subscribe("topic", () => {
      throw new Error("boom");
    });
    node.subscribe("topic", () => {
      delivered += 1;
    });

    node.publish("topic", {});

    assert.equal(errors.length, 1);
    assert.equal(errors[0].message, "boom");
    assert.equal(delivered, 1);
  });

  test("a throwing topic subscriber crashes the process when nobody listens", () => {
    const script = `
import { Node } from "./node.ts";

const node = new Node({ cluster: "test" });

node.subscribe("topic", () => {
  throw new Error("boom-topic");
});

node.publish("topic", {});

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
    assert.match(result.stderr, /boom-topic/);
    assert.doesNotMatch(result.stdout, /survived/);
  });

  test("register rejects metadata that cannot be encoded", async () => {
    const node = makeNode();

    const cyclic: MetaData = { build: 1 };
    const values: MetaData[] = [
      { build: () => {} },
      { tags: new Set(["a"]) },
      Object.assign(cyclic, { self: cyclic }),
    ];

    for (const meta of values) {
      assert.throws(
        () => node.register("math", { add: () => 0 }, meta),
        (err: SwarmError) =>
          err.kind === "rejected" &&
          err.message === "meta is not encodable" &&
          err.code === "CLONE_FAILED",
      );
    }

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
    const meta = { build: 1, nested: { flags: ["a"] } };

    node.register("math", { add: () => 0 }, meta);
    meta.build = 2;
    meta.nested.flags.push("b");

    let stored: unknown;

    node.watch("math", (event) => {
      stored = event.instance.meta;
    });

    assert.deepEqual(stored, { build: 1, nested: { flags: ["a"] } });
  });

  test("re-registering updates the service metadata", async (t) => {
    const a = new Node({ cluster: "meta-a" });
    const b = new Node({ cluster: "meta-a" });
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
    const b = new Node({ cluster: "echo-tri-a" });
    const c = new Node({ cluster: "echo-tri-a" });
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
          | { build?: number }
          | undefined;

        return meta?.build === 2;
      }),
      true,
    );

    a.unregister("math");

    assert.equal(await waitFor(() => a.snapshot().services.length === 0), true);
    assert.equal(await waitFor(() => b.snapshot().services.length === 0), true);
    assert.equal(await waitFor(() => c.snapshot().services.length === 0), true);
  });

  test("unregister converges across a diamond where both paths relay the owner", async (t) => {
    const a = new Node({ cluster: "echo-diamond-a" });
    const b = new Node({ cluster: "echo-diamond-a" });
    const c = new Node({ cluster: "echo-diamond-a" });
    const d = new Node({ cluster: "echo-diamond-a" });
    const nodes = [a, b, c, d];

    t.after(() => {
      for (const node of nodes) {
        node.close();
      }
    });

    for (const [left, right] of [
      [a, b],
      [a, c],
      [b, d],
      [c, d],
    ]) {
      const channel = new MessageChannel();

      left.attach(channel.port1);
      right.attach(channel.port2);
    }

    d.register("math", { add: () => 1 });

    assert.equal(
      await waitFor(() =>
        nodes.every((node) => node.snapshot().services.length === 1),
      ),
      true,
    );

    d.unregister("math");

    assert.equal(
      await waitFor(
        () => nodes.every((node) => node.snapshot().services.length === 0),
        1000,
      ),
      true,
    );
  });

  test("encodes a broadcast once for every link", async (t) => {
    const node = new Node({ cluster: "frame-share" });
    const internals = node as unknown as {
      attachLink: (link: Link) => void;
      broadcastPeers: (ups: unknown[], downs: string[]) => void;
      flushPending: () => void;
    };
    const links = ["peer-a", "peer-b", "peer-c"].map((peerId) => {
      const link = new FrameLink(node.nodeId);

      internals.attachLink(link);
      link.events.emit("envelope", {
        t: "hello",
        cluster: "frame-share",
        from: peerId,
        version: protocolVersion,
        startup: 1,
        token: signToken("", peerId),
      });

      return link;
    });

    t.after(() => node.close());

    node.register("math", { add: () => 1 });
    await runRelay();

    const updates = links.map((link) => link.frames.at(-1));
    const changes = updates[0];

    assert.equal(changes?.message.t, "changes");
    assert.equal(updates[1], changes);
    assert.equal(updates[2], changes);
    assert.equal(updates[1]?.bytes(), changes?.bytes());
    assert.equal(updates[2]?.bytes(), changes?.bytes());

    internals.broadcastPeers(
      [{ id: "peer-a", addr: "tcp://127.0.0.1:1", startup: 1 }],
      [],
    );
    internals.flushPending();

    const peers = links.map((link) => link.frames.at(-1));

    assert.equal(peers[0]?.message.t, "peers");
    assert.equal(peers[1], peers[0]);
    assert.equal(peers[2], peers[0]);
    assert.equal(peers[1]?.bytes(), peers[0]?.bytes());
    assert.equal(peers[2]?.bytes(), peers[0]?.bytes());
  });

  test("coalesces peer announcements inside the gossip window", async (t) => {
    const node = new Node({ cluster: "gossip-coalesce" });
    const internals = node as unknown as {
      attachLink: (link: Link) => void;
      membership: {
        upsertSelf: (member: {
          id: string;
          addr: string;
          startup: number;
          passive?: false | "leaf" | "ghost";
        }) => void;
      };
    };

    t.after(() => node.close());

    const link = new FrameLink(node.nodeId, "socket");
    internals.attachLink(link);
    link.events.emit("envelope", {
      t: "hello",
      cluster: "gossip-coalesce",
      from: "peer-a",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-a"),
    });

    link.frames.length = 0;

    for (let i = 0; i < 5; i++) {
      internals.membership.upsertSelf({
        id: node.nodeId,
        addr: `tcp://127.0.0.1:${i + 1}`,
        startup: 1,
      });
    }

    assert.equal(link.frames.length, 0);

    await new Promise((resolve) => setTimeout(resolve, 120));

    assert.equal(link.frames.length, 1);
    assert.equal(link.frames[0].message.t, "peers");
  });

  test("announces a member change as a delta", async (t) => {
    const node = new Node({ cluster: "peer-delta" });
    const internals = node as unknown as {
      attachLink: (link: Link) => void;
      membership: {
        learn: (members: unknown[], local: boolean) => void;
        leave: (id: string) => void;
      };
    };

    t.after(() => node.close());

    const link = new FrameLink(node.nodeId, "socket");
    internals.attachLink(link);
    link.events.emit("envelope", {
      t: "hello",
      cluster: "peer-delta",
      from: "peer-a",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-a"),
    });

    internals.membership.learn(
      [
        { id: "peer-x", addr: "tcp://10.0.0.9:1", startup: 1 },
        { id: "peer-y", addr: "tcp://10.0.0.9:2", startup: 1 },
      ],
      false,
    );

    await new Promise((resolve) => setTimeout(resolve, 120));

    link.frames.length = 0;
    internals.membership.learn(
      [{ id: "peer-z", addr: "tcp://10.0.0.9:3", startup: 1 }],
      false,
    );

    await new Promise((resolve) => setTimeout(resolve, 120));

    const added = link.frames.at(-1);

    assert.equal(added?.message.t, "peers");
    assert.deepEqual(
      added?.message.t === "peers" ? added.message.members : undefined,
      [{ id: "peer-z", addr: "tcp://10.0.0.9:3", startup: 1 }],
    );

    link.frames.length = 0;
    internals.membership.leave("peer-x");

    await new Promise((resolve) => setTimeout(resolve, 120));

    const removed = link.frames.at(-1);

    assert.equal(removed?.message.t, "peersGone");
    assert.deepEqual(
      removed?.message.t === "peersGone" ? removed.message.ids : undefined,
      ["peer-x"],
    );
  });

  test("removes members announced as departed", async (t) => {
    const node = new Node({ cluster: "peer-departed" });
    const internals = node as unknown as {
      attachLink: (link: Link) => void;
      membership: { learn: (members: unknown[], local: boolean) => void };
    };

    t.after(() => node.close());

    const link = new FrameLink(node.nodeId, "socket");
    internals.attachLink(link);
    link.events.emit("envelope", {
      t: "hello",
      cluster: "peer-departed",
      from: "peer-a",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-a"),
    });

    internals.membership.learn(
      [{ id: "peer-x", addr: "tcp://10.0.0.9:1", startup: 1 }],
      false,
    );

    const downs: string[] = [];
    node.events.on("member-down", (member) => downs.push(member.id));

    link.events.emit("envelope", {
      t: "peersGone",
      from: "peer-a",
      ids: ["peer-x"],
    });

    assert.deepEqual(downs, ["peer-x"]);
    assert.equal(
      node.snapshot().members.some((member) => member.id === "peer-x"),
      false,
    );
  });

  test("ignores peersGone that names the node itself", async (t) => {
    const node = new Node({ cluster: "peer-self" });
    const internals = node as unknown as {
      attachLink: (link: Link) => void;
      membership: {
        upsertSelf: (member: unknown) => void;
        self: () => unknown[];
        departed: Map<string, number>;
      };
    };

    t.after(() => node.close());

    const link = new FrameLink(node.nodeId, "socket");
    internals.attachLink(link);
    link.events.emit("envelope", {
      t: "hello",
      cluster: "peer-self",
      from: "peer-a",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-a"),
    });

    internals.membership.upsertSelf({
      id: node.nodeId,
      addr: "tcp://127.0.0.1:7001",
      startup: 1,
    });

    link.events.emit("envelope", {
      t: "peersGone",
      from: "peer-a",
      ids: [node.nodeId],
    });

    assert.equal(internals.membership.self().length, 1);
    assert.equal(internals.membership.departed.has(node.nodeId), false);
  });

  test("ignores peersGone for a node it is still linked to", async (t) => {
    const node = new Node({ cluster: "peer-linked" });
    const internals = node as unknown as {
      attachLink: (link: Link) => void;
      membership: { learn: (members: unknown[], local: boolean) => void };
    };

    t.after(() => node.close());

    const link = new FrameLink(node.nodeId, "socket");
    internals.attachLink(link);
    link.events.emit("envelope", {
      t: "hello",
      cluster: "peer-linked",
      from: "peer-a",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-a"),
    });

    internals.membership.learn(
      [{ id: "peer-a", addr: "tcp://10.0.0.9:1", startup: 1 }],
      false,
    );

    link.events.emit("envelope", {
      t: "peersGone",
      from: "peer-a",
      ids: ["peer-a"],
    });

    assert.equal(
      node.snapshot().members.some((member) => member.id === "peer-a"),
      true,
    );
  });

  test("lets a member rejoin once the tombstone expires", async (t) => {
    const node = new Node({ cluster: "tombstone", memberTtl: 40 });
    const internals = node as unknown as {
      attachLink: (link: Link) => void;
      membership: { learn: (members: unknown[], local: boolean) => void };
    };

    t.after(() => node.close());

    const link = new FrameLink(node.nodeId, "socket");
    internals.attachLink(link);
    link.events.emit("envelope", {
      t: "hello",
      cluster: "tombstone",
      from: "peer-a",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-a"),
    });

    const peerX = [{ id: "peer-x", addr: "tcp://10.0.0.9:1", startup: 1 }];
    const alive = () =>
      node.snapshot().members.some((member) => member.id === "peer-x");

    internals.membership.learn(peerX, false);
    assert.equal(alive(), true);

    link.events.emit("envelope", {
      t: "peersGone",
      from: "peer-a",
      ids: ["peer-x"],
    });
    assert.equal(alive(), false);

    internals.membership.learn(peerX, false);
    assert.equal(alive(), false);

    await new Promise((resolve) => setTimeout(resolve, 120));

    internals.membership.learn(peerX, false);
    assert.equal(alive(), true);
  });

  test("splits peer announcements that would exceed the frame limit", async (t) => {
    const node = new Node({ cluster: "peer-split" });
    const internals = node as unknown as {
      attachLink: (link: Link) => void;
      membership: {
        upsertSelf: (member: {
          id: string;
          addr: string;
          startup: number;
          passive?: false | "leaf" | "ghost";
        }) => void;
      };
      flushPeers: (ups: undefined, downs: string[]) => void;
    };

    t.after(() => node.close());

    const portLink = new FrameLink(node.nodeId, "port");
    const socketLink = new FrameLink(node.nodeId, "socket");
    internals.attachLink(portLink);
    internals.attachLink(socketLink);
    for (const [index, link] of [portLink, socketLink].entries()) {
      link.events.emit("envelope", {
        t: "hello",
        cluster: "peer-split",
        from: `peer-${index}`,
        version: protocolVersion,
        startup: 1,
        token: signToken("", `peer-${index}`),
      });
    }

    internals.membership.upsertSelf({
      id: node.nodeId,
      addr: "tcp://127.0.0.1:1",
      startup: 1,
    });

    const members = Array.from({ length: 20 }, (_, i) => ({
      id: `peer-${i}`,
      addr: `tcp://10.0.0.1:${1000 + i}?${"x".repeat(450000)}`,
      startup: 1,
    }));
    const internals2 = node as unknown as {
      membership: { members: Map<string, unknown> };
    };
    for (const member of members) {
      internals2.membership.members.set(member.id, member);
    }
    portLink.frames.length = 0;
    socketLink.frames.length = 0;
    internals.flushPeers(undefined, []);

    for (const link of [portLink, socketLink]) {
      let count = 0;

      for (const frame of link.frames) {
        assert.equal(frame.message.t, "peers");
        assert.ok(frame.bytes().byteLength <= 8 * 1024 * 1024);
        if (frame.message.t === "peers") {
          count += frame.message.members.length;
        }
      }

      assert.equal(count, 21);
    }
  });

  test("splits relayed changes that would exceed the frame limit", async (t) => {
    const node = new Node({ cluster: "frame-split" });

    t.after(() => node.close());

    await node.listen("tcp://127.0.0.1:0");

    const internals = node as unknown as { attachLink: (link: Link) => void };
    const link = new FrameLink(node.nodeId, "socket");

    internals.attachLink(link);
    link.events.emit("envelope", {
      t: "hello",
      cluster: "frame-split",
      from: "peer-a",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer-a"),
    });

    for (let i = 0; i < 4; i += 1) {
      node.register(
        `svc-${i}`,
        { run: () => 0 },
        { blob: "x".repeat(3 * 1024 * 1024) },
      );
    }

    await runRelay();

    const frames = link.frames.filter((frame) => frame.message.t === "changes");
    const items = frames.flatMap((frame) =>
      frame.message.t === "changes" ? frame.message.items : [],
    );

    assert.ok(frames.length > 1);
    assert.equal(items.length, 4);

    for (const frame of frames) {
      assert.ok(frame.bytes().byteLength <= maxEnvelopeSize);
    }
  });

  test("does not echo a down frame back to its source", async (t) => {
    const node = new Node({ cluster: "frame-echo" });
    const internals = node as unknown as { attachLink: (link: Link) => void };
    const links = ["peer-a", "peer-b", "peer-c"].map((peerId) => {
      const link = new FrameLink(node.nodeId);

      internals.attachLink(link);
      link.events.emit("envelope", {
        t: "hello",
        cluster: "frame-echo",
        from: peerId,
        version: protocolVersion,
        startup: 1,
        token: signToken("", peerId),
      });

      return link;
    });

    t.after(() => node.close());

    links[0].events.emit("envelope", {
      t: "changes",
      items: [{ t: "up", from: "peer-a", service: "math" }],
    });
    links[0].events.emit("envelope", {
      t: "changes",
      items: [{ t: "down", from: "peer-a", service: "math" }],
    });

    await runRelay();

    const downs = (link: FrameLink) =>
      link.frames
        .flatMap((frame) =>
          frame.message.t === "changes" ? frame.message.items : [],
        )
        .filter((message) => message.t === "down").length;

    assert.deepEqual(links.map(downs), [0, 1, 1]);
  });

  test("coalesces a burst of relayed updates into one frame per link", async (t) => {
    const node = new Node({ cluster: "frame-batch" });
    const internals = node as unknown as { attachLink: (link: Link) => void };
    const links = ["edge-a", "edge-b", "edge-c"].map((peerId) => {
      const link = new FrameLink(node.nodeId);

      internals.attachLink(link);
      link.events.emit("envelope", {
        t: "hello",
        cluster: "frame-batch",
        from: peerId,
        version: protocolVersion,
        startup: 1,
        token: signToken("", peerId),
      });

      return link;
    });

    t.after(() => node.close());

    const relayed = (link: FrameLink) =>
      link.frames.filter((frame) => frame.message.t === "changes");

    for (let i = 0; i < 100; i += 1) {
      links[0].events.emit("envelope", {
        t: "changes",
        items: [{ t: "up", from: "edge-a", service: `svc-${i}` }],
      });
    }

    await runRelay();

    assert.equal(relayed(links[0]).length, 0);
    assert.deepEqual(
      links.slice(1).map((link) => relayed(link).length),
      [1, 1],
    );

    const batch = relayed(links[1])[0].message;

    assert.ok(batch.t === "changes");
    assert.equal(batch.items.length, 100);

    for (let i = 0; i < 100; i += 1) {
      links[0].events.emit("envelope", {
        t: "changes",
        items: [{ t: "down", from: "edge-a", service: `svc-${i}` }],
      });
    }

    await runRelay();

    const downs = relayed(links[2])[1].message;

    assert.ok(downs.t === "changes");
    assert.equal(downs.items.length, 100);
    assert.equal(
      downs.items.every((item) => item.t === "down"),
      true,
    );
  });

  test("a broken path keeps an instance reachable through another peer", async (t) => {
    const a = new Node({ cluster: "echo-path-a" });
    const b = new Node({ cluster: "echo-path-a" });
    const c = new Node({ cluster: "echo-path-a" });
    const d = new Node({ cluster: "echo-path-a" });
    const nodes = [a, b, c, d];

    t.after(() => {
      for (const node of nodes) {
        node.close();
      }
    });

    const plain = [
      [a, b],
      [a, c],
      [c, d],
    ];

    for (const [left, right] of plain) {
      const channel = new MessageChannel();

      left.attach(channel.port1);
      right.attach(channel.port2);
    }

    const bd = new MessageChannel();

    b.attach(bd.port1);
    d.attach(bd.port2);

    d.register("math", { add: () => 1 });

    assert.equal(
      await waitFor(() =>
        nodes.every((node) => node.snapshot().services.length === 1),
      ),
      true,
    );

    bd.port1.close();
    bd.port2.close();

    assert.equal(await waitFor(() => b.snapshot().links.length === 1), true);

    assert.equal(a.snapshot().services.length, 1);
    assert.equal(await a.call("math", "add"), 1);
  });

  test("unregister propagates across duplicate links to the same peer", async (t) => {
    const a = new Node({ cluster: "echo-dup-a" });
    const b = new Node({ cluster: "echo-dup-a" });
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

  test("routes calls to a local service before a link", async (t) => {
    const node = makeNode(20);
    const ghost = await attachGhost(node, "math");
    t.after(ghost.close);

    node.register("math", { add: () => 10 });

    assert.equal(await node.call("math", "add"), 10);
  });

  test("get returns a proxy that calls the service", async () => {
    type MathApi = { add(ctx: CallContext, a: number, b: number): number };

    const node = makeNode();
    node.register("math", { add: (_ctx, a: number, b: number) => a + b });
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

  test("close waits for in-flight calls when draining", async (t) => {
    const port = await freePort();
    const url = `tcp://127.0.0.1:${port}`;
    const hub = new Node({ cluster: "drain" });
    const peer = new Node({ cluster: "drain" });

    await hub.listen(url);

    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });

    hub.register("slow", {
      wait: async () => {
        entered();
        await new Promise((resolve) => setTimeout(resolve, 100));
        return "done";
      },
    });

    await peer.connect(url);
    t.after(() => peer.close());

    await waitFor(() =>
      peer.snapshot().services.some((instance) => instance.service === "slow"),
    );

    const call = peer.call("slow", "wait");

    await started;

    const closing = hub.close({ drain: 2000 });

    assert.throws(
      () => hub.register("other", { ping: () => 1 }),
      (err: SwarmError) => err.code === "CLOSED",
    );

    await closing;

    assert.equal(await call, "done");
    assert.equal(hub.isClosed, true);
  });

  test("close stops waiting when the drain window expires", async (t) => {
    const port = await freePort();
    const url = `tcp://127.0.0.1:${port}`;
    const hub = new Node({ cluster: "drain-timeout" });
    const peer = new Node({ cluster: "drain-timeout" });

    await hub.listen(url);

    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });

    hub.register("slow", {
      wait: async () => {
        entered();
        await new Promise((resolve) => setTimeout(resolve, 300));
        return "done";
      },
    });

    await peer.connect(url);
    t.after(() => peer.close());

    await waitFor(() =>
      peer.snapshot().services.some((instance) => instance.service === "slow"),
    );

    const call = peer.call("slow", "wait");

    await started;

    const begun = Date.now();

    await hub.close({ drain: 20 });

    assert.ok(Date.now() - begun < 250);
    await assert.rejects(call, (err: SwarmError) => err.code === "CLOSED");
  });

  test("close with a zero drain window closes at once", async (t) => {
    const port = await freePort();
    const url = `tcp://127.0.0.1:${port}`;
    const hub = new Node({ cluster: "drain-zero" });
    const peer = new Node({ cluster: "drain-zero" });

    await hub.listen(url);

    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });

    hub.register("slow", {
      wait: async () => {
        entered();
        await new Promise((resolve) => setTimeout(resolve, 300));
        return "done";
      },
    });

    await peer.connect(url);
    t.after(() => peer.close());

    await waitFor(() =>
      peer.snapshot().services.some((instance) => instance.service === "slow"),
    );

    const call = peer.call("slow", "wait");

    await started;

    const begun = Date.now();

    await hub.close({ drain: 0 });

    assert.ok(Date.now() - begun < 100);
    await assert.rejects(call, (err: SwarmError) => err.code === "CLOSED");
  });

  test("draining aborts in-flight handlers", async (t) => {
    const port = await freePort();
    const url = `tcp://127.0.0.1:${port}`;
    const hub = new Node({ cluster: "drain-abort" });
    const peer = new Node({ cluster: "drain-abort" });

    await hub.listen(url);

    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });

    hub.register("slow", {
      wait: (ctx) =>
        new Promise((resolve) => {
          entered();
          ctx.signal.addEventListener("abort", () => resolve("aborted"), {
            once: true,
          });
        }),
    });

    await peer.connect(url);
    t.after(() => peer.close());

    await waitFor(() =>
      peer.snapshot().services.some((instance) => instance.service === "slow"),
    );

    const call = peer.call("slow", "wait");

    await started;

    const begun = Date.now();

    await hub.close({ drain: 2000 });

    assert.ok(
      Date.now() - begun < 1000,
      "drain should not wait for the window",
    );
    assert.equal(await call, "aborted");
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

  test("snapshot reports empty after close", () => {
    const node = makeNode();
    node.register("math", { add: () => 0 });

    assert.equal(node.snapshot().services.length, 1);

    node.close();

    const snap = node.snapshot();
    assert.deepEqual(snap.services, []);
    assert.deepEqual(snap.members, []);
    assert.deepEqual(snap.links, []);
  });

  test("snapshot exposes seeds and discovery state", (t) => {
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
    assert.equal(snap.unix, false);
    assert.equal(snap.startup, node.startup);
  });

  test("snapshot reports connected links", async (t) => {
    const hub = new Node({ cluster: "obs-link-info" });
    const peer = new Node({ cluster: "obs-link-info" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    const address = await hub.listen("tcp://127.0.0.1:0");
    await peer.connect(`tcp://127.0.0.1:${address.port}`);

    await waitFor(() => hub.snapshot().links.length === 1);

    const link = hub.snapshot().links[0];

    assert.equal(link.peerId, peer.nodeId);
    assert.equal(link.transport, "socket");
    assert.deepEqual(link.services, []);
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
    port2.postMessage({
      t: "hello",
      cluster: "obs-link-events",
      from: "ghost",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "ghost"),
    });

    await waitFor(() => ups.length === 1);

    port2.close();

    await waitFor(() => downs.length === 1);

    assert.equal((downs[0] as NodeLinkDown).reason, "error");
    assert.equal((downs[0] as NodeLinkDown).code, "CLOSED");
    assert.equal((downs[0] as NodeLinkDown).message, "link is closed");
  });

  test("emits link-down with a bye reason when the peer closes", async (t) => {
    const hub = new Node({ cluster: "bye-reason" });
    const peer = new Node({ cluster: "bye-reason" });
    const port = await freePort();

    t.after(() => {
      hub.close();
      peer.close();
    });

    await hub.listen(`tcp://127.0.0.1:${port}`);

    const down = new Promise<NodeLinkDown>((resolve) =>
      hub.events.on("link-down", resolve),
    );

    await peer.connect(`tcp://127.0.0.1:${port}`);

    assert.equal(await waitFor(() => hub.snapshot().links.length === 1), true);

    peer.close();

    const info = await within(down, 2000);

    assert.equal(info?.reason, "bye");
    assert.equal(info?.message, "peer said goodbye");
  });

  test("closes an outbound link with a peer id mismatch message", async (t) => {
    const node = new Node({ cluster: "peer-id-mismatch" });
    const { port1, port2 } = new MessageChannel();
    const internals = node as unknown as {
      attachLink: (link: LinkPort, secret?: string, expected?: string) => void;
      nodeLinks: NodeLink[];
    };

    t.after(() => {
      node.close();
      port2.close();
    });

    internals.attachLink(
      new LinkPort(port1, node.nodeId),
      undefined,
      "expected-peer",
    );

    const link = internals.nodeLinks[0];

    port2.start();
    port2.postMessage({
      t: "hello",
      cluster: "peer-id-mismatch",
      from: "other-peer",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "other-peer"),
    });

    assert.equal(
      await waitFor(() => link.closeMessage === "peer id mismatch"),
      true,
    );
  });

  test("reports a failed hello as a handshake failure", (t) => {
    const node = new Node({ cluster: "hello-failure" });
    const { port1, port2 } = new MessageChannel();
    const hello = NodeLink.prototype.hello;
    const close = NodeLink.prototype.close;
    let seen: { reason: string; message: string } | undefined;

    t.after(() => {
      node.close();
      port2.close();
    });

    NodeLink.prototype.hello = () => {
      throw new Error("send failed");
    };
    NodeLink.prototype.close = function (
      reason = "close",
      message = "node is closed",
    ) {
      seen = { reason, message };
      close.call(this, reason, message);
    };

    try {
      assert.throws(() => node.attach(port1), /send failed/);
    } finally {
      NodeLink.prototype.hello = hello;
      NodeLink.prototype.close = close;
    }

    assert.deepEqual(seen, { reason: "close", message: "handshake failed" });
  });

  test("close emits link-down for an outbound link", async (t) => {
    const hub = new Node({ cluster: "close-outbound-hub" });
    const peer = new Node({ cluster: "close-outbound-hub" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    const address = await hub.listen("tcp://127.0.0.1:0");
    const reasons: string[] = [];

    peer.events.on("link-down", (link) => reasons.push(link.reason));

    await peer.connect(`tcp://127.0.0.1:${address.port}`);

    assert.equal(peer.snapshot().links[0].peerId, hub.nodeId);

    peer.close();

    assert.equal(await waitFor(() => reasons.length === 1), true);
    assert.deepEqual(reasons, ["close"]);
  });

  test("emits member-up and member-down events", async (t) => {
    const hub = new Node({ cluster: "obs-member-events" });
    const peer = new Node({ cluster: "obs-member-events" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    const ups: unknown[] = [];
    const downs: unknown[] = [];

    hub.events.on("member-up", (member) => ups.push(member));
    hub.events.on("member-down", (member) => downs.push(member));

    const address = await hub.listen("tcp://127.0.0.1:0");
    const peerAddress = await peer.listen("tcp://127.0.0.1:0", {
      advertise: "tcp://127.0.0.1:0",
    });

    await peer.connect(`tcp://127.0.0.1:${address.port}`);

    assert.notEqual(peerAddress.port, 0);
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

  test("propagates cancellation to the remote handler", async (t) => {
    const a = new Node({ cluster: "cancel-remote-a" });
    const b = new Node({ cluster: "cancel-remote-a" });
    const { port1, port2 } = new MessageChannel();
    let seen: { from: string; signal: AbortSignal } | undefined;

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const up = new Promise<void>((resolve) => a.watch("slow", () => resolve()));

    b.register("slow", {
      work: (ctx) =>
        new Promise((resolve) => {
          seen = { from: ctx.from, signal: ctx.signal };
          ctx.signal.addEventListener("abort", () => resolve("late"));
        }),
    });

    a.attach(port1);
    b.attach(port2);

    await within(up, 2000);

    const controller = new AbortController();
    const call = a.call("slow", "work", [], {
      signal: controller.signal,
      timeout: 5000,
    });

    await waitFor(() => seen !== undefined);

    controller.abort();

    await assert.rejects(call, (err: SwarmError) => err.code === "CANCELLED");
    await waitFor(() => seen?.signal.aborted === true);

    assert.equal(seen?.from, a.nodeId);
  });

  test("aborts the handler signal when the inbound call times out", async (t) => {
    const a = new Node({ cluster: "inbound-timeout" });
    const b = new Node({ cluster: "inbound-timeout" });
    const { port1, port2 } = new MessageChannel();
    let seen: AbortSignal | undefined;

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const up = new Promise<void>((resolve) => a.watch("slow", () => resolve()));

    b.register("slow", {
      work: (ctx) => {
        seen = ctx.signal;
        return new Promise(() => {});
      },
    });

    a.attach(port1);
    b.attach(port2);

    await within(up, 2000);

    await assert.rejects(
      a.call("slow", "work", [], { timeout: 50 }),
      (err: SwarmError) => err.code === "TIMEOUT",
    );

    await waitFor(() => seen?.aborted === true);

    assert.equal((seen?.reason as SwarmError).code, "TIMEOUT");
  });

  test("starts inbound handlers in arrival order and runs them concurrently", async (t) => {
    const a = new Node({ cluster: "inbound-order" });
    const b = new Node({ cluster: "inbound-order" });
    const { port1, port2 } = new MessageChannel();
    const order: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const up = new Promise<void>((resolve) => a.watch("svc", () => resolve()));

    b.register("svc", {
      hit: async (_ctx, n: number) => {
        order.push(`start${n}`);

        if (n === 1) {
          await gate;
        }

        order.push(`end${n}`);

        return n;
      },
    });

    a.attach(port1);
    b.attach(port2);

    await within(up, 2000);

    const first = a.call("svc", "hit", [1]);
    const second = a.call("svc", "hit", [2]);

    assert.equal(await within(second, 2000), 2);
    assert.deepEqual(order, ["start1", "start2", "end2"]);

    release();

    assert.equal(await within(first, 2000), 1);
    assert.deepEqual(order, ["start1", "start2", "end2", "end1"]);
  });

  test("does not abort the handler signal after a successful return", async (t) => {
    const a = new Node({ cluster: "inbound-signal" });
    const b = new Node({ cluster: "inbound-signal" });
    const { port1, port2 } = new MessageChannel();
    let signal: AbortSignal | undefined;
    let aborts = 0;

    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const up = new Promise<void>((resolve) => a.watch("svc", () => resolve()));

    b.register("svc", {
      hit: async (ctx) => {
        signal = ctx.signal;
        ctx.signal.addEventListener("abort", () => {
          aborts += 1;
        });

        await new Promise((resolve) => setTimeout(resolve, 20));

        return "ok";
      },
    });

    a.attach(port1);
    b.attach(port2);

    await within(up, 2000);

    assert.equal(await within(a.call("svc", "hit", []), 2000), "ok");

    await new Promise((resolve) => setTimeout(resolve, 30));

    assert.equal(signal?.aborted, false);
    assert.equal(aborts, 0);
  });

  test("callAll rejects with cancelled when the signal aborts", async (t) => {
    const node = new Node({ cluster: "cancel-call-all" });

    t.after(() => node.close());

    let calls = 0;

    node.register("slow", {
      work: () => {
        calls += 1;
        return new Promise((resolve) => setTimeout(() => resolve("done"), 20));
      },
    });

    const controller = new AbortController();
    const promise = node.callAll("slow", "work", [], {
      signal: controller.signal,
    });

    controller.abort();

    await assert.rejects(
      promise,
      (err: SwarmError) => err.kind === "cancelled" && err.code === "CANCELLED",
    );

    assert.equal(calls, 0);
  });

  test("leave removes a member and closes its link", async (t) => {
    const hub = new Node({ cluster: "leave-hub" });
    const peer = new Node({ cluster: "leave-hub" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    const address = await hub.listen("tcp://127.0.0.1:0");

    await peer.listen("tcp://127.0.0.1:0", {
      advertise: "tcp://127.0.0.1:0",
    });
    await peer.connect(`tcp://127.0.0.1:${address.port}`);

    await waitFor(
      () =>
        hub.snapshot().members.some((m) => m.id === peer.nodeId) &&
        hub.snapshot().links.length > 0,
    );

    const member = hub
      .snapshot()
      .members.find((item) => item.id === peer.nodeId);

    assert.equal(
      member !== undefined ? member.startup : undefined,
      peer.startup,
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
    const b = new Node({ cluster: "bp-a" });
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

  test("addSeed dials the new seed and removeSeed forgets it", async (t) => {
    const hub = new Node({ cluster: "seed-hub" });
    const node = new Node({ cluster: "seed-hub" });

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

  test("rejects invalid seeds and refuses removeSeed after close", (t) => {
    const node = new Node({ cluster: "seed-validation" });

    t.after(() => node.close());

    const rejected = (err: SwarmError) =>
      err.kind === "rejected" && err.code === "REJECTED";

    assert.throws(
      () => new Node({ cluster: "seed-validation", seeds: ["not a url"] }),
      rejected,
    );
    assert.throws(() => node.addSeed("not a url"), rejected);
    assert.deepEqual(node.snapshot().seeds, []);

    node.close();

    assert.throws(
      () => node.removeSeed("tcp://127.0.0.1:1"),
      (err: SwarmError) => err.code === "CLOSED",
    );
  });

  test("keeps redundant explicit links to the same peer", async (t) => {
    const a = new Node({ cluster: "redun-a" });
    const b = new Node({ cluster: "redun-a" });
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
    const b = new Node({ cluster: "from-a" });
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

  test("delivers locally but not remotely when the payload is not encodable", async (t) => {
    const a = new Node({ cluster: "publish-a" });
    const b = new Node({ cluster: "publish-a" });
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

    const errors: SwarmError[] = [];

    a.events.on("error", (error) => errors.push(error as SwarmError));

    a.publish("news", () => {});

    assert.equal(await waitFor(() => errors.length === 1), true);
    assert.equal(errors[0].code, "PUB_DROPPED");
    assert.equal(errors[0].message, "data is not cloneable");

    assert.equal(local, 1);
    assert.equal(remote, 0);
  });

  test("call rejects arguments that cannot round-trip", async (t) => {
    const a = new Node({ cluster: "codec-a" });
    const b = new Node({ cluster: "codec-a" });

    t.after(() => {
      a.close();
      b.close();
    });

    b.register("math", { add: (_ctx, x: number, y: number) => x + y });

    const address = await b.listen("tcp://127.0.0.1:0");

    await a.connect(`tcp://127.0.0.1:${address.port}`);
    await waitFor(() => a.snapshot().services.length === 1);

    const started = Date.now();

    await assert.rejects(
      a.call("math", "add", [new Map([[{}, 1]]), 2], { timeout: 200 }),
      (err: SwarmError) =>
        err.kind === "failed" && err.code === "ENVELOPE_INVALID",
    );

    assert.ok(Date.now() - started < 500);
    assert.equal(await a.call("math", "add", [1, 2]), 3);
  });

  test("routes to another link after the first link closes", async (t) => {
    const a = new Node({ cluster: "failover-a" });
    const b = new Node({ cluster: "failover-a" });
    const c = new Node({ cluster: "failover-a" });
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
    const b = new Node({ cluster: "balance-a" });
    const c = new Node({ cluster: "balance-a" });
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
    const b = new Node({ cluster: "retry-a" });
    const c = new Node({ cluster: "retry-a" });
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
    const b = new Node({ cluster: "no-retry-a" });
    const c = new Node({ cluster: "no-retry-a" });
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
    const b = new Node({ cluster: "union-a" });
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
    const b = new Node({ cluster: "watch-unsubscribe-a" });
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
    const b = new Node({ cluster: "late-register-a" });
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
    const b = new Node({ cluster: "late-publish-a" });
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

  test("attach and join reject a port that is already linked", (t) => {
    const pairs = [
      ["attach", "attach"],
      ["attach", "join"],
      ["join", "attach"],
      ["join", "join"],
    ] as const;

    for (const [first, second] of pairs) {
      const a = new Node({ cluster: `duplicate-${first}-a` });
      const b = new Node({ cluster: `duplicate-${first}-a` });
      const { port1, port2 } = new MessageChannel();

      t.after(() => {
        a.close();
        b.close();
        port1.close();
        port2.close();
      });

      first === "attach" ? a.attach(port1) : a.join(port1);

      assert.throws(
        () => (second === "attach" ? b.attach(port1) : b.join(port1)),
        (err: SwarmError) =>
          err.kind === "rejected" && /already linked/.test(err.message),
      );
    }
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
      const peer = new Node({ cluster: `${protocol}-return-hub` });

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
        const peer = new Node({ cluster: `${protocol}-call-hub` });

        t.after(() => {
          hub.close();
          peer.close();
        });

        const up = new Promise<WatchEvent>((resolve) =>
          hub.watch("math", resolve),
        );

        await connectPair(protocol, hub, peer);

        peer.register("math", { add: (_ctx, a: number, b: number) => a + b });

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
          cluster: `${protocol}-host-hub`,
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
          cluster: `${protocol}-secret-hub`,
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
      const peer = new Node({ cluster: `${protocol}-publish-hub` });

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
      "delivers locally but not remotely when the payload is not encodable",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({ cluster: `${protocol}-circular-hub` });
        const peer = new Node({ cluster: `${protocol}-circular-hub` });

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

        const errors: SwarmError[] = [];

        peer.events.on("error", (error) => errors.push(error as SwarmError));

        peer.publish("news", circular);

        assert.equal(await waitFor(() => errors.length === 1), true);
        assert.equal(errors[0].code, "PUB_DROPPED");
        assert.equal(errors[0].message, "envelope is not encodable");

        assert.equal(local, 1);
        assert.equal(remote, 0);
      },
    );

    test(
      "delivers locally but not remotely when the payload exceeds the envelope limit",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({ cluster: `${protocol}-oversize-hub` });
        const peer = new Node({ cluster: `${protocol}-oversize-hub` });

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

        const errors: SwarmError[] = [];

        peer.events.on("error", (error) => errors.push(error as SwarmError));

        peer.publish("news", "x".repeat(9 * 1024 * 1024));

        assert.equal(await waitFor(() => errors.length === 1), true);
        assert.equal(errors[0].code, "PUB_DROPPED");
        assert.equal(errors[0].message, "envelope is too large");

        assert.equal(local, 1);
        assert.equal(remote, 0);
      },
    );

    test(
      "returns the bound address when listening on port 0",
      { timeout: 3000 },
      async (t) => {
        const hub = new Node({ cluster: `${protocol}-ephemeral` });
        const peer = new Node({ cluster: `${protocol}-ephemeral` });

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
        const peer = new Node({ cluster: `${protocol}-secret-hub` });
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
        const peer = new Node({ cluster: `${protocol}-close-hub` });

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
        const peer = new Node({ cluster: `${protocol}-fallback-hub` });

        t.after(() => peer.close());
        t.after(() => hub.close());

        const up = new Promise<WatchEvent>((resolve) =>
          hub.watch("math", (event) => {
            if (event.type === "up" && event.instance.nodeId === peer.nodeId) {
              resolve(event);
            }
          }),
        );

        hub.register("math", { add: (_ctx, a: number, b: number) => a + b });

        await connectPair(protocol, hub, peer);

        peer.register("math", { mul: (_ctx, a: number, b: number) => a * b });

        await up;

        assert.equal(await hub.call("math", "add", [2, 3]), 5);
        assert.equal(await hub.call("math", "mul", [2, 3]), 6);
      },
    );
  });
}

describe("Node transport and mesh", () => {
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
        err.kind === "rejected" &&
        err.message === "http server is not listening",
    );

    assert.equal(http.listenerCount("upgrade"), 0);
  });

  test("close during listen releases the bound server", async (t) => {
    const port = await freePort();
    const node = new Node({ cluster: "listen-close" });

    t.after(() => node.close());

    const listening = node.listen(`tcp://127.0.0.1:${port}`);

    node.close();

    await assert.rejects(
      listening,
      (err: SwarmError) =>
        err.kind === "rejected" && err.message === "node is closed",
    );

    await waitFor(async () => {
      const probe = createServer();

      try {
        await new Promise<void>((resolve, reject) => {
          probe.once("error", reject);
          probe.listen(port, "127.0.0.1", resolve);
        });

        return true;
      } catch {
        return false;
      } finally {
        await new Promise<void>((resolve) => probe.close(() => resolve()));
      }
    });
  });

  test("connect racing with close rejects without leaving a client", async (t) => {
    const peer = new Node({ cluster: "node" });
    const { port } = await peer.listen("tcp://127.0.0.1:0");
    const node = new Node({ cluster: "node" });

    t.after(() => {
      peer.close();
      node.close();
    });

    const connecting = node.connect(`tcp://127.0.0.1:${port}`);

    node.close();

    await assert.rejects(connecting, (err: SwarmError) => {
      return err.kind === "rejected" && err.message === "node is closed";
    });

    const clients = (node as unknown as { transport: { clients: unknown[] } })
      .transport.clients;

    assert.equal(clients.length, 0);
    assert.equal(peer.snapshot().links.length, 0);
  });

  test("registrations hands out frozen instances", async (t) => {
    const node = new Node({ cluster: "node" });

    t.after(() => node.close());

    node.register(
      "math",
      { add: (_ctx, a: number, b: number) => a + b },
      { tags: { tier: ["a"] } },
    );

    const listed = node.registrations()[0] as unknown as {
      service: string;
      meta: { tags: { tier: string[] } };
    };

    assert.equal(Object.isFrozen(listed), true);
    assert.equal(Object.isFrozen(listed.meta.tags), true);
    assert.throws(() => {
      listed.service = "hacked";
    }, TypeError);
    assert.throws(() => {
      listed.meta.tags.tier.push("b");
    }, TypeError);

    assert.deepEqual(
      node.registrations().map((item) => item.service),
      ["math"],
    );
    assert.equal(await node.call("math", "add", [1, 2]), 3);
  });

  test("closing an established link twice tears it down once", async (t) => {
    const a = new Node({ cluster: "node" });
    const { port } = await a.listen("tcp://127.0.0.1:0");
    const b = new Node({
      cluster: "node",
      seeds: [`tcp://127.0.0.1:${port}`],
    });

    await b.listen("tcp://127.0.0.1:0");

    t.after(() => {
      a.close();
      b.close();
    });

    await waitFor(() => a.snapshot().links.length === 1);
    await waitFor(() => b.snapshot().links.length === 1);

    b.close();
    b.close();

    assert.equal(b.snapshot().links.length, 0);
    await waitFor(() => a.snapshot().links.length === 0);
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

  test("listen emits an error when multicast cannot start", async (t) => {
    const node = new Node({
      cluster: "multicast-fail",
      multicast: { group: "not-an-ip" },
    });

    t.after(() => node.close());

    const raised = new Promise<Error>((resolve) =>
      node.events.once("error", resolve),
    );

    await node.listen("tcp://127.0.0.1:0", {
      advertise: "tcp://127.0.0.1:0",
    });

    assert.equal(((await raised) as SwarmError).code, "MULTICAST_UNAVAILABLE");
  });

  test("snapshot reports links and services", async (t) => {
    const hub = new Node({ cluster: "snapshot-hub" });
    const peer = new Node({ cluster: "snapshot-hub" });

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
    const peer = new Node({ cluster: "snapshot-ws-hub" });

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
    const peer = new Node({ cluster: "wss-ca-hub" });

    t.after(() => {
      hub.close();
      peer.close();
    });

    hub.register("math", { add: (_ctx, a: number, b: number) => a + b });

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
    const peer = new Node({ cluster: "bye-host" });

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

  test("connecting to the same address twice reuses one outbound client", async (t) => {
    const hub = new Node({ cluster: "connect-twice" });
    const peer = new Node({ cluster: "connect-twice" });
    const port = await freePort();
    const url = `tcp://127.0.0.1:${port}`;
    let ups = 0;
    let downs = 0;

    t.after(() => {
      hub.close();
      peer.close();
    });

    await hub.listen(url);

    hub.events.on("link-up", () => (ups += 1));
    hub.events.on("link-down", () => (downs += 1));

    await peer.connect(url, { retryDelay: 20 });
    await waitFor(() => ups === 1);

    await peer.connect(url, { retryDelay: 20 });

    await new Promise<void>((resolve) => setTimeout(resolve, 300));

    assert.equal(ups, 1);
    assert.equal(downs, 0);
    assert.equal(peer.snapshot().links.length, 1);
  });

  test("rejects a seed that points back at the node itself", async (t) => {
    const node = new Node({ cluster: "self-seed" });
    let ups = 0;
    let attaches = 0;
    const target = node as unknown as {
      attachLink: (...args: unknown[]) => void;
    };
    const attachLink = target.attachLink.bind(target);

    t.after(() => node.close());

    const address = await node.listen("tcp://127.0.0.1:0");

    node.events.on("link-up", () => (ups += 1));
    target.attachLink = (...args: unknown[]) => {
      attaches += 1;
      attachLink(...args);
    };

    node.addSeed(`tcp://127.0.0.1:${address.port}`);

    await new Promise<void>((resolve) => setTimeout(resolve, 2300));

    assert.equal(ups, 0);
    assert.equal(attaches, 1);
    assert.deepEqual(node.snapshot().links, []);

    const clients = (node as unknown as { transport: { clients: unknown[] } })
      .transport.clients;

    assert.equal(clients.length, 0);
  });

  test("dedupe closes dialed links with a duplicate link message", (t) => {
    const node = new Node({ cluster: "dedupe-message" });
    const internals = node as unknown as {
      attachLink: (
        link: LinkPort,
        secret?: string,
        expected?: string,
        addr?: string,
      ) => void;
      dropDialed: (link: NodeLink) => void;
      dropOutbound: (link: NodeLink) => void;
      nodeLinks: NodeLink[];
    };

    t.after(() => node.close());

    for (const [index, method] of ["dropDialed", "dropOutbound"].entries()) {
      const { port1, port2 } = new MessageChannel();

      t.after(() => port2.close());

      internals.attachLink(
        new LinkPort(port1, node.nodeId),
        undefined,
        undefined,
        `tcp://127.0.0.1:${10000 + index}`,
      );

      const link = internals.nodeLinks[internals.nodeLinks.length - 1];

      assert.notEqual(link, undefined);
      internals[method as "dropDialed" | "dropOutbound"](link);

      assert.equal(link.closeReason, "dedupe");
      assert.equal(link.closeMessage, "duplicate link");
    }
  });

  test("leave drops the outbound client instead of letting it reconnect", async (t) => {
    const hub = new Node({ cluster: "leave-outbound" });
    const peer = new Node({ cluster: "leave-outbound" });
    const port = await freePort();
    const url = `tcp://127.0.0.1:${port}`;
    let ups = 0;
    const messages: string[] = [];

    t.after(() => {
      hub.close();
      peer.close();
    });

    await hub.listen(url);

    hub.events.on("link-up", () => (ups += 1));
    peer.events.on("link-down", (link) => messages.push(link.message));

    await peer.connect(url, { retryDelay: 20 });
    await waitFor(() => ups === 1);

    peer.leave(hub.nodeId);

    await waitFor(() => peer.snapshot().links.length === 0);
    assert.equal(
      await waitFor(
        () => messages.length === 1 && messages[0] === "peer removed",
      ),
      true,
    );

    await new Promise<void>((resolve) => setTimeout(resolve, 300));

    assert.equal(ups, 1);
    assert.equal(peer.snapshot().links.length, 0);
  });

  test("leave lets discovery dial the seed again", async (t) => {
    const hub = new Node({ cluster: "leave-seed" });
    const peer = new Node({ cluster: "leave-seed" });
    const port = await freePort();
    const url = `tcp://127.0.0.1:${port}`;
    let ups = 0;

    t.after(() => {
      hub.close();
      peer.close();
    });

    await peer.listen(url);

    hub.events.on("link-up", () => (ups += 1));

    hub.addSeed(url);
    await waitFor(() => ups === 1);

    hub.leave(peer.nodeId);
    await waitFor(() => hub.snapshot().links.length === 0);

    await hub.listen("tcp://127.0.0.1:0");

    assert.equal(await waitFor(() => ups === 2), true);
    assert.equal(hub.snapshot().links.length, 1);
  });

  test("removeSeed drops the connection it was maintaining", async (t) => {
    const hub = new Node({ cluster: "seed-drop" });
    const peer = new Node({ cluster: "seed-drop" });
    const port = await freePort();
    const url = `tcp://127.0.0.1:${port}`;

    t.after(() => {
      hub.close();
      peer.close();
    });

    await peer.listen(url);

    hub.addSeed(url);
    await waitFor(() => hub.snapshot().links.length === 1);

    hub.removeSeed(url);

    await waitFor(() => hub.snapshot().links.length === 0);

    await new Promise<void>((resolve) => setTimeout(resolve, 300));

    assert.deepEqual(hub.snapshot().links, []);
    assert.deepEqual(hub.snapshot().seeds, []);
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
        err.kind === "rejected" &&
        err.message === "invalid url: tcp://127.0.0.1",
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

  test("a call may raise its hop budget above the configured limit", async (t) => {
    const a = new Node({ cluster: "hops-up", maxHops: 1 });
    const b = new Node({ cluster: "hops-up", maxHops: 1 });
    const c = new Node({ cluster: "hops-up", maxHops: 1 });
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
      () => a.call("math", "add"),
      (err: SwarmError) => err.kind === "not-found",
    );

    assert.equal(await a.call("math", "add", [], { ttl: 2 }), 1);
  });

  test("a call may lower its hop budget", async (t) => {
    const a = new Node({ cluster: "hops-down" });
    const b = new Node({ cluster: "hops-down" });
    const c = new Node({ cluster: "hops-down" });
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

    assert.equal(await a.call("math", "add", [], { ttl: 3 }), 1);

    await assert.rejects(
      () => a.call("math", "add", [], { ttl: 1 }),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("rejects an invalid hop budget", async (t) => {
    const node = new Node({ cluster: "hops-invalid" });

    t.after(() => node.close());

    node.register("math", { add: () => 1 });

    for (const ttl of [0, -1, 1.5, NaN, Infinity]) {
      await assert.rejects(
        () => node.call("math", "add", [], { ttl }),
        (err: SwarmError) =>
          err.kind === "rejected" && err.code === "INVALID_HOPS",
      );
      await assert.rejects(
        () => node.callAll("math", "add", [], { ttl }),
        (err: SwarmError) =>
          err.kind === "rejected" && err.code === "INVALID_HOPS",
      );
    }
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

    a.register("math", { add: (_ctx, x: number, y: number) => x * y });
    c.register("math", {
      add: (_ctx, x: number, y: number) => {
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

    const results = await a.callAll("math", "add", [1, 2]);
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

    b.register("math", { add: (_ctx, x: number, y: number) => x + y });

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

  test("callAll invokes local and remote instances and dedupes by peer", async (t) => {
    const local = new Node({ cluster: "collect-local" });
    const remote = new Node({ cluster: "collect-local" });

    t.after(() => {
      local.close();
      remote.close();
    });

    local.register("math", {
      add: async (_ctx, a: number, b: number) => a + b,
    });
    remote.register("math", {
      add: async (_ctx, a: number, b: number) => a + b + 2,
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

    const results = await local.callAll("math", "add", [1, 2]);

    assert.equal(results.length, 2);
    assert.deepEqual(results[0], { nodeId: local.nodeId, ok: true, value: 3 });
    assert.deepEqual(results[1], { nodeId: remote.nodeId, ok: true, value: 5 });
  });

  test("listens on a websocket path and advertises it", async (t) => {
    const provider = new Node({ cluster: "ws-path-provider" });
    const consumer = new Node({ cluster: "ws-path-provider" });
    const http = createHttpServer((_request, response) => response.end("ok"));
    const port = await freePort();

    await new Promise<void>((resolve) =>
      http.listen(port, "127.0.0.1", resolve),
    );

    t.after(() => {
      consumer.close();
      provider.close();
      http.close();
    });

    provider.register("math", {
      add: async (_ctx, a: number, b: number) => a + b,
    });

    const listening = await provider.listen(`ws://127.0.0.1:${port}/swarm`, {
      advertise: `ws://127.0.0.1:${port}`,
      ws: { server: http },
    });

    const advertised = [
      ...(provider as unknown as NodeInternals).membership.members.values(),
    ][0];

    assert.equal(listening.port, port);
    assert.equal(advertised.addr, `ws://127.0.0.1:${port}/swarm`);

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

  test("callAll rejects after close", async () => {
    const node = new Node({ cluster: "collect-closed" });

    node.close();

    await assert.rejects(
      () => node.callAll("math", "add", [1, 2]),
      (error: SwarmError) => error.kind === "rejected",
    );
  });

  test("seeds dial peers for a client that never listens", async (t) => {
    const port = await freePort();
    const seed = new Node({ cluster: "seed-client" });

    t.after(() => seed.close());

    seed.register("math", { add: async (_ctx, a: number, b: number) => a + b });

    await seed.listen(`tcp://127.0.0.1:${port}`);

    const node = new Node({
      cluster: "seed-client",
      seeds: [`tcp://127.0.0.1:${port}`],
    });

    t.after(() => node.close());

    const up = new Promise<boolean>((resolve) =>
      node.watch("math", (event) => {
        if (event.type === "up" && event.instance.nodeId === seed.nodeId) {
          resolve(true);
        }
      }),
    );
    const late = new Promise<boolean>((resolve) =>
      setTimeout(() => resolve(false), 2000).unref(),
    );

    assert.equal(await Promise.race([up, late]), true);

    assert.equal(await node.call("math", "add", [1, 2]), 3);
  });

  test("peers converge through a shared seed", async (t) => {
    const seedPort = await freePort();
    const leftPort = await freePort();
    const rightPort = await freePort();
    const seed = new Node({ cluster: "mesh-seed" });
    const left = new Node({
      cluster: "mesh-seed",
      seeds: [`tcp://127.0.0.1:${seedPort}`],
    });
    const right = new Node({
      cluster: "mesh-seed",
      seeds: [`tcp://127.0.0.1:${seedPort}`],
    });

    t.after(() => {
      left.close();
      right.close();
      seed.close();
    });

    right.register("math", {
      add: async (_ctx, a: number, b: number) => a + b,
    });

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

  test("keeps one link when both peers seed each other", async (t) => {
    const leftPort = await freePort();
    const rightPort = await freePort();
    const left = new Node({ cluster: "dedupe-left" });
    const right = new Node({ cluster: "dedupe-left" });

    t.after(() => {
      left.close();
      right.close();
    });

    let ups = 0;
    let downs = 0;
    const reasons: string[] = [];

    left.events.on("link-up", () => ups++);
    left.events.on("link-down", (link) => {
      downs++;
      reasons.push(link.reason);
    });
    right.events.on("link-down", (link) => reasons.push(link.reason));

    await left.listen(`tcp://0.0.0.0:${leftPort}`, {
      advertise: `tcp://127.0.0.1:${leftPort}`,
    });
    await right.listen(`tcp://0.0.0.0:${rightPort}`, {
      advertise: `tcp://127.0.0.1:${rightPort}`,
    });

    left.addSeed(`tcp://127.0.0.1:${rightPort}`);
    right.addSeed(`tcp://127.0.0.1:${leftPort}`);

    assert.equal(
      await waitFor(
        () =>
          left.snapshot().links.length === 1 &&
          right.snapshot().links.length === 1,
      ),
      true,
    );

    await new Promise<void>((resolve) => setTimeout(resolve, 300));

    assert.equal(left.snapshot().links.length, 1);
    assert.equal(right.snapshot().links.length, 1);
    assert.equal(ups - downs, left.snapshot().links.length);
    assert.equal(reasons.length > 0, true);
    assert.equal(
      reasons.every((reason) => reason === "dedupe" || reason === "error"),
      true,
    );
  });

  test("keeps one link when unix discovery and a seed overlap", async (t) => {
    const dir = await mkdtemp(join(tmpdir(), "swarm-dedupe-unix-"));
    const cluster = `dedupe-unix-${Date.now()}`;
    const port = await freePort();
    const a = new Node({ cluster, discovery: { dir, interval: 20 } });
    const b = new Node({ cluster, discovery: { dir, interval: 20 } });

    t.after(() => {
      a.close();
      b.close();
      return rm(dir, { recursive: true, force: true });
    });

    await a.listen(`tcp://0.0.0.0:${port}`, {
      advertise: `tcp://127.0.0.1:${port}`,
    });

    b.addSeed(`tcp://127.0.0.1:${port}`);

    assert.equal(
      await waitFor(
        () =>
          a.snapshot().links.length === 1 && b.snapshot().links.length === 1,
      ),
      true,
    );

    await new Promise<void>((resolve) => setTimeout(resolve, 300));

    assert.equal(a.snapshot().links.length, 1);
    assert.equal(b.snapshot().links.length, 1);
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

    provider.register("math", { add: (_ctx, a: number, b: number) => a + b });

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

  test("tcp advertise does not inherit a websocket listen path", async (t) => {
    const provider = new Node({ cluster: "mixed-advertise" });
    const http = createHttpServer();
    const port = await freePort();

    await new Promise<void>((resolve) =>
      http.listen(port, "127.0.0.1", resolve),
    );

    t.after(() => {
      provider.close();
      http.close();
    });

    await provider.listen(`ws://127.0.0.1:${port}/swarm`, {
      advertise: `tcp://127.0.0.1:0`,
      ws: { server: http },
    });

    const advertised = [
      ...(provider as unknown as NodeInternals).membership.members.values(),
    ][0];

    assert.equal(advertised.addr, `tcp://127.0.0.1:${port}`);
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

    const learned = new Promise<
      Array<{ id: string; addr: string; startup: number }>
    >((resolve) => {
      port2.addEventListener("message", (event) => {
        const message = event.data as { t?: string; members?: unknown };

        if (
          message.t === "peers" &&
          Array.isArray(message.members) &&
          message.members.length > 0
        ) {
          resolve(
            message.members as Array<{
              id: string;
              addr: string;
              startup: number;
            }>,
          );
        }
      });
    });

    node.join(port1);
    port2.start();
    port2.postMessage({
      t: "hello",
      cluster: "advertise-on",
      from: "ghost",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "ghost"),
    });

    assert.deepEqual(await learned, [
      {
        id: node.nodeId,
        addr: `tcp://127.0.0.1:${port}`,
        startup: node.startup,
      },
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
          err.kind === "rejected" &&
          err.message.startsWith("invalid advertise"),
      );
    }
  });

  test("listen fills the bound port when advertise leaves it open", async () => {
    const cases = [
      {
        cluster: "advertise-port-zero",
        listen: "tcp://0.0.0.0:0",
        advertise: "tcp://127.0.0.1:0",
        scheme: "tcp",
      },
      {
        cluster: "advertise-port-zero",
        listen: "ws://0.0.0.0:0",
        advertise: "ws://127.0.0.1/",
        scheme: "ws",
      },
    ];

    for (const item of cases) {
      const node = new Node({ cluster: item.cluster });
      const { port1, port2 } = new MessageChannel();

      try {
        const bound = await node.listen(item.listen, {
          advertise: item.advertise,
        });

        const learned = new Promise<
          Array<{ id: string; addr: string; startup: number }>
        >((resolve) => {
          port2.addEventListener("message", (event) => {
            const message = event.data as { t?: string; members?: unknown };

            if (
              message.t === "peers" &&
              Array.isArray(message.members) &&
              message.members.length > 0
            ) {
              resolve(
                message.members as Array<{
                  id: string;
                  addr: string;
                  startup: number;
                }>,
              );
            }
          });
        });

        node.join(port1);
        port2.start();
        port2.postMessage({
          t: "hello",
          cluster: "advertise-port-zero",
          from: "ghost",
          version: protocolVersion,
          startup: 1,
          token: signToken("", "ghost"),
        });

        assert.deepEqual(await learned, [
          {
            id: node.nodeId,
            addr: `${item.scheme}://127.0.0.1:${bound.port}`,
            startup: node.startup,
          },
        ]);
      } finally {
        node.close();
        port1.close();
        port2.close();
      }
    }
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
    port2.postMessage({
      t: "hello",
      cluster: "advertise-off",
      from: "ghost",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "ghost"),
    });

    assert.deepEqual(await learned, []);
  });

  test("a passive node does not dial the members it learns", async (t) => {
    const hub = new Node({ cluster: "passive-dial" });
    const target = new Node({ cluster: "passive-dial" });
    const passive = new Node({ cluster: "passive-dial", passive: "leaf" });
    const active = new Node({ cluster: "passive-dial" });

    t.after(() => {
      hub.close();
      target.close();
      passive.close();
      active.close();
    });

    target.register("idle", { ping: () => 1 });
    hub.register("math", { add: (_ctx, a: number, b: number) => a + b });

    const targetAddr = await target.listen("tcp://127.0.0.1:0", {
      advertise: "tcp://127.0.0.1:0",
    });
    const hubAddr = await hub.listen("tcp://127.0.0.1:0", {
      advertise: "tcp://127.0.0.1:0",
    });

    await hub.connect(`tcp://127.0.0.1:${targetAddr.port}`);
    await passive.connect(`tcp://127.0.0.1:${hubAddr.port}`);
    await active.connect(`tcp://127.0.0.1:${hubAddr.port}`);

    await waitFor(() =>
      target.snapshot().links.some((link) => link.peerId === active.nodeId),
    );

    assert.equal(
      target.snapshot().links.some((link) => link.peerId === passive.nodeId),
      false,
    );
  });

  test("a passive node serves its services without keeping the cluster table", async (t) => {
    const passive = new Node({ cluster: "passive-table", passive: "leaf" });
    const caller = new Node({ cluster: "passive-table" });

    t.after(() => {
      passive.close();
      caller.close();
    });

    passive.register("svc", { ping: () => "pong" });
    caller.register("other", { ping: () => "pong" });

    const addr = await passive.listen("tcp://127.0.0.1:0", {
      advertise: "tcp://127.0.0.1:0",
    });

    await caller.connect(`tcp://127.0.0.1:${addr.port}`);

    assert.equal(
      await waitFor(() =>
        caller.snapshot().services.some((item) => item.service === "svc"),
      ),
      true,
    );
    assert.equal(await caller.call("svc", "ping"), "pong");

    assert.deepEqual(
      passive.snapshot().services.map((item) => item.service),
      ["svc"],
    );
    assert.deepEqual(passive.snapshot().links[0].services, []);
    assert.equal(
      passive.snapshot().members.find((member) => member.id === passive.nodeId)
        ?.passive,
      "leaf",
    );
  });

  test("a passive node calls back the node that called it", async (t) => {
    const passive = new Node({ cluster: "passive-callback", passive: "leaf" });
    const caller = new Node({ cluster: "passive-callback" });
    const seen: string[] = [];

    t.after(() => {
      passive.close();
      caller.close();
    });

    caller.register("sink", {
      log: (_ctx, message: string) => {
        seen.push(message);
        return seen.length;
      },
    });

    passive.register("job", {
      run: (ctx, payload: string) =>
        passive.callTo(ctx.from, "sink", "log", [payload]),
    });

    const addr = await passive.listen("tcp://127.0.0.1:0");

    await caller.connect(`tcp://127.0.0.1:${addr.port}`);

    assert.equal(
      await waitFor(() =>
        caller.snapshot().services.some((item) => item.service === "job"),
      ),
      true,
    );
    assert.equal(await caller.call("job", "run", ["done"]), 1);
    assert.deepEqual(seen, ["done"]);
    assert.deepEqual(
      passive.snapshot().services.map((item) => item.service),
      ["job"],
    );
  });

  test("a hub never sends changes to a leaf link", async (t) => {
    const node = new Node({ cluster: "relay-leaf" });
    const leaf = new FrameLink(node.nodeId, "socket");
    const active = new FrameLink(node.nodeId, "socket");
    const internals = node as unknown as { attachLink: (link: Link) => void };
    const handshake = (from: string, passive?: "leaf" | "ghost") => ({
      t: "hello" as const,
      passive,
      cluster: "relay-leaf",
      from,
      version: protocolVersion,
      startup: 1,
      token: signToken("", from),
    });
    const changes = (link: FrameLink) =>
      link.frames.filter((frame) => frame.message.t === "changes");

    t.after(() => {
      node.close();
      leaf.close();
      active.close();
    });

    node.register("pre-svc", { ping: () => "pre" });

    internals.attachLink(leaf);
    internals.attachLink(active);
    leaf.events.emit("envelope", handshake("leaf-node", "leaf"));
    active.events.emit("envelope", handshake("active-node"));

    await runRelay();

    const sent = changes(active).length;

    assert.equal(changes(leaf).length, 0);
    assert.equal(sent >= 1, true);

    node.register("post-svc", { ping: () => "post" });
    await runRelay();

    assert.equal(changes(leaf).length, 0);
    assert.equal(changes(active).length > sent, true);
  });

  test("a hub keeps sending changes to a ghost link", async (t) => {
    const node = new Node({ cluster: "relay-ghost" });
    const ghost = new FrameLink(node.nodeId, "socket");
    const internals = node as unknown as { attachLink: (link: Link) => void };

    t.after(() => {
      node.close();
      ghost.close();
    });

    internals.attachLink(ghost);
    ghost.events.emit("envelope", {
      t: "hello",
      passive: "ghost",
      cluster: "relay-ghost",
      from: "ghost-node",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "ghost-node"),
    });

    node.register("svc", { ping: () => "pong" });
    await runRelay();

    assert.equal(
      ghost.frames.filter((frame) => frame.message.t === "changes").length,
      1,
    );
  });

  test("a ghost node learns the cluster and calls it without dialing", async (t) => {
    const hub = new Node({ cluster: "ghost", callTimeout: 2000 });
    const ghost = new Node({
      cluster: "ghost",
      passive: "ghost",
      callTimeout: 2000,
    });
    const other = new Node({ cluster: "ghost" });

    t.after(() => {
      hub.close();
      ghost.close();
      other.close();
    });

    hub.register("math", { add: (_ctx, a: number, b: number) => a + b });
    other.register("idle", { ping: () => "pong" });

    const hubAddr = await hub.listen("tcp://127.0.0.1:0", {
      advertise: "tcp://127.0.0.1:0",
    });
    await other.listen("tcp://127.0.0.1:0", {
      advertise: "tcp://127.0.0.1:0",
    });
    await other.connect(`tcp://127.0.0.1:${hubAddr.port}`);
    await ghost.connect(`tcp://127.0.0.1:${hubAddr.port}`);

    assert.equal(
      await waitFor(() =>
        ghost.snapshot().services.some((item) => item.service === "idle"),
      ),
      true,
    );
    assert.equal(await ghost.call("math", "add", [1, 2]), 3);
    assert.equal(await ghost.call("idle", "ping"), "pong");
    assert.deepEqual(
      ghost
        .snapshot()
        .links.filter((link) => link.local)
        .map((link) => link.peerId),
      [hub.nodeId],
    );

    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(
      other.snapshot().links.some((link) => link.peerId === ghost.nodeId),
      false,
    );
  });

  test("a ghost node announces itself as passive", async (t) => {
    const ghost = new Node({
      cluster: "ghost-advertise",
      passive: "ghost",
    });

    t.after(() => ghost.close());

    await ghost.listen("tcp://127.0.0.1:0", {
      advertise: "tcp://127.0.0.1:0",
    });

    assert.equal(
      ghost.snapshot().members.find((member) => member.id === ghost.nodeId)
        ?.passive,
      "ghost",
    );
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

    const learned = new Promise<
      Array<{ id: string; addr: string; startup: number }>
    >((resolve) => {
      port2.addEventListener("message", (event) => {
        const message = event.data as { t?: string; members?: unknown };

        if (
          message.t === "peers" &&
          Array.isArray(message.members) &&
          message.members.length > 0
        ) {
          resolve(
            message.members as Array<{
              id: string;
              addr: string;
              startup: number;
            }>,
          );
        }
      });
    });

    port2.postMessage({
      t: "hello",
      cluster: "peers-invalid",
      from: "ghost",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "ghost"),
    });
    port2.postMessage({
      t: "peers",
      from: "ghost",
      members: [
        { id: "wild", addr: "tcp://0.0.0.0:1", startup: 1 },
        { id: "bad", addr: "not a url", startup: 1 },
        { id: "0", addr: "tcp://127.0.0.1:1", startup: 1 },
        { id: "00", addr: "tcp://192.0.2.1:1", startup: 1 },
      ],
    });

    assert.deepEqual(await learned, [
      { id: "0", addr: "tcp://127.0.0.1:1", startup: 1 },
      { id: "00", addr: "tcp://192.0.2.1:1", startup: 1 },
    ]);
  });

  test("routes a call through a client-only relay", async (t) => {
    const consumerPort = await freePort();
    const providerPort = await freePort();
    const consumer = new Node({ cluster: "relay-consumer" });
    const relay = new Node({ cluster: "relay-consumer" });
    const provider = new Node({ cluster: "relay-consumer" });

    t.after(() => {
      consumer.close();
      relay.close();
      provider.close();
    });

    provider.register("math", {
      add: async (_ctx, a: number, b: number) => a + b,
    });

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
        if (
          event.type === "down" &&
          event.instance.nodeId === provider.nodeId
        ) {
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
    port2.postMessage({
      t: "hello",
      cluster: "hop-budget",
      from: "peer",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "peer"),
    });
    port2.postMessage({
      t: "changes",
      items: [{ t: "up", from: "peer", service: "math" }],
    });
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
    const node = new Node({ cluster: "dial-identity-target" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      node.close();
      target.close();
      port1.close();
      port2.close();
    });

    target.register("math", { add: (_ctx, x: number, y: number) => x + y });
    node.register("pingpong", { ping: () => "pong" });

    const bound = await target.listen("tcp://127.0.0.1:0");

    const up = new Promise<WatchEvent>((resolve) =>
      target.watch("pingpong", resolve),
    );

    node.join(port1);
    port2.start();
    port2.postMessage({
      t: "hello",
      cluster: "dial-identity-target",
      from: "ghost",
      version: protocolVersion,
      startup: 1,
      token: signToken("", "ghost"),
    });
    port2.postMessage({
      t: "peers",
      from: "ghost",
      members: [
        { id: "expected", addr: `tcp://127.0.0.1:${bound.port}`, startup: 1 },
      ],
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

    internals.membership.learn(
      [{ id: a.nodeId, addr: `tcp://127.0.0.1:${address.port}` }],
      true,
    );

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

    (
      a as unknown as { transport: { servers: Array<{ close(): void }> } }
    ).transport.servers[0].close();

    internals.membership.members.clear();
    internals.membership.seen.clear();

    await new Promise((resolve) => setTimeout(resolve, 50));

    await a.listen(`tcp://127.0.0.1:${address.port}`);

    const restored = await within(
      new Promise<boolean>((resolve) => {
        const tick = () => {
          if (internals.membership.members.has(a.nodeId)) {
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

  test("discovers a same-machine peer over a unix socket", async (t) => {
    const dir = await mkdtemp(join(tmpdir(), "swarm-unix-node-"));
    const cluster = `unix-node-${Date.now()}`;
    const a = new Node({ cluster, discovery: { dir, interval: 20 } });
    const b = new Node({ cluster, discovery: { dir, interval: 20 } });

    t.after(() => {
      a.close();
      b.close();
      return rm(dir, { recursive: true, force: true });
    });

    const started = [
      new Promise<void>((resolve) => a.events.on("discovery", () => resolve())),
      new Promise<void>((resolve) => b.events.on("discovery", () => resolve())),
    ];

    b.register("math", { add: (_ctx, x: number, y: number) => x + y });

    await Promise.all(started);

    assert.equal(a.snapshot().unix, true);
    assert.equal(b.snapshot().unix, true);
    assert.equal(await waitFor(() => a.snapshot().links.length === 1), true);
    assert.equal(await waitFor(() => b.snapshot().links.length === 1), true);
    assert.equal(
      await waitFor(() => a.snapshot().links[0]?.services.length === 1),
      true,
    );

    assert.equal(await a.call("math", "add", [1, 2]), 3);

    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    assert.equal(a.snapshot().links.length, 1);
    assert.equal(b.snapshot().links.length, 1);
  });

  type MembershipInternals = {
    members: Map<string, { id: string; addr: string }>;
    seen: Map<string, number>;
    learn(entries: Array<{ id: string; addr: string }>, local: boolean): void;
  };

  type NodeInternals = {
    membership: MembershipInternals;
  };

  test("attach emits link-up once per link", async (t) => {
    const first = new Node({ cluster: "node" });
    const second = new Node({ cluster: "node" });
    const { port1, port2 } = new MessageChannel();
    let firstCount = 0;
    let secondCount = 0;

    first.events.on("link-up", () => (firstCount += 1));
    second.events.on("link-up", () => (secondCount += 1));
    t.after(() => {
      first.close();
      second.close();
    });

    first.attach(port1);
    second.attach(port2);

    await waitFor(
      () =>
        first.snapshot().links.length === 1 &&
        second.snapshot().links.length === 1,
    );
    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(firstCount, 1);
    assert.equal(secondCount, 1);
  });

  test("unregister converges when it races the handshake", async (t) => {
    const a = new Node({ cluster: "node" });
    const b = new Node({ cluster: "node" });
    const c = new Node({ cluster: "node" });
    const ab = new MessageChannel();
    const bc = new MessageChannel();

    t.after(() => {
      a.close();
      b.close();
      c.close();
    });

    b.attach(bc.port1);
    c.attach(bc.port2);
    a.attach(ab.port1);
    b.attach(ab.port2);
    c.register("db", { ping: () => "pong" });

    await waitFor(() =>
      a.registrations().some((item) => item.service === "db"),
    );

    c.unregister("db");

    await waitFor(
      () =>
        !a.registrations().some((item) => item.service === "db") &&
        !b.registrations().some((item) => item.service === "db"),
    );
  });

  test("relays a cancelled failure instead of timing out", async (t) => {
    const first = new Node({ cluster: "node" });
    const second = new Node({ cluster: "node" });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      first.close();
      second.close();
    });

    first.attach(port1);
    second.attach(port2);
    second.register("svc", {
      boom: () => {
        throw new SwarmError("cancelled", "CANCELLED", "stop");
      },
    });

    await waitFor(
      () =>
        first.snapshot().links.length === 1 &&
        first.registrations().some((item) => item.service === "svc"),
    );

    await assert.rejects(
      first.call("svc", "boom", [], { timeout: 2000 }),
      (err: SwarmError) => err.kind === "cancelled" && err.code === "CANCELLED",
    );
  });

  test("applies timeout configuration to attached links", (t) => {
    const node = new Node({
      cluster: "node",
      handshakeTimeout: 123,
      heartbeatInterval: 456,
      heartbeatTimeout: 789,
      maxInboundTimeout: 42,
    });
    const { port1, port2 } = new MessageChannel();

    t.after(() => {
      node.close();
      port1.close();
      port2.close();
    });

    node.attach(port1);

    const internals = node as unknown as {
      nodeLinks: Array<{
        handshakeTimeout: number;
        heartbeatInterval: number;
        heartbeatTimeout: number;
        maxInboundTimeout: number;
      }>;
    };
    const link = internals.nodeLinks[0];

    assert.ok(link);
    assert.equal(link.handshakeTimeout, 123);
    assert.equal(link.heartbeatInterval, 456);
    assert.equal(link.heartbeatTimeout, 789);
    assert.equal(link.maxInboundTimeout, 42);
  });

  test("rejects a heartbeat timeout that is not greater than its interval", () => {
    const cases: [number | undefined, number | undefined][] = [
      [1000, 1000],
      [1000, 500],
      [undefined, 1000],
      [NaN, 45000],
      [1000, NaN],
      [0, 45000],
      [1000, Infinity],
    ];

    for (const [heartbeatInterval, heartbeatTimeout] of cases) {
      assert.throws(
        () =>
          new Node({ cluster: "node", heartbeatInterval, heartbeatTimeout }),
        (err: SwarmError) =>
          err.kind === "rejected" && err.code === "INVALID_TIMEOUT",
      );
    }
  });

  test("rejects a non-positive maxInflight", () => {
    for (const maxInflight of [0, -1, 1.5, NaN]) {
      assert.throws(
        () => new Node({ cluster: "node", maxInflight }),
        (err: SwarmError) => err.kind === "rejected" && err.code === "REJECTED",
      );
    }
  });

  test("a multicast failure crashes the process when nobody listens", () => {
    const script = `
import { Node } from "./node.ts";

const node = new Node({
  cluster: "no-error-listener",
  multicast: { group: "not-an-ip" },
});

await node.listen("tcp://127.0.0.1:0", {
  advertise: "tcp://127.0.0.1:0",
});

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
    assert.match(result.stderr, /MULTICAST_UNAVAILABLE/);
    assert.doesNotMatch(result.stdout, /survived/);
  });

  test("rejects invalid hop, dial and handshake configuration", () => {
    for (const maxHops of [0, -1, 1.5, NaN, Infinity, 65]) {
      assert.throws(
        () => new Node({ cluster: "node", maxHops }),
        (err: SwarmError) => err.kind === "rejected" && err.code === "REJECTED",
      );
    }

    for (const dialTtl of [0, -1, 1.5, NaN, Infinity]) {
      assert.throws(
        () => new Node({ cluster: "node", dialTtl }),
        (err: SwarmError) =>
          err.kind === "rejected" && err.code === "INVALID_TIMEOUT",
      );
    }

    assert.throws(
      () =>
        new Node({
          cluster: "node",
          passive: "spy" as unknown as "leaf",
        }),
      (err: SwarmError) => err.kind === "rejected" && err.code === "REJECTED",
    );

    for (const key of ["handshakeTimeout", "maxInboundTimeout"] as const) {
      for (const value of [0, -1, NaN, Infinity]) {
        assert.throws(
          () => new Node({ cluster: "node", [key]: value }),
          (err: SwarmError) =>
            err.kind === "rejected" && err.code === "INVALID_TIMEOUT",
        );
      }
    }
  });

  test("rejects a non-positive memberTtl", () => {
    for (const memberTtl of [0, -1, 1.5, NaN]) {
      assert.throws(
        () => new Node({ cluster: "node", memberTtl }),
        (err: SwarmError) =>
          err.kind === "rejected" && err.code === "INVALID_TIMEOUT",
      );
    }
  });

  test("connect rejects a url with a fragment", async (t) => {
    const node = new Node({ cluster: "node" });

    t.after(() => node.close());

    await assert.rejects(
      node.connect("ws://127.0.0.1:1/swarm#frag"),
      (err: SwarmError) =>
        err.kind === "rejected" &&
        err.message === "url does not support a fragment",
    );
  });

  test("connect resolves only after the peer has authenticated", async (t) => {
    const host = new Node({ cluster: "connect-ready" });
    const peer = new Node({ cluster: "connect-ready" });
    const { port } = await host.listen("tcp://127.0.0.1:0");

    const received = new Promise<unknown>((resolve) =>
      host.subscribe("news", resolve),
    );

    t.after(() => {
      host.close();
      peer.close();
    });

    await peer.connect(`tcp://127.0.0.1:${port}`);

    assert.equal(
      peer.snapshot().links.some((link) => link.peerId === host.nodeId),
      true,
    );

    peer.publish("news", { ok: true });

    assert.deepEqual(await received, { ok: true });
  });

  test("connect rejects when the peer never completes the handshake", async (t) => {
    const server = createServer();
    const port = await freePort();

    await new Promise<void>((resolve) =>
      server.listen(port, "127.0.0.1", resolve),
    );

    const node = new Node({ cluster: "connect-timeout", handshakeTimeout: 50 });

    t.after(() => {
      node.close();
      server.close();
    });

    await assert.rejects(
      node.connect(`tcp://127.0.0.1:${port}`),
      (err: SwarmError) =>
        err.kind === "timeout" && err.code === "HANDSHAKE_TIMEOUT",
    );
  });

  test("stops reconnecting after a handshake timeout", async (t) => {
    const server = createServer();
    const port = await freePort();
    let connections = 0;

    server.on("connection", () => {
      connections += 1;
    });

    await new Promise<void>((resolve) =>
      server.listen(port, "127.0.0.1", resolve),
    );

    const node = new Node({ cluster: "connect-stop", handshakeTimeout: 50 });

    t.after(() => {
      node.close();
      server.close();
    });

    await assert.rejects(
      node.connect(`tcp://127.0.0.1:${port}`, { retryDelay: 50 }),
      (err: SwarmError) => err.code === "HANDSHAKE_TIMEOUT",
    );

    await new Promise((resolve) => setTimeout(resolve, 200));

    assert.equal(connections, 1);
  });

  test("connect retries with a fresh handshake after a timeout", async (t) => {
    const server = createServer();
    const port = await freePort();
    let connections = 0;

    server.on("connection", () => {
      connections += 1;
    });

    await new Promise<void>((resolve) =>
      server.listen(port, "127.0.0.1", resolve),
    );

    const node = new Node({ cluster: "connect-retry", handshakeTimeout: 50 });
    const url = `tcp://127.0.0.1:${port}`;
    const timedOut = (err: SwarmError) => err.code === "HANDSHAKE_TIMEOUT";

    t.after(() => {
      node.close();
      server.close();
    });

    await assert.rejects(node.connect(url), timedOut);
    await assert.rejects(node.connect(url), timedOut);

    assert.equal(connections, 2);
  });
});
