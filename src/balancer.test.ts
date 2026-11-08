/*
 * balancer.test.ts
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
import { Balancer } from "./balancer.ts";
import type { SwarmError } from "./errors.ts";
import type { Transport } from "./link.ts";

function stub(transport: Transport, ...services: string[]) {
  return {
    transport,
    latency: undefined as number | undefined,
    has: (service: string) => services.includes(service),
  };
}

describe("Balancer", () => {
  test("orders port links before socket links", () => {
    const balancer = new Balancer();
    const socket = stub("socket", "math");
    const port = stub("port", "math");

    assert.deepEqual(balancer.order("math", [socket, port]), [port, socket]);
  });

  test("rotates port links on each call", () => {
    const balancer = new Balancer();
    const a = stub("port", "math");
    const b = stub("port", "math");

    assert.deepEqual(balancer.order("math", [a, b]), [a, b]);
    assert.deepEqual(balancer.order("math", [a, b]), [b, a]);
    assert.deepEqual(balancer.order("math", [a, b]), [a, b]);
  });

  test("rotates port links and orders sockets by latency", () => {
    const balancer = new Balancer();
    const p1 = stub("port", "math");
    const p2 = stub("port", "math");
    const s1 = stub("socket", "math");
    const s2 = stub("socket", "math");

    s1.latency = 10;
    s2.latency = 2;

    assert.deepEqual(balancer.order("math", [p1, p2], [s1, s2]), [
      p1,
      p2,
      s2,
      s1,
    ]);
    assert.deepEqual(balancer.order("math", [p1, p2], [s1, s2]), [
      p2,
      p1,
      s2,
      s1,
    ]);
  });

  test("rotates sockets inside the same latency bucket", () => {
    const balancer = new Balancer();
    const a = stub("socket", "math");
    const b = stub("socket", "math");

    a.latency = 12;
    b.latency = 14;

    assert.deepEqual(balancer.order("math", [a, b]), [a, b]);
    assert.deepEqual(balancer.order("math", [a, b]), [b, a]);
  });

  test("places sockets without a measurement after measured ones", () => {
    const balancer = new Balancer();
    const measured = stub("socket", "math");
    const unknown = stub("socket", "math");

    measured.latency = 30;

    assert.deepEqual(balancer.order("math", [unknown, measured]), [
      measured,
      unknown,
    ]);
  });

  test("rotates across sets", () => {
    const balancer = new Balancer();
    const a = stub("port", "math");
    const b = stub("port", "math");

    assert.deepEqual(balancer.order("math", [a], [b]), [a, b]);
    assert.deepEqual(balancer.order("math", [a], [b]), [b, a]);
  });

  test("skips links that do not serve the service", () => {
    const balancer = new Balancer();
    const a = stub("port", "math");
    const b = stub("port", "echo");

    assert.deepEqual(balancer.order("math", [a, b]), [a]);
    assert.deepEqual(balancer.order("echo", [a, b]), [b]);
  });

  test("returns an empty list when nothing serves the service", () => {
    const balancer = new Balancer();
    const a = stub("port", "math");

    assert.deepEqual(balancer.order("nope", [a]), []);
    assert.deepEqual(balancer.order("nope"), []);
  });

  test("keeps a separate cursor per service", () => {
    const balancer = new Balancer();
    const a = stub("port", "math", "echo");
    const b = stub("port", "math", "echo");

    assert.deepEqual(balancer.order("math", [a, b]), [a, b]);
    assert.deepEqual(balancer.order("echo", [a, b]), [a, b]);
    assert.deepEqual(balancer.order("math", [a, b]), [b, a]);
    assert.deepEqual(balancer.order("echo", [a, b]), [b, a]);
  });

  test("invoke rejects a service no link serves", () => {
    const balancer = new Balancer();

    assert.throws(
      () => balancer.invoke("nope", "add", [], 10, 8, undefined),
      (err: SwarmError) => err.kind === "not-found",
    );
  });
});
