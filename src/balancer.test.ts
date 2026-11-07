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

  test("rotates port and socket groups independently", () => {
    const balancer = new Balancer();
    const p1 = stub("port", "math");
    const p2 = stub("port", "math");
    const s1 = stub("socket", "math");
    const s2 = stub("socket", "math");

    assert.deepEqual(balancer.order("math", [p1, p2], [s1, s2]), [
      p1,
      p2,
      s1,
      s2,
    ]);
    assert.deepEqual(balancer.order("math", [p1, p2], [s1, s2]), [
      p2,
      p1,
      s2,
      s1,
    ]);
    assert.deepEqual(balancer.order("math", [p1, p2], [s1, s2]), [
      p1,
      p2,
      s1,
      s2,
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
      () => balancer.invoke("nope", "add", [], 10),
      (err: SwarmError) => err.kind === "not-found",
    );
  });
});
