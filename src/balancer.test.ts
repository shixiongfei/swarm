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
import { SwarmError } from "./errors.ts";
import type { Transport } from "./link.ts";
import type { NodeLink } from "./nodelink.ts";

function stub(transport: Transport, ...services: string[]) {
  return {
    transport,
    latency: undefined as number | undefined,
    has: (service: string) => services.includes(service),
  };
}

function link(peerId: string, call: () => Promise<unknown>) {
  return {
    transport: "port" as Transport,
    latency: undefined as number | undefined,
    peerId,
    has: (service: string) => service === "math",
    call,
  } as unknown as NodeLink;
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
      () => balancer.invoke("nope", "add", [], 10, 8, undefined, undefined),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("invoke retries the next link when a peer misses the service", async () => {
    const balancer = new Balancer();
    const stale = link("stale", async () => {
      throw new SwarmError("not-found", "service not found: math");
    });
    const healthy = link("healthy", async () => 7);

    assert.equal(
      await balancer.invoke(
        "math",
        "add",
        [1, 2],
        10,
        8,
        undefined,
        undefined,
        [stale, healthy],
      ),
      7,
    );
  });

  test("invoke does not retry the next link when a call is rejected", async () => {
    const balancer = new Balancer();
    let calls = 0;
    const rejecting = link("rejecting", async () => {
      calls += 1;
      throw new SwarmError("rejected", "bad arguments");
    });
    const healthy = link("healthy", async () => 7);

    await assert.rejects(
      balancer.invoke("math", "add", [], 10, 8, undefined, undefined, [
        rejecting,
        healthy,
      ]),
      (err: SwarmError) => err.kind === "rejected",
    );
    assert.equal(calls, 1);
  });

  test("invoke retries the next link when a call times out", async () => {
    const balancer = new Balancer();
    const slow = link("slow", async () => {
      throw new SwarmError("timeout", "call timed out: math.add");
    });
    const healthy = link("healthy", async () => 7);

    assert.equal(
      await balancer.invoke(
        "math",
        "add",
        [1, 2],
        10,
        8,
        undefined,
        undefined,
        [slow, healthy],
      ),
      7,
    );
  });

  test("invoke passes a reduced timeout to retried calls", async () => {
    const balancer = new Balancer();
    const seen: number[] = [];
    const slow = {
      ...link("slow", async () => {
        throw new SwarmError("timeout", "call timed out: math.add");
      }),
      call: (...args: unknown[]) => {
        seen.push(args[4] as number);
        return new Promise<never>((_resolve, reject) => {
          setTimeout(
            () => reject(new SwarmError("timeout", "call timed out: math.add")),
            20,
          );
        });
      },
    } as unknown as NodeLink;
    const healthy = {
      ...link("healthy", async () => 7),
      call: (...args: unknown[]) => {
        seen.push(args[4] as number);
        return Promise.resolve(7);
      },
    } as unknown as NodeLink;

    await balancer.invoke(
      "math",
      "add",
      [1, 2],
      1000,
      8,
      undefined,
      undefined,
      [slow, healthy],
    );

    assert.equal(seen.length, 2);
    assert.ok(seen[0] <= 1000);
    assert.ok(seen[1] < seen[0]);
  });

  test("invoke stops retrying when the timeout budget is exhausted", async () => {
    const balancer = new Balancer();
    let calls = 0;
    const slow = link(
      "slow",
      () =>
        new Promise<never>((_resolve, reject) => {
          calls += 1;
          setTimeout(
            () => reject(new SwarmError("timeout", "call timed out: math.add")),
            50,
          );
        }),
    );
    const healthy = link("healthy", async () => {
      calls += 1;
      return 7;
    });

    await assert.rejects(
      balancer.invoke("math", "add", [], 20, 8, undefined, undefined, [
        slow,
        healthy,
      ]),
      (err: SwarmError) => err.kind === "timeout",
    );
    assert.equal(calls, 1);
  });

  test("invoke rejects with timeout immediately when no budget remains", async () => {
    const balancer = new Balancer();
    let calls = 0;
    const slow = link("slow", async () => {
      calls += 1;
      throw new SwarmError("timeout", "call timed out: math.add");
    });
    const healthy = link("healthy", async () => {
      calls += 1;
      return 7;
    });

    await assert.rejects(
      balancer.invoke("math", "add", [], 0, 8, undefined, undefined, [
        slow,
        healthy,
      ]),
      (err: SwarmError) => err.kind === "timeout",
    );
    assert.equal(calls, 0);
  });
});
