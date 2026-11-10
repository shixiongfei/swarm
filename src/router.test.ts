/*
 * router.test.ts
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
import { NodeLocal } from "./nodelocal.ts";
import { Router } from "./router.ts";
import type { SwarmError } from "./errors.ts";
import type { NodeLink } from "./nodelink.ts";
import type { Instance } from "./registry.ts";

type Call = {
  peer: string;
  service: string;
  method: string;
  args: unknown[];
  timeout: number;
  ttl: number | undefined;
  target: string | undefined;
};

function fakeLink(
  peerId: string,
  services: string[],
  result: (call: Call) => unknown = () => "ok",
) {
  const calls: Call[] = [];

  const link = {
    peerId,
    transport: "tcp" as const,
    latency: 1,
    calls,
    has(service: string, target?: string) {
      return (
        services.includes(service) &&
        (target === undefined || target === peerId)
      );
    },
    async call(
      peer: string,
      service: string,
      method: string,
      args: unknown[],
      timeout: number,
      ttl: number | undefined,
      target: string | undefined,
    ) {
      const call = { peer, service, method, args, timeout, ttl, target };

      calls.push(call);

      return result(call);
    },
  };

  return link as unknown as NodeLink & { calls: Call[] };
}

function makeRouter(options: {
  local?: NodeLocal;
  links?: NodeLink[];
  hubLinks?: NodeLink[];
  instances?: Instance[];
  maxHops?: number;
  callTimeout?: number;
}) {
  const local = options.local ?? new NodeLocal("self");

  const router = new Router({
    nodeId: "self",
    local,
    maxHops: options.maxHops !== undefined ? options.maxHops : 8,
    callTimeout: options.callTimeout !== undefined ? options.callTimeout : 1000,
    links: () => options.links ?? [],
    hubLinks: () => options.hubLinks ?? [],
    instances: () => options.instances ?? [],
  });

  return { router, local };
}

describe("Router", () => {
  test("invokes a local service when no link serves it", async () => {
    const local = new NodeLocal("self");

    local.register("math", { add: (a: number, b: number) => a + b });

    const { router } = makeRouter({ local, links: [fakeLink("peer", [])] });

    assert.equal(await router.invoke("math", "add", [1, 2]), 3);
  });

  test("targets this node without touching links", async () => {
    const local = new NodeLocal("self");

    local.register("math", { add: () => 7 });

    const link = fakeLink("peer", ["math"]);
    const { router } = makeRouter({ local, links: [link] });

    assert.equal(
      await router.invoke(
        "math",
        "add",
        [],
        undefined,
        undefined,
        undefined,
        "self",
      ),
      7,
    );
    assert.equal(link.calls.length, 0);
  });

  test("throws not-found when nothing serves the service", () => {
    const { router } = makeRouter({});

    assert.throws(
      () => router.invoke("nope", "add", []),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("throws not-found when the hop budget is exhausted", () => {
    const link = fakeLink("peer", ["math"]);
    const { router } = makeRouter({ links: [link] });

    assert.throws(
      () => router.invoke("math", "add", [], undefined, 0),
      (err: SwarmError) => err.kind === "not-found",
    );
    assert.equal(link.calls.length, 0);
  });

  test("skips the link a call came from", async () => {
    const source = fakeLink("a", ["math"]);
    const other = fakeLink("b", ["math"]);
    const { router } = makeRouter({ links: [source, other] });

    await router.invoke("math", "add", [], undefined, undefined, source);

    assert.equal(source.calls.length, 0);
    assert.equal(other.calls.length, 1);
  });

  test("routes to the link that serves the target node", async () => {
    const a = fakeLink("a", ["math"]);
    const b = fakeLink("b", ["math"]);
    const { router } = makeRouter({ links: [a, b] });

    await router.invoke(
      "math",
      "add",
      [],
      undefined,
      undefined,
      undefined,
      "b",
    );

    assert.equal(a.calls.length, 0);
    assert.equal(b.calls.length, 1);
    assert.equal(b.calls[0].target, "b");
  });

  test("collects local and remote instances and isolates failures", async () => {
    const local = new NodeLocal("self");
    const failure = new Error("boom");

    local.register("math", {
      add: () => {
        throw failure;
      },
    });

    const remote = fakeLink("remote", ["math"], (call) =>
      (call.args as number[]).reduce((sum, value) => sum + value, 0),
    );

    const { router } = makeRouter({
      local,
      links: [remote],
      instances: [
        { nodeId: "self", service: "math" },
        { nodeId: "remote", service: "math" },
      ],
    });

    const results = await router.collect("math", "add", [1, 2]);

    assert.deepEqual(results, [
      { nodeId: "self", ok: false, error: failure },
      { nodeId: "remote", ok: true, value: 3 },
    ]);
  });

  test("collect throws not-found when no instance serves the service", () => {
    const { router } = makeRouter({});

    assert.throws(
      () => router.collect("missing", "add", []),
      (err: SwarmError) => err.kind === "not-found",
    );
  });

  test("rejects a call whose signal already aborted", async () => {
    const local = new NodeLocal("self");

    local.register("math", { add: () => 1 });

    const { router } = makeRouter({ local });
    const controller = new AbortController();

    controller.abort();

    await assert.rejects(
      router.invoke(
        "math",
        "add",
        [],
        undefined,
        undefined,
        undefined,
        undefined,
        controller.signal,
      ),
      (err: SwarmError) => err.kind === "cancelled",
    );
  });
});
