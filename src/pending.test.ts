/*
 * pending.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { describe, test } from "node:test";
import { PendingCalls } from "./pending.ts";
import { SwarmError } from "./errors.ts";
import type { SwarmError as SwarmErrorType } from "./errors.ts";

function makePending() {
  const ids: string[] = [];
  const pending = new PendingCalls();

  const send = (id: string) => {
    ids.push(id);
  };

  const timeoutError = () => new SwarmError("timeout", "call timed out");
  const sendError = (cause: unknown) =>
    new SwarmError("failed", cause instanceof Error ? cause.message : "failed");

  return { pending, ids, send, timeoutError, sendError };
}

describe("PendingCalls", () => {
  test("settles a call by id", async () => {
    const { pending, ids, send, timeoutError, sendError } = makePending();
    const reply = pending.call(send, 1000, timeoutError, sendError);

    assert.equal(pending.size, 1);

    pending.settle(ids[0], 42);

    assert.equal(await reply, 42);
    assert.equal(pending.size, 0);
  });

  test("rejects a call by id", async () => {
    const { pending, ids, send, timeoutError, sendError } = makePending();
    const reply = pending.call(send, 1000, timeoutError, sendError);
    const failure = new SwarmError("failed", "boom");

    pending.reject(ids[0], failure);

    await assert.rejects(reply, (err: SwarmErrorType) => err === failure);
    assert.equal(pending.size, 0);
  });

  test("ignores replies for unknown ids", () => {
    const { pending } = makePending();

    assert.doesNotThrow(() => pending.settle("missing", 1));
    assert.doesNotThrow(() =>
      pending.reject("missing", new SwarmError("failed", "boom")),
    );
    assert.equal(pending.size, 0);
  });

  test("rejects with the send error when send throws", async () => {
    const { pending, timeoutError, sendError } = makePending();
    const reply = pending.call(
      () => {
        throw new Error("socket is gone");
      },
      1000,
      timeoutError,
      sendError,
    );

    await assert.rejects(
      reply,
      (err: SwarmErrorType) =>
        err.kind === "failed" && err.message === "socket is gone",
    );
    assert.equal(pending.size, 0);
  });

  test("rejects without sending when the signal already aborted", async () => {
    const { pending, ids, send, timeoutError, sendError } = makePending();
    const controller = new AbortController();

    controller.abort();

    const reply = pending.call(
      send,
      1000,
      timeoutError,
      sendError,
      controller.signal,
    );

    await assert.rejects(
      reply,
      (err: SwarmErrorType) => err.kind === "cancelled",
    );
    assert.deepEqual(ids, []);
    assert.equal(pending.size, 0);
  });

  test("rejects when the signal aborts after the send", async () => {
    const { pending, ids, send, timeoutError, sendError } = makePending();
    const controller = new AbortController();
    const reply = pending.call(
      send,
      1000,
      timeoutError,
      sendError,
      controller.signal,
    );

    assert.equal(ids.length, 1);

    controller.abort();

    await assert.rejects(
      reply,
      (err: SwarmErrorType) => err.kind === "cancelled",
    );
    assert.equal(pending.size, 0);
  });

  test("times out and drops the entry", async () => {
    const { pending, send, timeoutError, sendError } = makePending();
    const reply = pending.call(send, 10, timeoutError, sendError);

    await assert.rejects(
      reply,
      (err: SwarmErrorType) => err.kind === "timeout",
    );
    assert.equal(pending.size, 0);

    pending.settle("late", 1);
    assert.equal(pending.size, 0);
  });

  test("clear rejects every pending call with the same error", async () => {
    const { pending, send, timeoutError, sendError } = makePending();
    const first = pending.call(send, 1000, timeoutError, sendError);
    const second = pending.call(send, 1000, timeoutError, sendError);
    const failure = new SwarmError("failed", "node is closed");

    assert.equal(pending.size, 2);

    pending.clear(() => failure);

    await assert.rejects(first, (err: SwarmErrorType) => err === failure);
    await assert.rejects(second, (err: SwarmErrorType) => err === failure);
    assert.equal(pending.size, 0);

    assert.doesNotThrow(() => pending.clear(() => failure));
  });

  test("removes the abort listener when the call settles", async () => {
    const { pending, ids, send, timeoutError, sendError } = makePending();
    const controller = new AbortController();
    const reply = pending.call(
      send,
      1000,
      timeoutError,
      sendError,
      controller.signal,
    );

    assert.equal(getEventListeners(controller.signal, "abort").length, 1);

    pending.settle(ids[0], 1);
    await reply;

    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  });

  test("removes the abort listener when the call is rejected", async () => {
    const { pending, ids, send, timeoutError, sendError } = makePending();
    const controller = new AbortController();
    const reply = pending.call(
      send,
      1000,
      timeoutError,
      sendError,
      controller.signal,
    );

    pending.reject(ids[0], new SwarmError("failed", "boom"));

    await assert.rejects(reply, (err: SwarmErrorType) => err.kind === "failed");
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  });

  test("removes the abort listener on timeout", async () => {
    const { pending, send, timeoutError, sendError } = makePending();
    const controller = new AbortController();
    const reply = pending.call(
      send,
      10,
      timeoutError,
      sendError,
      controller.signal,
    );

    await assert.rejects(
      reply,
      (err: SwarmErrorType) => err.kind === "timeout",
    );
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  });

  test("removes the abort listener on clear", async () => {
    const { pending, send, timeoutError, sendError } = makePending();
    const controller = new AbortController();
    const first = pending.call(
      send,
      1000,
      timeoutError,
      sendError,
      controller.signal,
    );
    const second = pending.call(
      send,
      1000,
      timeoutError,
      sendError,
      controller.signal,
    );

    assert.equal(getEventListeners(controller.signal, "abort").length, 2);

    pending.clear(() => new SwarmError("failed", "node is closed"));

    await assert.rejects(first);
    await assert.rejects(second);
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  });
});
