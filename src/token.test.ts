/*
 * token.test.ts
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
import { signToken, verifyToken } from "./token.ts";

describe("token", () => {
  test("accepts a token it signed", async () => {
    const token = await signToken("s3cret", "peer");

    assert.equal(await verifyToken(token, "s3cret", "peer"), true);
  });

  test("rejects a token for another node", async () => {
    const token = await signToken("s3cret", "peer");

    assert.equal(await verifyToken(token, "s3cret", "other"), false);
  });

  test("rejects a token signed with another secret", async () => {
    const token = await signToken("s3cret", "peer");

    assert.equal(await verifyToken(token, "wrong-secret", "peer"), false);
  });

  test("rejects a tampered token", async () => {
    const token = await signToken("s3cret", "peer");
    const [header, payload, signature] = token.split(".");
    const forged = `${header}.${payload}x.${signature}`;

    assert.equal(await verifyToken(forged, "s3cret", "peer"), false);
  });

  test("rejects a malformed token", async () => {
    assert.equal(await verifyToken(undefined, "s3cret", "peer"), false);
    assert.equal(await verifyToken("", "s3cret", "peer"), false);
    assert.equal(await verifyToken("a.b", "s3cret", "peer"), false);
    assert.equal(await verifyToken("a.b.c", "s3cret", "peer"), false);
  });

  test("rejects an expired token", async () => {
    const token = await signToken("s3cret", "peer");
    const now = Date.now;

    Date.now = () => now() + 120 * 1000;

    try {
      assert.equal(await verifyToken(token, "s3cret", "peer"), false);
    } finally {
      Date.now = now;
    }
  });
});
