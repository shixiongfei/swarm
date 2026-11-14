/*
 * address.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  isAdvertisable,
  isLoopback,
  normalizePath,
  parseUrl,
} from "./address.ts";
import type { SwarmError } from "./errors.ts";

test("isLoopback matches loopback hosts only", () => {
  for (const host of [
    "localhost",
    "127.0.0.1",
    "127.1.2.3",
    "::1",
    "::ffff:127.0.0.1",
  ]) {
    assert.equal(isLoopback(host), true, host);
  }

  for (const host of ["0.0.0.0", "::", "192.168.0.9", "10.0.0.1", undefined]) {
    assert.equal(isLoopback(host), false, String(host));
  }
});

test("isAdvertisable rejects wildcard hosts only", () => {
  for (const host of ["0.0.0.0", "::", "::ffff:0:0"]) {
    assert.equal(isAdvertisable(host), false, host);
  }

  for (const host of ["127.0.0.1", "::1", "192.168.0.9"]) {
    assert.equal(isAdvertisable(host), true, host);
  }
});

test("parseUrl normalizes tcp, ws, and wss urls", () => {
  assert.deepEqual(parseUrl("tcp://127.0.0.1:9000"), {
    protocol: "tcp:",
    secure: false,
    host: "127.0.0.1",
    port: 9000,
    portOmitted: false,
    path: "/",
    secret: "",
  });

  assert.deepEqual(parseUrl("ws://example.com/ws"), {
    protocol: "ws:",
    secure: false,
    host: "example.com",
    port: 80,
    portOmitted: true,
    path: "/ws",
    secret: "",
  });

  assert.deepEqual(parseUrl("wss://[::1]:9443/a/b/"), {
    protocol: "ws:",
    secure: true,
    host: "::1",
    port: 9443,
    portOmitted: false,
    path: "/a/b",
    secret: "",
  });

  assert.equal(parseUrl("tcp://user:p%40ss@127.0.0.1:9000").secret, "p@ss");
});

test("parseUrl rejects malformed urls and secrets", () => {
  const rejects = (url: string, message: string) => {
    assert.throws(
      () => parseUrl(url),
      (err: SwarmError) => err.kind === "rejected" && err.message === message,
      url,
    );
  };

  rejects("not a url", "invalid url");
  rejects("http://127.0.0.1:1", "invalid url: http://127.0.0.1:1");
  rejects("tcp://127.0.0.1", "invalid url: tcp://127.0.0.1");
  rejects("tcp://127.0.0.1:1/path", "tcp url does not support a path");
  rejects("tcp://user:%E0%A4%A@127.0.0.1:1", "invalid secret");
});

test("normalizePath adds a leading slash and trims a trailing one", () => {
  assert.equal(normalizePath(undefined), "/");
  assert.equal(normalizePath(""), "/");
  assert.equal(normalizePath("/"), "/");
  assert.equal(normalizePath("ws"), "/ws");
  assert.equal(normalizePath("/ws"), "/ws");
  assert.equal(normalizePath("/ws/"), "/ws");
});
