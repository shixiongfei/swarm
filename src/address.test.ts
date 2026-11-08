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
import { isAdvertisable, isLoopback } from "./address.ts";

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
