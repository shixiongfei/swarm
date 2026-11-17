/*
 * index.test.ts
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
import { createNode } from "./index.ts";

describe("createNode", () => {
  test("returns a fresh node for every call", (t) => {
    const first = createNode({ cluster: "same-cluster" });
    const second = createNode({ cluster: "same-cluster" });

    t.after(() => {
      first.close();
      second.close();
    });

    assert.notEqual(first, second);
  });

  test("defaults to the default cluster", (t) => {
    const node = createNode();

    t.after(() => node.close());

    assert.equal(node.cluster, "default");
  });

  test("passes cluster and secret through", (t) => {
    const node = createNode({ cluster: "secret-cluster", secret: "s3cret" });

    t.after(() => node.close());

    assert.equal(node.cluster, "secret-cluster");
    assert.equal(node.secret, "s3cret");
  });

  test("closing one node does not close another", (t) => {
    const first = createNode({ cluster: "shared-cluster" });
    const second = createNode({ cluster: "shared-cluster" });

    t.after(() => second.close());

    first.close();

    assert.equal(second.cluster, "shared-cluster");
  });
});
