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
import { createNode, Node } from "./index.ts";

describe("createNode", () => {
  test("returns the same node for the same cluster", () => {
    const first = createNode({ cluster: "same-cluster" });
    const second = createNode({ cluster: "same-cluster" });

    assert.equal(first, second);
  });

  test("defaults to the default cluster", () => {
    const node = createNode();

    assert.equal(node.cluster, "default");
  });

  test("keeps clusters separate", () => {
    const first = createNode({ cluster: "cluster-a" });
    const second = createNode({ cluster: "cluster-b" });

    assert.notEqual(first, second);
    assert.equal(first.cluster, "cluster-a");
    assert.equal(second.cluster, "cluster-b");
  });

  test("passes secret through", () => {
    const node = createNode({ cluster: "secret-cluster", secret: "s3cret" });

    assert.equal(node.secret, "s3cret");

    node.close();
  });

  test("close evicts the cluster so a new node is created", () => {
    const first = createNode({ cluster: "evict-cluster" });
    first.close();

    const second = createNode({ cluster: "evict-cluster" });

    assert.notEqual(second, first);
    assert.equal(second.cluster, "evict-cluster");
  });

  test("closing another node does not evict the cached one", () => {
    const cached = createNode({ cluster: "shared-cluster" });
    const other = new Node({ cluster: "shared-cluster" });

    other.close();

    assert.equal(createNode({ cluster: "shared-cluster" }), cached);
  });
});
