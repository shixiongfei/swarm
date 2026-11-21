/*
 * object.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { deepFreeze, isObject, isPlainObject } from "./object.ts";

test("isPlainObject tells plain objects from everything else", () => {
  class Point {
    x = 1;
  }

  assert.equal(isPlainObject({}), true);
  assert.equal(isPlainObject(Object.create(null)), true);
  assert.equal(isPlainObject(new Point()), false);
  assert.equal(isPlainObject([]), false);
  assert.equal(isPlainObject(new Date()), false);
  assert.equal(isObject(null), false);
  assert.equal(
    isObject(() => 0),
    false,
  );
  assert.equal(isObject({}), true);
});

test("deepFreeze freezes nested values and returns the same reference", () => {
  const value = { nested: { list: [{ v: 1 }] } };

  assert.equal(deepFreeze(value), value);
  assert.equal(Object.isFrozen(value), true);
  assert.equal(Object.isFrozen(value.nested), true);
  assert.equal(Object.isFrozen(value.nested.list), true);
  assert.equal(Object.isFrozen(value.nested.list[0]), true);
});

test("deepFreeze tolerates cycles and leaves dates and views mutable", () => {
  const date = new Date(0);
  const view = new Uint8Array([1]);
  const cyclic: Record<string, unknown> = { date, view };

  cyclic.self = cyclic;

  assert.doesNotThrow(() => deepFreeze(cyclic));
  assert.equal(Object.isFrozen(cyclic), true);
  assert.equal(Object.isFrozen(date), false);
  assert.equal(Object.isFrozen(view), false);

  date.setTime(1);
  view[0] = 2;

  assert.equal(date.getTime(), 1);
  assert.equal(view[0], 2);
});

test("deepFreeze leaves class instances alone", () => {
  class Point {
    x = 1;
  }

  const point = new Point();
  const value = { point };

  deepFreeze(value);

  assert.equal(Object.isFrozen(value), true);
  assert.equal(Object.isFrozen(point), false);

  point.x = 2;

  assert.equal(point.x, 2);
});
