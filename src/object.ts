/*
 * object.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

export function isObject(value: unknown): value is object {
  return value !== null && typeof value === "object";
}

export function isPlainObject(value: object): boolean {
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

export function deepFreeze<T>(value: T, seen = new WeakSet<object>()): T {
  if (!isObject(value) || seen.has(value)) {
    return value;
  }

  if (!Array.isArray(value) && !isPlainObject(value)) {
    return value;
  }

  seen.add(value);

  for (const key of Object.keys(value)) {
    deepFreeze((value as Record<string, unknown>)[key], seen);
  }

  return Object.freeze(value);
}
