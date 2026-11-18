/*
 * events.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

export function rethrowAsync(error: unknown) {
  queueMicrotask(() => {
    throw error;
  });
}
