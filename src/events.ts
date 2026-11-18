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

export type ErrorEvents = {
  listenerCount(event: "error"): number;
  emit(event: "error", error: Error): void;
};

export function reportError(events: ErrorEvents, error: unknown) {
  const err = error instanceof Error ? error : new Error(String(error));

  if (events.listenerCount("error") === 0) {
    rethrowAsync(err);
    return;
  }

  events.emit("error", err);
}
