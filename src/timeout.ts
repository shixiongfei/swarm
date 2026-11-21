/*
 * timeout.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { SwarmError } from "./errors.ts";

export function isTimeout(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

export function assertTimeout(timeout: number): void {
  if (!isTimeout(timeout)) {
    throw new SwarmError(
      "rejected",
      "INVALID_TIMEOUT",
      `invalid timeout: ${timeout}`,
      false,
    );
  }
}

export function withCancel<T>(
  promise: Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  if (signal === undefined) {
    return promise;
  }

  if (signal.aborted) {
    return Promise.reject(
      new SwarmError("cancelled", "CANCELLED", "call was cancelled"),
    );
  }

  return new Promise((resolve, reject) => {
    const onAbort = () =>
      reject(new SwarmError("cancelled", "CANCELLED", "call was cancelled"));

    signal.addEventListener("abort", onAbort, { once: true });

    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

export function withTimeout<T>(
  value: T | Promise<T>,
  timeout: number,
  error: () => SwarmError,
  onTimeout?: () => void,
) {
  return new Promise<Awaited<T>>((resolve, reject) => {
    const timer = setTimeout(() => {
      if (onTimeout !== undefined) {
        onTimeout();
      }

      reject(error());
    }, timeout);

    Promise.resolve(value).then(
      (result) => {
        clearTimeout(timer);
        resolve(result);
      },
      (cause) => {
        clearTimeout(timer);
        reject(cause);
      },
    );
  });
}
