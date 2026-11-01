/*
 * timeout.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import type { MusterError } from "./errors.ts";

export function withTimeout<T>(
  value: T | Promise<T>,
  timeout: number,
  error: () => MusterError,
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
