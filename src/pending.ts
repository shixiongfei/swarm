/*
 * pending.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { randomId } from "./random.ts";
import { withTimeout } from "./timeout.ts";
import type { MusterError } from "./errors.ts";

type Entry = {
  resolve(value: unknown): void;
  reject(error: MusterError): void;
};

export class PendingCalls {
  private readonly entries = new Map<string, Entry>();

  call(
    send: (id: string) => void,
    timeout: number,
    timeoutError: () => MusterError,
    sendError: (cause: unknown) => MusterError,
  ) {
    const id = randomId();

    const reply = new Promise<unknown>((resolve, reject) => {
      this.entries.set(id, { resolve, reject });

      try {
        send(id);
      } catch (err) {
        this.entries.delete(id);
        reject(sendError(err));
      }
    });

    return withTimeout(reply, timeout, timeoutError, () => {
      this.entries.delete(id);
    });
  }

  settle(id: string, value: unknown) {
    const entry = this.entries.get(id);

    if (entry === undefined) {
      return;
    }

    this.entries.delete(id);
    entry.resolve(value);
  }

  reject(id: string, error: MusterError) {
    const entry = this.entries.get(id);

    if (entry === undefined) {
      return;
    }

    this.entries.delete(id);
    entry.reject(error);
  }

  clear(error: () => MusterError) {
    if (this.entries.size === 0) {
      return;
    }

    const failure = error();

    for (const entry of this.entries.values()) {
      entry.reject(failure);
    }

    this.entries.clear();
  }
}
