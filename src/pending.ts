/*
 * pending.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { randomId } from "./random.ts";
import { withTimeout } from "./timeout.ts";
import { SwarmError } from "./errors.ts";

type Entry = {
  resolve(value: unknown): void;
  reject(error: SwarmError): void;
};

export class PendingCalls {
  private readonly entries = new Map<string, Entry>();

  get size() {
    return this.entries.size;
  }

  call(
    send: (id: string) => void,
    timeout: number,
    timeoutError: () => SwarmError,
    sendError: (cause: unknown) => SwarmError,
    signal?: AbortSignal,
  ) {
    const id = randomId();

    const reply = new Promise<unknown>((resolve, reject) => {
      this.entries.set(id, { resolve, reject });

      if (signal !== undefined && signal.aborted) {
        this.entries.delete(id);
        reject(new SwarmError("cancelled", "call was cancelled"));
        return;
      }

      try {
        send(id);
      } catch (err) {
        this.entries.delete(id);
        reject(sendError(err));
        return;
      }

      if (signal !== undefined) {
        signal.addEventListener(
          "abort",
          () => {
            const entry = this.entries.get(id);

            if (entry === undefined) {
              return;
            }

            this.entries.delete(id);
            entry.reject(new SwarmError("cancelled", "call was cancelled"));
          },
          { once: true },
        );
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

  reject(id: string, error: SwarmError) {
    const entry = this.entries.get(id);

    if (entry === undefined) {
      return;
    }

    this.entries.delete(id);
    entry.reject(error);
  }

  clear(error: () => SwarmError) {
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
