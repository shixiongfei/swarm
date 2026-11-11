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
  cancel?: () => void;
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
      const entry: Entry = { resolve, reject };

      this.entries.set(id, entry);

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
        const onAbort = () => {
          const current = this.entries.get(id);

          if (current === undefined) {
            return;
          }

          this.entries.delete(id);
          current.reject(new SwarmError("cancelled", "call was cancelled"));
        };

        signal.addEventListener("abort", onAbort, { once: true });

        entry.cancel = () => signal.removeEventListener("abort", onAbort);
      }
    });

    return withTimeout(reply, timeout, timeoutError, () => {
      this.take(id);
    });
  }

  settle(id: string, value: unknown) {
    const entry = this.take(id);

    if (entry === undefined) {
      return;
    }

    entry.resolve(value);
  }

  reject(id: string, error: SwarmError) {
    const entry = this.take(id);

    if (entry === undefined) {
      return;
    }

    entry.reject(error);
  }

  clear(error: () => SwarmError) {
    if (this.entries.size === 0) {
      return;
    }

    const failure = error();

    for (const entry of this.entries.values()) {
      if (entry.cancel !== undefined) {
        entry.cancel();
      }

      entry.reject(failure);
    }

    this.entries.clear();
  }

  private take(id: string) {
    const entry = this.entries.get(id);

    if (entry === undefined) {
      return undefined;
    }

    this.entries.delete(id);

    if (entry.cancel !== undefined) {
      entry.cancel();
    }

    return entry;
  }
}
