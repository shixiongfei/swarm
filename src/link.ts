/*
 * link.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { MusterError } from "./errors.ts";
import type { ErrorKind } from "./errors.ts";

export type Channel = {
  postMessage(value: unknown): void;
  addEventListener(event: string, listener: (...args: any[]) => void): void;
  removeEventListener(event: string, listener: (...args: any[]) => void): void;
  start(): void;
  close(): void;
};

type Pending = {
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
};

export class Link {
  private readonly channel: Channel;
  private readonly receive: (message: unknown) => void;
  private readonly notifyClose: () => void;
  private readonly pending = new Map<string, Pending>();
  private closed = false;

  private readonly onMessage = (message: { data?: unknown }) => {
    if (!this.closed) {
      this.receive(message.data);
    }
  };

  private readonly onClose = () => {
    this.close();
  };

  constructor(
    channel: Channel,
    handlers: {
      receive: (message: unknown) => void;
      close: () => void;
    },
  ) {
    this.channel = channel;
    this.receive = handlers.receive;
    this.notifyClose = handlers.close;

    channel.addEventListener("message", this.onMessage);
    channel.addEventListener("close", this.onClose);
    channel.start();
  }

  send(message: unknown) {
    if (this.closed) {
      return false;
    }

    this.channel.postMessage(message);
    return true;
  }

  request(id: string, message: unknown, timeout: number) {
    if (this.closed) {
      return Promise.reject(new MusterError("failed", "link is closed"));
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new MusterError("timeout", `request timed out: ${id}`));
      }, timeout);

      this.pending.set(id, { resolve, reject, timer });
      this.send(message);
    });
  }

  resolve(id: string, value: unknown) {
    const entry = this.pending.get(id);

    if (entry === undefined) {
      return;
    }

    this.pending.delete(id);
    clearTimeout(entry.timer);
    entry.resolve(value);
  }

  reject(id: string, kind: ErrorKind, message: string) {
    const entry = this.pending.get(id);

    if (entry === undefined) {
      return;
    }

    this.pending.delete(id);
    clearTimeout(entry.timer);
    entry.reject(new MusterError(kind, message));
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.channel.removeEventListener("message", this.onMessage);
    this.channel.removeEventListener("close", this.onClose);
    this.channel.close();

    for (const entry of this.pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(new MusterError("failed", "link is closed"));
    }

    this.pending.clear();
    this.notifyClose();
  }
}
