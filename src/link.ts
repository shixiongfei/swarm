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
  on(event: string, listener: (...args: any[]) => void): void;
  off?(event: string, listener: (...args: any[]) => void): void;
  close?(): void;
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

  private readonly onMessage = (message: unknown) => {
    if (!this.closed) {
      this.receive(message);
    }
  };

  private readonly onGone = () => {
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

    channel.on("message", this.onMessage);
    channel.on("exit", this.onGone);
    channel.on("close", this.onGone);
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
    this.channel.off?.("message", this.onMessage);
    this.channel.off?.("exit", this.onGone);
    this.channel.off?.("close", this.onGone);
    this.channel.close?.();

    for (const entry of this.pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(new MusterError("failed", "link is closed"));
    }

    this.pending.clear();
    this.notifyClose();
  }
}
