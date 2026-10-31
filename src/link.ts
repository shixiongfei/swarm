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

type Channel = {
  postMessage(value: unknown): void;
  on(event: string, listener: (...args: any[]) => void): void;
  off?(event: string, listener: (...args: any[]) => void): void;
  close?(): void;
};

export class Link {
  private readonly channel: Channel;
  private readonly receive: (message: unknown) => void;
  private readonly notifyClose: () => void;
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

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.channel.off?.("message", this.onMessage);
    this.channel.off?.("exit", this.onGone);
    this.channel.off?.("close", this.onGone);
    this.channel.close?.();
    this.notifyClose();
  }
}
