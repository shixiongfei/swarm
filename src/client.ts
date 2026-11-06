/*
 * client.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { eventlite } from "eventlite.js";
import { SwarmError } from "./errors.ts";

export type ClientEvents<S = any> = {
  online: (socket: S) => void;
  offline: (socket: S) => void;
  retry: (attempt: number, delay: number, error: Error | undefined) => void;
};

export type ClientOptions = {
  reconnect?: boolean;
  retryDelay?: number;
  maxRetryDelay?: number;
};

export abstract class Client<S = any> {
  readonly events = eventlite<ClientEvents<S>>({
    allowDuplicate: true,
    isolate: true,
  });

  socket: S | undefined;

  protected lastError: Error | undefined;

  private readonly reconnectEnabled: boolean;
  private readonly retryDelay: number;
  private readonly maxRetryDelay: number;

  private timer: ReturnType<typeof setTimeout> | undefined;
  private attempt = 0;
  private connecting = false;
  private closed = false;

  constructor(options: ClientOptions = {}) {
    this.reconnectEnabled = options.reconnect ?? true;
    this.retryDelay = options.retryDelay ?? 1000;
    this.maxRetryDelay = options.maxRetryDelay ?? 30000;
  }

  protected get isClosed() {
    return this.closed;
  }

  async connect(): Promise<this> {
    if (this.closed) {
      throw new SwarmError("rejected", "client is closed");
    }

    if (this.connecting) {
      throw new SwarmError("rejected", "client is connecting");
    }

    if (this.socket !== undefined) {
      return this;
    }

    this.clearTimer();
    this.connecting = true;

    try {
      await this.open();
    } finally {
      this.connecting = false;
    }

    return this;
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.clearTimer();

    if (this.socket !== undefined) {
      this.destroy(this.socket);
    }
  }

  protected markOnline(socket: S) {
    this.socket = socket;
    this.lastError = undefined;
    this.attempt = 0;
    this.events.emit("online", socket);
  }

  protected markOffline(socket: S) {
    if (this.socket !== socket) {
      return;
    }

    this.socket = undefined;
    this.events.emit("offline", socket);
    this.scheduleReconnect();
  }

  protected abstract open(): Promise<void>;
  protected abstract destroy(socket: S): void;

  private scheduleReconnect() {
    if (this.closed || !this.reconnectEnabled || this.timer !== undefined) {
      return;
    }

    this.attempt += 1;

    const delay = Math.min(
      this.retryDelay * 2 ** (this.attempt - 1),
      this.maxRetryDelay,
    );

    this.events.emit("retry", this.attempt, delay, this.lastError);
    this.lastError = undefined;

    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.connect().catch(() => this.scheduleReconnect());
    }, delay);
  }

  private clearTimer() {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }
}
