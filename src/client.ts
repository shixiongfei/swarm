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
};

export type ClientOptions = {
  reconnect?: boolean;
  retryDelay?: number;
  maxRetryDelay?: number;
  connectTimeout?: number;
  secret?: string;
};

const defaultConnectTimeout = 15000;

export abstract class Client<S = any> {
  readonly events = eventlite<ClientEvents<S>>({
    allowDuplicate: true,
    isolate: {
      onError: (error) => {
        queueMicrotask(() => {
          throw error;
        });
      },
    },
  });

  socket: S | undefined;

  private readonly reconnectEnabled: boolean;
  private readonly retryDelay: number;
  private readonly maxRetryDelay: number;
  private readonly connectTimeout: number;

  private timer: ReturnType<typeof setTimeout> | undefined;
  private attempt = 0;
  private connecting = false;
  private closed = false;

  constructor(options: ClientOptions = {}) {
    this.reconnectEnabled =
      options.reconnect !== undefined ? options.reconnect : true;
    this.retryDelay =
      options.retryDelay !== undefined ? options.retryDelay : 1000;
    this.maxRetryDelay =
      options.maxRetryDelay !== undefined ? options.maxRetryDelay : 30000;
    this.connectTimeout =
      options.connectTimeout !== undefined
        ? options.connectTimeout
        : defaultConnectTimeout;
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

    let timer: ReturnType<typeof setTimeout> | undefined;

    try {
      const opening = this.open();

      // a late rejection after the timeout must not become unhandled
      void opening.catch(() => {});

      await Promise.race([
        opening,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            if (this.socket !== undefined) {
              this.destroy(this.socket);
              this.socket = undefined;
            }

            reject(new SwarmError("timeout", "connect timed out"));
          }, this.connectTimeout);
        }),
      ]);
    } catch (err) {
      this.scheduleReconnect();
      throw err;
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer);
      }

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
