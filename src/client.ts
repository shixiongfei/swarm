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
import { rethrowAsync } from "./events.ts";
import { assertTimeout } from "./timeout.ts";

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
  signal?: AbortSignal;
};

const defaultConnectTimeout = 15000;

export abstract class Client<S = any> {
  readonly events = eventlite<ClientEvents<S>>({
    allowDuplicate: true,
    isolate: { onError: rethrowAsync },
  });

  socket: S | undefined;

  private readonly reconnectEnabled: boolean;
  private readonly retryDelay: number;
  private readonly maxRetryDelay: number;
  private readonly connectTimeout: number;
  private readonly signal: AbortSignal | undefined;
  private readonly onAbort = () => this.close();

  private timer: ReturnType<typeof setTimeout> | undefined;
  private attempt = 0;
  private onlineAt = 0;
  private connecting = false;
  private closed = false;
  private abandoned = false;
  private inflight: Promise<void> | undefined;

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
    this.signal = options.signal;

    assertTimeout(this.connectTimeout);

    if (
      !Number.isFinite(this.retryDelay) ||
      this.retryDelay <= 0 ||
      !Number.isFinite(this.maxRetryDelay) ||
      this.maxRetryDelay < this.retryDelay
    ) {
      throw new SwarmError(
        "rejected",
        "INVALID_TIMEOUT",
        "invalid retry delay",
      );
    }

    if (this.signal !== undefined) {
      this.signal.addEventListener("abort", this.onAbort, { once: true });
    }
  }

  protected get isClosed() {
    return this.closed;
  }

  async connect(): Promise<this> {
    if (this.signal !== undefined && this.signal.aborted) {
      this.close();

      throw connectCancelled();
    }

    if (this.closed) {
      throw new SwarmError("rejected", "CLOSED", "client is closed", false);
    }

    if (this.connecting || this.inflight !== undefined) {
      throw new SwarmError("rejected", "REJECTED", "client is connecting");
    }

    if (this.socket !== undefined) {
      return this;
    }

    this.clearTimer();
    this.connecting = true;
    this.abandoned = false;

    let timer: ReturnType<typeof setTimeout> | undefined;

    try {
      const opening = this.open();
      const inflight = opening.then(
        () => undefined,
        () => undefined,
      );

      this.inflight = inflight;

      void inflight.then(() => {
        if (this.inflight !== inflight) {
          return;
        }

        this.inflight = undefined;

        if (!this.abandoned) {
          return;
        }

        this.abandoned = false;

        if (this.socket !== undefined) {
          this.destroy(this.socket);
          this.socket = undefined;
        }

        this.scheduleReconnect();
      });

      await Promise.race([
        opening,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            this.abandoned = true;

            if (this.socket !== undefined) {
              this.destroy(this.socket);
              this.socket = undefined;
            }

            reject(new SwarmError("timeout", "TIMEOUT", "connect timed out"));
          }, this.connectTimeout);
        }),
      ]);
    } catch (err) {
      if (this.signal !== undefined && this.signal.aborted) {
        throw connectCancelled();
      }

      if (!this.abandoned) {
        this.scheduleReconnect();
      }

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

    if (this.signal !== undefined) {
      this.signal.removeEventListener("abort", this.onAbort);
    }

    if (this.socket !== undefined) {
      this.destroy(this.socket);
    }
  }

  protected markOnline(socket: S) {
    if (this.closed || this.abandoned) {
      this.destroy(socket);

      if (this.socket === socket) {
        this.socket = undefined;
      }

      return;
    }

    this.socket = socket;
    this.onlineAt = Date.now();
    this.events.emit("online", socket);
  }

  protected markOffline(socket: S) {
    if (this.socket !== socket) {
      return;
    }

    this.socket = undefined;

    if (Date.now() - this.onlineAt >= this.backoff()) {
      this.attempt = 0;
    }

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

    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.connect().catch(() => this.scheduleReconnect());
    }, this.backoff());
  }

  private backoff() {
    return Math.min(
      this.retryDelay * 2 ** (Math.max(this.attempt, 1) - 1),
      this.maxRetryDelay,
    );
  }

  private clearTimer() {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }
}

function connectCancelled() {
  return new SwarmError(
    "cancelled",
    "CANCELLED",
    "connect was cancelled",
    false,
  );
}
