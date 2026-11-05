/*
 * tcpclient.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { eventlite } from "eventlite.js";
import { connect as netConnect } from "node:net";
import { MusterError } from "./errors.ts";
import type { Socket } from "node:net";

export type TcpClientEvents = {
  online: (socket: Socket) => void;
  offline: (socket: Socket) => void;
  retry: (attempt: number, delay: number, error: Error | undefined) => void;
};

export type TcpClientOptions = {
  reconnect?: boolean;
  retryDelay?: number;
  maxRetryDelay?: number;
};

export class TcpClient {
  readonly events = eventlite<TcpClientEvents>({
    allowDuplicate: true,
    isolate: true,
  });

  socket: Socket | undefined;

  private readonly binding: { host: string; port: number };
  private readonly reconnectEnabled: boolean;
  private readonly retryDelay: number;
  private readonly maxRetryDelay: number;

  private timer: ReturnType<typeof setTimeout> | undefined;
  private lastError: Error | undefined;
  private attempt = 0;
  private connecting = false;
  private closed = false;

  constructor(host: string, port: number, options: TcpClientOptions = {}) {
    this.binding = { host, port };
    this.reconnectEnabled = options.reconnect ?? true;
    this.retryDelay = options.retryDelay ?? 1000;
    this.maxRetryDelay = options.maxRetryDelay ?? 30000;
  }

  get connected() {
    return this.socket?.readyState === "open";
  }

  async connect() {
    if (this.closed) {
      throw new MusterError("rejected", "tcp client is closed");
    }

    if (this.connecting) {
      throw new MusterError("rejected", "tcp client is connecting");
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
    this.socket?.destroy();
  }

  private open() {
    return new Promise<void>((resolve, reject) => {
      const socket = netConnect(this.binding);

      this.socket = socket;
      this.lastError = undefined;

      const onError = (error: Error) => {
        this.clearSocket(socket);
        socket.destroy();

        reject(
          this.closed
            ? new MusterError("rejected", "tcp client is closed")
            : new MusterError("failed", error.message),
        );
      };

      const onClose = () => {
        this.clearSocket(socket);

        reject(
          this.closed
            ? new MusterError("rejected", "tcp client is closed")
            : new MusterError("failed", "connection closed before connect"),
        );
      };

      socket.once("error", onError);
      socket.once("close", onClose);
      socket.once("connect", () => {
        socket.off("error", onError);
        socket.off("close", onClose);
        this.attach(socket);
        resolve();
      });
    });
  }

  private attach(socket: Socket) {
    this.lastError = undefined;
    this.attempt = 0;

    socket.on("error", (error) => {
      this.lastError = error;
      socket.destroy();
    });

    socket.once("close", () => {
      if (this.socket !== socket) {
        return;
      }

      this.socket = undefined;
      this.events.emit("offline", socket);
      this.scheduleReconnect();
    });

    this.events.emit("online", socket);
  }

  private scheduleReconnect(): void {
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

  private clearSocket(socket: Socket): void {
    if (this.socket === socket) {
      this.socket = undefined;
    }
  }

  private clearTimer(): void {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }
}
