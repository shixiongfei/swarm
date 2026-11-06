/*
 * server.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { eventlite } from "eventlite.js";

export type ServerEvents<S = any> = {
  online: (socket: S) => void;
  offline: (socket: S) => void;
  error: (error: Error) => void;
};

export abstract class Server<S = any> {
  readonly events = eventlite<ServerEvents<S>>({
    allowDuplicate: true,
    isolate: true,
  });

  private readonly sockets = new Set<S>();
  private closed = false;

  get size() {
    return this.sockets.size;
  }

  get connections(): readonly S[] {
    return Array.from(this.sockets);
  }

  protected get isClosed() {
    return this.closed;
  }

  abstract listen(): Promise<this>;

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.stop();

    for (const socket of this.sockets) {
      this.destroy(socket);
      this.events.emit("offline", socket);
    }

    this.sockets.clear();
  }

  protected accept(socket: S) {
    if (this.closed) {
      this.destroy(socket);
      return;
    }

    this.sockets.add(socket);
    this.events.emit("online", socket);
  }

  protected drop(socket: S) {
    if (!this.sockets.delete(socket)) {
      return;
    }

    this.events.emit("offline", socket);
  }

  protected abstract stop(): void;
  protected abstract destroy(socket: S): void;
}
