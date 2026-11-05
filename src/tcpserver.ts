/*
 * tcpserver.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { eventlite } from "eventlite.js";
import { createServer } from "node:net";
import { MusterError } from "./errors.ts";
import type { Server, Socket } from "node:net";

export type TcpServerEvents = {
  online: (socket: Socket) => void;
  offline: (socket: Socket) => void;
  error: (error: Error) => void;
};

export class TcpServer {
  readonly events = eventlite<TcpServerEvents>({
    allowDuplicate: true,
    isolate: true,
  });

  private readonly binding: { host: string; port: number };
  private readonly backlog: number | undefined;
  private readonly sockets = new Set<Socket>();
  private readonly server: Server = createServer(this.accept.bind(this));
  private closed = false;

  constructor(host: string, port: number, backlog?: number) {
    this.binding = { host, port };
    this.backlog = backlog;
  }

  get host() {
    const address = this.server.address();

    return typeof address === "object" && address !== null
      ? address.address
      : this.binding.host;
  }

  get port() {
    const address = this.server.address();

    return typeof address === "object" && address !== null
      ? address.port
      : this.binding.port;
  }

  get size() {
    return this.sockets.size;
  }

  get connections() {
    return Array.from(this.sockets);
  }

  async listen() {
    if (this.closed) {
      throw new MusterError("rejected", "tcp server is closed");
    }

    if (this.server.listening) {
      return this;
    }

    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => {
        reject(new MusterError("failed", error.message));
      };

      this.server.once("error", onError);
      this.server.listen(
        {
          host: this.binding.host,
          port: this.binding.port,
          backlog: this.backlog,
        },
        () => {
          this.server.off("error", onError);
          resolve();
        },
      );
    });

    if (this.closed) {
      this.server.close();
      throw new MusterError("rejected", "tcp server is closed");
    }

    this.server.on("error", (error) => this.events.emit("error", error));
    return this;
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    if (this.server.listening) {
      this.server.close();
    }

    for (const socket of this.sockets) {
      socket.destroy();
      this.events.emit("offline", socket);
    }

    this.sockets.clear();
  }

  private accept(socket: Socket) {
    if (this.closed) {
      socket.destroy();
      return;
    }

    this.sockets.add(socket);

    socket.on("error", () => socket.destroy());
    socket.once("close", () => {
      if (!this.sockets.delete(socket)) {
        return;
      }

      this.events.emit("offline", socket);
    });

    this.events.emit("online", socket);
  }
}
