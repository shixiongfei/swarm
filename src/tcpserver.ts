/*
 * tcpserver.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { createServer } from "node:net";
import { SwarmError } from "./errors.ts";
import { Server } from "./server.ts";
import type { Socket } from "node:net";

export class TcpServer extends Server<Socket> {
  private readonly binding: { host: string; port: number };
  private readonly backlog: number | undefined;
  private readonly server = createServer((socket) => this.onSocket(socket));

  constructor(host: string, port: number, backlog?: number) {
    super();

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

  async listen() {
    if (this.isClosed) {
      throw new SwarmError("rejected", "tcp server is closed");
    }

    if (this.server.listening) {
      return this;
    }

    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => {
        reject(new SwarmError("failed", error.message));
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

    if (this.isClosed) {
      this.server.close();
      throw new SwarmError("rejected", "tcp server is closed");
    }

    this.server.on("error", (error) => this.events.emit("error", error));
    return this;
  }

  protected stop() {
    if (this.server.listening) {
      this.server.close();
    }
  }

  protected destroy(socket: Socket) {
    socket.destroy();
  }

  private onSocket(socket: Socket) {
    socket.on("error", () => socket.destroy());
    socket.once("close", () => this.drop(socket));

    this.accept(socket);
  }
}
