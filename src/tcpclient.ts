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

import { connect as netConnect } from "node:net";
import { MusterError } from "./errors.ts";
import { Client } from "./client.ts";
import type { ClientOptions } from "./client.ts";
import type { Socket } from "node:net";

export class TcpClient extends Client<Socket> {
  private readonly binding: { host: string; port: number };

  constructor(host: string, port: number, options: ClientOptions = {}) {
    super(options);

    this.binding = { host, port };
  }

  get connected() {
    return this.socket?.readyState === "open";
  }

  protected open() {
    return new Promise<void>((resolve, reject) => {
      const socket = netConnect(this.binding);

      this.socket = socket;
      this.lastError = undefined;

      const onError = (error: Error) => {
        this.clearSocket(socket);
        socket.destroy();

        reject(
          this.isClosed
            ? new MusterError("rejected", "client is closed")
            : new MusterError("failed", error.message),
        );
      };

      const onClose = () => {
        this.clearSocket(socket);

        reject(
          this.isClosed
            ? new MusterError("rejected", "client is closed")
            : new MusterError("failed", "connection closed before connect"),
        );
      };

      socket.once("error", onError);
      socket.once("close", onClose);
      socket.once("connect", () => {
        socket.off("error", onError);
        socket.off("close", onClose);

        socket.on("error", (error) => {
          this.lastError = error;
          socket.destroy();
        });

        socket.once("close", () => this.markOffline(socket));

        this.markOnline(socket);
        resolve();
      });
    });
  }

  protected destroy(socket: Socket) {
    socket.destroy();
  }

  private clearSocket(socket: Socket) {
    if (this.socket === socket) {
      this.socket = undefined;
    }
  }
}
