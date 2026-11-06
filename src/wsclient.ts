/*
 * wsclient.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { WebSocket } from "ws";
import { SwarmError } from "./errors.ts";
import { Client } from "./client.ts";
import type { ClientOptions } from "./client.ts";

export class WsClient extends Client<WebSocket> {
  private readonly url: string | URL;

  constructor(url: string | URL, options: ClientOptions = {}) {
    super(options);

    this.url = url;
  }

  get connected() {
    return this.socket?.readyState === WebSocket.OPEN;
  }

  protected open() {
    return new Promise<void>((resolve, reject) => {
      const socket = new WebSocket(this.url);

      this.socket = socket;
      this.lastError = undefined;

      const onError = (error: Error) => {
        this.clearSocket(socket);

        reject(
          this.isClosed
            ? new SwarmError("rejected", "client is closed")
            : new SwarmError("failed", error.message),
        );
      };

      const onClose = () => {
        this.clearSocket(socket);

        reject(
          this.isClosed
            ? new SwarmError("rejected", "client is closed")
            : new SwarmError("failed", "connection closed before connect"),
        );
      };

      socket.once("error", onError);
      socket.once("close", onClose);
      socket.once("open", () => {
        socket.off("error", onError);
        socket.off("close", onClose);

        socket.on("error", (error) => {
          this.lastError = error;
          socket.terminate();
        });

        socket.once("close", () => this.markOffline(socket));

        this.markOnline(socket);
        resolve();
      });
    });
  }

  protected destroy(socket: WebSocket) {
    socket.terminate();
  }

  private clearSocket(socket: WebSocket) {
    if (this.socket === socket) {
      this.socket = undefined;
    }
  }
}
