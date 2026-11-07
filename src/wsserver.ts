/*
 * wsserver.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { createServer } from "node:http";
import { WebSocketServer } from "ws";
import { SwarmError } from "./errors.ts";
import { maxEnvelopeSize } from "./codec.ts";
import { Server } from "./server.ts";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import type { WebSocket } from "ws";

export class WsServer extends Server<WebSocket> {
  private readonly binding: { host: string; port: number };
  private readonly backlog: number | undefined;
  private readonly http = createServer();
  private readonly server = new WebSocketServer({
    noServer: true,
    maxPayload: maxEnvelopeSize,
  });

  constructor(host: string, port: number, backlog?: number) {
    super();

    this.binding = { host, port };
    this.backlog = backlog;

    this.http.on("upgrade", (request, socket, head) =>
      this.upgrade(request, socket, head),
    );
  }

  get host() {
    const address = this.http.address();

    return typeof address === "object" && address !== null
      ? address.address
      : this.binding.host;
  }

  get port() {
    const address = this.http.address();

    return typeof address === "object" && address !== null
      ? address.port
      : this.binding.port;
  }

  async listen() {
    if (this.isClosed) {
      throw new SwarmError("rejected", "ws server is closed");
    }

    if (this.http.listening) {
      return this;
    }

    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => {
        reject(new SwarmError("failed", error.message));
      };

      this.http.once("error", onError);
      this.http.listen(
        {
          host: this.binding.host,
          port: this.binding.port,
          backlog: this.backlog,
        },
        () => {
          this.http.off("error", onError);
          resolve();
        },
      );
    });

    if (this.isClosed) {
      this.http.close();
      throw new SwarmError("rejected", "ws server is closed");
    }

    this.http.on("error", (error) => this.events.emit("error", error));
    return this;
  }

  protected stop() {
    if (this.http.listening) {
      this.http.close();
    }

    this.server.close();
  }

  protected destroy(socket: WebSocket) {
    socket.terminate();
  }

  private upgrade(request: IncomingMessage, socket: Duplex, head: Buffer) {
    this.server.handleUpgrade(request, socket, head, (ws) => this.onSocket(ws));
  }

  private onSocket(socket: WebSocket) {
    socket.on("error", () => socket.terminate());
    socket.once("close", () => this.drop(socket));

    this.accept(socket);
  }
}
