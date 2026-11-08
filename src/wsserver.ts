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
import type { IncomingMessage, Server as HttpServer } from "node:http";
import type { Duplex } from "node:stream";
import type { WebSocket } from "ws";

export type WsServerOptions = {
  backlog?: number;
  path?: string;
  server?: HttpServer;
};

export class WsServer extends Server<WebSocket> {
  private readonly binding: { host: string; port: number };
  private readonly backlog: number | undefined;
  private readonly path: string;
  private readonly owned: boolean;
  private readonly http: HttpServer;
  private readonly server = new WebSocketServer({
    noServer: true,
    maxPayload: maxEnvelopeSize,
  });

  private readonly onUpgrade = (
    request: IncomingMessage,
    socket: Duplex,
    head: Buffer,
  ) => {
    const url = request.url !== undefined ? request.url : "/";
    const path = url.split("?", 2)[0];

    if (path !== this.path) {
      if (this.http.listenerCount("upgrade") === 1) {
        socket.destroy();
      }

      return;
    }

    this.server.handleUpgrade(request, socket, head, (ws) => this.onSocket(ws));
  };

  constructor(host: string, port: number, options?: WsServerOptions) {
    super();

    const attach = options !== undefined ? options.server : undefined;

    this.binding = { host, port };
    this.backlog = options !== undefined ? options.backlog : undefined;
    this.path =
      options !== undefined && options.path !== undefined ? options.path : "/";
    this.owned = attach === undefined;
    this.http = attach !== undefined ? attach : createServer();

    this.http.on("upgrade", this.onUpgrade);
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

    if (!this.owned) {
      throw new SwarmError("rejected", "http server is not listening");
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
    this.http.off("upgrade", this.onUpgrade);
    this.server.close();

    if (this.owned && this.http.listening) {
      this.http.close();
    }
  }

  protected destroy(socket: WebSocket) {
    socket.terminate();
  }

  private onSocket(socket: WebSocket) {
    socket.on("error", () => socket.terminate());
    socket.once("close", () => this.drop(socket));

    this.accept(socket);
  }
}
