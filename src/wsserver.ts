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
import { createServer as createHttpsServer } from "node:https";
import { WebSocket, WebSocketServer } from "ws";
import { SwarmError } from "./errors.ts";
import { maxEnvelopeSize } from "./codec.ts";
import { Server } from "./server.ts";
import type { IncomingMessage, Server as HttpServer } from "node:http";
import type { ServerOptions as HttpsServerOptions } from "node:https";
import type { Duplex } from "node:stream";

export type WsServerOptions = {
  backlog?: number;
  path?: string;
  server?: HttpServer;
  tls?: HttpsServerOptions;
};

type UpgradeHandler = (
  request: IncomingMessage,
  socket: Duplex,
  head: Buffer,
) => void;

type Router = {
  readonly paths: Map<string, UpgradeHandler>;
  readonly onUpgrade: UpgradeHandler;
};

const routers = new WeakMap<HttpServer, Router>();

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

  private readonly onUpgrade: UpgradeHandler = (request, socket, head) => {
    this.server.handleUpgrade(request, socket, head, (ws) =>
      this.onSocket(ws, (socket as { remoteAddress?: string }).remoteAddress),
    );
  };

  constructor(host: string, port: number, options?: WsServerOptions) {
    super();

    const attach = options !== undefined ? options.server : undefined;
    const tls = options !== undefined ? options.tls : undefined;

    this.binding = { host, port };
    this.backlog = options !== undefined ? options.backlog : undefined;
    this.path =
      options !== undefined && options.path !== undefined ? options.path : "/";
    this.owned = attach === undefined;
    this.http =
      attach !== undefined
        ? attach
        : tls !== undefined
          ? createHttpsServer(tls)
          : createServer();

    let router = routers.get(this.http);

    if (router === undefined) {
      const http = this.http;
      const paths = new Map<string, UpgradeHandler>();
      const onUpgrade: UpgradeHandler = (request, socket, head) => {
        const url = request.url !== undefined ? request.url : "/";
        const handler = paths.get(url.split("?", 2)[0]);

        if (handler !== undefined) {
          handler(request, socket, head);
          return;
        }

        if (http.listenerCount("upgrade") === 1) {
          socket.destroy();
        }
      };

      router = { paths, onUpgrade };
      routers.set(this.http, router);
      this.http.on("upgrade", onUpgrade);
    }

    router.paths.set(this.path, this.onUpgrade);
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
      throw new SwarmError("rejected", "CLOSED", "ws server is closed", false);
    }

    if (this.http.listening) {
      return this;
    }

    if (!this.owned) {
      throw new SwarmError(
        "rejected",
        "REJECTED",
        "http server is not listening",
      );
    }

    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => {
        reject(new SwarmError("failed", "FAILED", error.message));
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
      throw new SwarmError("rejected", "CLOSED", "ws server is closed", false);
    }

    this.http.on("error", (error) => this.events.emit("error", error));
    return this;
  }

  protected stop() {
    const router = routers.get(this.http);

    if (router !== undefined) {
      if (router.paths.get(this.path) === this.onUpgrade) {
        router.paths.delete(this.path);
      }

      if (router.paths.size === 0) {
        this.http.off("upgrade", router.onUpgrade);
        routers.delete(this.http);
      }
    }

    this.server.close();

    if (this.owned && this.http.listening) {
      this.http.close();
    }
  }

  protected destroy(socket: WebSocket) {
    if (socket.readyState === WebSocket.CLOSED) {
      return;
    }

    socket.terminate();
  }

  protected isClosing(socket: WebSocket) {
    return socket.readyState === WebSocket.CLOSING;
  }

  private onSocket(socket: WebSocket, remoteAddress?: string) {
    socket.on("error", () => socket.terminate());
    socket.once("close", () => this.drop(socket));

    this.accept(socket, remoteAddress);
  }
}
