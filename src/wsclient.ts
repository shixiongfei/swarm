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

import { SwarmError } from "./errors.ts";
import { Client } from "./client.ts";
import type { ClientOptions } from "./client.ts";

export type WsTlsOptions = {
  ca?: string | Buffer | Array<string | Buffer>;
  cert?: string | Buffer;
  key?: string | Buffer;
  passphrase?: string;
  rejectUnauthorized?: boolean;
  servername?: string;
};

export type WsClientOptions = ClientOptions & { tls?: WsTlsOptions };

export class WsClient extends Client<WebSocket> {
  private readonly url: string | URL;
  private readonly tls: WsTlsOptions | undefined;

  constructor(url: string | URL, options: WsClientOptions = {}) {
    super(options);

    if (options.tls === undefined && typeof WebSocket === "undefined") {
      throw new SwarmError("failed", "websocket is unavailable");
    }

    this.url = url;
    this.tls = options.tls;
  }

  get connected() {
    return (
      this.socket !== undefined && this.socket.readyState === WebSocket.OPEN
    );
  }

  protected async open() {
    const socket =
      this.tls !== undefined
        ? await openTls(this.url, this.tls)
        : new WebSocket(this.url);

    this.socket = socket;

    return new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        socket.removeEventListener("error", onError);
        socket.removeEventListener("close", onClose);
        socket.removeEventListener("open", onOpen);
      };

      const onError = (event: unknown) => {
        cleanup();
        this.clearSocket(socket);

        reject(
          this.isClosed
            ? new SwarmError("rejected", "client is closed")
            : new SwarmError("failed", errorMessage(event)),
        );
      };

      const onClose = () => {
        cleanup();
        this.clearSocket(socket);

        reject(
          this.isClosed
            ? new SwarmError("rejected", "client is closed")
            : new SwarmError("failed", "connection closed before connect"),
        );
      };

      const onOpen = () => {
        cleanup();

        socket.addEventListener("error", () => socket.close());

        socket.addEventListener("close", () => this.markOffline(socket));

        this.markOnline(socket);
        resolve();
      };

      socket.addEventListener("error", onError);
      socket.addEventListener("close", onClose);
      socket.addEventListener("open", onOpen);
    });
  }

  protected destroy(socket: WebSocket) {
    socket.close();
  }

  private clearSocket(socket: WebSocket) {
    if (this.socket === socket) {
      this.socket = undefined;
    }
  }
}

function errorMessage(event: unknown) {
  const message = (event as { message?: unknown }).message;
  return typeof message === "string" ? message : "socket error";
}

async function openTls(url: string | URL, tls: WsTlsOptions) {
  const { WebSocket: WsSocket } = await import("ws");

  return new WsSocket(url, undefined, tls) as unknown as WebSocket;
}
