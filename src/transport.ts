/*
 * transport.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { SwarmError } from "./errors.ts";
import { isLoopback, normalizePath, parseUrl } from "./address.ts";
import { LinkStream } from "./linkstream.ts";
import { LinkWs } from "./linkws.ts";
import type { Client } from "./client.ts";
import type { WsClientOptions } from "./wsclient.ts";
import type { Server } from "./server.ts";
import type { Link } from "./link.ts";
import type { WsServerOptions } from "./wsserver.ts";

export type WsListenOptions = Omit<WsServerOptions, "backlog">;

export type ListenOptions = {
  backlog?: number;
  advertise?: string | URL;
  ws?: WsListenOptions;
};

export type TransportOptions = {
  nodeId: string;
  join: (nodeLink: Link, secret?: string) => void;
  attach: (
    nodeLink: Link,
    secret?: string,
    expected?: string,
    addr?: string,
    auto?: boolean,
  ) => void;
  onError: (error: Error) => void;
};

export class Transport {
  private readonly nodeId: string;
  private readonly join: TransportOptions["join"];
  private readonly attach: TransportOptions["attach"];
  private readonly onError: TransportOptions["onError"];
  private readonly servers: Server[] = [];
  private readonly clients: Client[] = [];

  constructor(options: TransportOptions) {
    this.nodeId = options.nodeId;
    this.join = options.join;
    this.attach = options.attach;
    this.onError = options.onError;
  }

  async open(url: string | URL, options?: ListenOptions) {
    const server = await this.createServer(url, options);

    this.servers.push(server);

    try {
      await server.listen();
    } catch (err) {
      this.close(server);
      throw err;
    }

    return server;
  }

  close(server: Server) {
    const index = this.servers.indexOf(server);

    if (index >= 0) {
      this.servers.splice(index, 1);
    }

    server.close();
  }

  async connect(url: string | URL, options?: WsClientOptions) {
    const client = await this.createClient(url, options);

    this.track(client);

    try {
      await client.connect();
    } catch (err) {
      if (options !== undefined && options.reconnect === false) {
        this.untrack(client);
        client.close();
      }

      throw err;
    }
  }

  async createClient(
    url: string | URL,
    options?: WsClientOptions,
    expected?: string,
  ): Promise<Client> {
    const { protocol, host, port, secret: urlSecret } = parseUrl(url);
    const secret =
      options !== undefined && options.secret !== undefined
        ? options.secret
        : urlSecret;

    if (protocol === "tcp:") {
      const { TcpClient } = await import("./tcpclient.ts");
      const client = new TcpClient(host, port, options);

      client.events.on("online", (socket) => {
        this.attach(
          new LinkStream(socket, this.nodeId),
          secret,
          expected,
          String(url),
        );
      });

      return client;
    }

    const { WsClient } = await import("./wsclient.ts");
    const client = new WsClient(url, options);

    client.events.on("online", (socket) => {
      this.attach(
        new LinkWs(socket, this.nodeId, isLoopback(host)),
        secret,
        expected,
        String(url),
      );
    });

    return client;
  }

  track(client: Client) {
    this.clients.push(client);
  }

  untrack(client: Client) {
    const index = this.clients.indexOf(client);

    if (index >= 0) {
      this.clients.splice(index, 1);
    }
  }

  dispose() {
    for (const client of this.clients) {
      client.close();
    }

    this.clients.length = 0;

    for (const server of this.servers) {
      server.close();
    }

    this.servers.length = 0;
  }

  private async createServer(
    url: string | URL,
    options: ListenOptions | undefined,
  ): Promise<Server> {
    const { protocol, secure, host, port, secret, path } = parseUrl(url);
    const ws = options !== undefined ? options.ws : undefined;
    const backlog = options !== undefined ? options.backlog : undefined;
    const tls = ws !== undefined ? ws.tls : undefined;
    const server = ws !== undefined ? ws.server : undefined;
    const listenPath =
      ws !== undefined && ws.path !== undefined ? normalizePath(ws.path) : path;

    if (secure && server === undefined && tls === undefined) {
      throw new SwarmError("rejected", "wss listen requires tls options");
    }

    if (protocol === "tcp:") {
      if (ws !== undefined) {
        throw new SwarmError("rejected", "tcp listen does not support http");
      }

      const { TcpServer } = await import("./tcpserver.ts");
      const tcpServer = new TcpServer(host, port, backlog);

      tcpServer.events.on("online", (socket) => {
        this.join(new LinkStream(socket, this.nodeId), secret);
      });
      tcpServer.events.on("error", (error) => {
        this.onError(error);
        this.close(tcpServer);
      });

      return tcpServer;
    }

    const { WsServer } = await import("./wsserver.ts");
    const wsServer = new WsServer(host, port, {
      backlog,
      path: listenPath,
      server,
      tls,
    });

    wsServer.events.on("online", (socket, remoteAddress) => {
      this.join(
        new LinkWs(
          socket as unknown as WebSocket,
          this.nodeId,
          isLoopback(remoteAddress),
        ),
        secret,
      );
    });
    wsServer.events.on("error", (error) => {
      this.onError(error);
      this.close(wsServer);
    });

    return wsServer;
  }
}
