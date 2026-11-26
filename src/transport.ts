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
import { withCancel } from "./timeout.ts";
import { isLoopback, normalizePath, parseUrl } from "./address.ts";
import { LinkStream } from "./linkstream.ts";
import { LinkWs } from "./linkws.ts";
import type { Client } from "./client.ts";
import type { WsClientOptions } from "./wsclient.ts";
import type { Server } from "./server.ts";
import type { Link } from "./link.ts";
import type { NodeLink } from "./nodelink.ts";
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
  ) => NodeLink | void;
  onError: (error: Error) => void;
};

export class Transport {
  private readonly nodeId: string;
  private readonly join: TransportOptions["join"];
  private readonly attach: TransportOptions["attach"];
  private readonly onError: TransportOptions["onError"];
  private readonly servers: Server[] = [];
  private readonly clients: Client[] = [];
  private readonly outbound = new Map<string, Client>();
  private readonly attached = new Map<Client, NodeLink>();
  private readonly pendingConnects = new Map<
    string,
    Promise<NodeLink | undefined>
  >();
  private disposed = false;

  constructor(options: TransportOptions) {
    this.nodeId = options.nodeId;
    this.join = options.join;
    this.attach = options.attach;
    this.onError = options.onError;
  }

  async open(url: string | URL, options: ListenOptions = {}) {
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

  async connect(url: string | URL, options: WsClientOptions = {}) {
    const addr = String(url);
    const pending = this.pendingConnects.get(addr);

    if (pending !== undefined) {
      return await withCancel(pending, options.signal);
    }

    const connecting = this.connectOnce(addr, url, options).finally(() => {
      this.pendingConnects.delete(addr);
    });

    this.pendingConnects.set(addr, connecting);

    return await connecting;
  }

  async createClient(
    url: string | URL,
    options: WsClientOptions = {},
    expected?: string,
  ): Promise<Client> {
    const { protocol, host, port, secret: urlSecret } = parseUrl(url);
    const secret =
      options.secret !== undefined ? options.secret : urlSecret || undefined;

    if (protocol === "tcp:") {
      const { TcpClient } = await import("./tcpclient.ts");
      const client = new TcpClient(host, port, options);

      client.events.on("online", (socket) => {
        this.remember(
          client,
          this.attach(
            new LinkStream(socket, this.nodeId),
            secret,
            expected,
            String(url),
          ),
        );
      });

      return client;
    }

    const { WsClient } = await import("./wsclient.ts");
    const client = new WsClient(url, options);

    client.events.on("online", (socket) => {
      this.remember(
        client,
        this.attach(
          new LinkWs(socket, this.nodeId, isLoopback(host)),
          secret,
          expected,
          String(url),
        ),
      );
    });

    return client;
  }

  track(client: Client, addr: string) {
    this.clients.push(client);
    this.outbound.set(addr, client);
  }

  untrack(client: Client) {
    const index = this.clients.indexOf(client);

    if (index >= 0) {
      this.clients.splice(index, 1);
    }

    this.attached.delete(client);

    for (const [addr, existing] of this.outbound) {
      if (existing === client) {
        this.outbound.delete(addr);
      }
    }
  }

  drop(addr: string) {
    const client = this.outbound.get(addr);

    if (client === undefined) {
      return;
    }

    this.untrack(client);
    client.close();
  }

  dispose() {
    this.disposed = true;

    for (const client of this.clients) {
      client.close();
    }

    this.clients.length = 0;
    this.outbound.clear();
    this.attached.clear();

    for (const server of this.servers) {
      server.close();
    }

    this.servers.length = 0;
  }

  private async connectOnce(
    addr: string,
    url: string | URL,
    options: WsClientOptions,
  ) {
    const existing = this.outbound.get(addr);

    if (existing !== undefined) {
      await existing.connect();

      const attached = this.attached.get(existing);

      if (attached !== undefined) {
        return attached;
      }

      this.untrack(existing);
      existing.close();
    }

    const client = await this.createClient(url, options);

    if (this.disposed) {
      client.close();
      throw new SwarmError("rejected", "CLOSED", "node is closed", false);
    }

    this.track(client, addr);

    try {
      await client.connect();
    } catch (err) {
      if (options.reconnect === false) {
        this.untrack(client);
        client.close();
      }

      throw err;
    }

    return this.attached.get(client);
  }

  private remember(client: Client, nodeLink: NodeLink | void) {
    if (nodeLink === undefined) {
      return;
    }

    this.attached.set(client, nodeLink);

    nodeLink.events.on("close", () => {
      if (this.attached.get(client) === nodeLink) {
        this.attached.delete(client);
      }
    });
  }

  private async createServer(
    url: string | URL,
    options: ListenOptions,
  ): Promise<Server> {
    const {
      protocol,
      secure,
      host,
      port,
      secret: urlSecret,
      path,
    } = parseUrl(url);
    const secret = urlSecret || undefined;
    const ws = options.ws;
    const backlog = options.backlog;
    const tls = ws !== undefined ? ws.tls : undefined;
    const server = ws !== undefined ? ws.server : undefined;
    const listenPath =
      ws !== undefined && ws.path !== undefined ? normalizePath(ws.path) : path;

    if (secure && server === undefined && tls === undefined) {
      throw new SwarmError(
        "rejected",
        "REJECTED",
        "wss listen requires tls options",
      );
    }

    if (protocol === "tcp:") {
      if (ws !== undefined) {
        throw new SwarmError(
          "rejected",
          "REJECTED",
          "tcp listen does not support http",
        );
      }

      const { TcpServer } = await import("./tcpserver.ts");
      const tcpServer = new TcpServer(host, port, backlog);

      tcpServer.events.on("online", (socket) => {
        this.join(new LinkStream(socket, this.nodeId), secret);
      });
      tcpServer.events.on("error", (error) => this.onError(error));

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
    wsServer.events.on("error", (error) => this.onError(error));

    return wsServer;
  }
}
