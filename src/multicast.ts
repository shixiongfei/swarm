/*
 * multicast.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { createSocket } from "node:dgram";
import { eventlite } from "eventlite.js";
import { sign, verify } from "./jwt.ts";
import { isLoopback } from "./address.ts";
import type { RemoteInfo, Socket } from "node:dgram";
import type { PeerEntry } from "./envelope.ts";

export type MulticastOptions = {
  family?: 4 | 6;
  group?: string;
  port?: number;
  interval?: number;
  interface?: string;
  ttl?: number;
};

export type MulticastConfig = MulticastOptions & {
  cluster: string;
  nodeId: string;
  started: number;
  secret?: string;
};

export type MulticastEvents = {
  peers: (entries: PeerEntry[]) => void;
};

const defaultGroup = "239.83.104.105";
const defaultGroup6 = "ff02:53:68:69:58:46::";
const defaultPort = 8870;
const defaultInterval = 10000;

export class Multicast {
  readonly events = eventlite<MulticastEvents>({
    isolate: true,
  });

  private readonly cluster: string;
  private readonly nodeId: string;
  private readonly started: number;
  private readonly secret: string | undefined;
  private readonly family: 4 | 6;
  private readonly group: string;
  private readonly port: number;
  private readonly interval: number;
  private readonly iface: string | undefined;
  private readonly ttl: number;
  private local: () => PeerEntry[] = () => [];
  private socket: Socket | undefined;
  private timer: ReturnType<typeof setInterval> | undefined;
  private retry: ReturnType<typeof setTimeout> | undefined;
  private closed = false;

  constructor(config: MulticastConfig) {
    this.cluster = config.cluster;
    this.nodeId = config.nodeId;
    this.started = config.started;
    this.secret = config.secret;
    this.family = config.family !== undefined ? config.family : 4;

    const groupForFamily = this.family === 6 ? defaultGroup6 : defaultGroup;

    this.group = config.group !== undefined ? config.group : groupForFamily;
    this.port = config.port !== undefined ? config.port : defaultPort;
    this.interval =
      config.interval !== undefined ? config.interval : defaultInterval;
    this.iface = config.interface;
    this.ttl = config.ttl !== undefined ? config.ttl : 1;
  }

  async open(local: () => PeerEntry[]) {
    this.local = local;

    if (this.closed) {
      return;
    }

    const socket = createSocket({
      type: this.family === 6 ? "udp6" : "udp4",
      reuseAddr: true,
    });

    this.socket = socket;

    await new Promise<void>((resolve, reject) => {
      const onError = (err: Error) => {
        this.dropSocket();
        reject(err);
      };

      socket.once("error", onError);
      socket.bind(this.port, () => {
        socket.removeListener("error", onError);

        try {
          socket.setMulticastTTL(this.ttl);

          if (this.iface !== undefined) {
            socket.setMulticastInterface(this.iface);
          }

          socket.addMembership(this.group, this.iface);
        } catch (err) {
          this.dropSocket();
          reject(err);
          return;
        }

        if (this.closed) {
          this.dropSocket();
          resolve();
          return;
        }

        socket.on("error", () => this.recover(socket));
        socket.on("message", (data, rinfo) => this.receive(data, rinfo));

        this.announce();

        this.timer = setInterval(() => this.announce(), this.interval);

        resolve();
      });
    });
  }

  send(entries: PeerEntry[]) {
    if (this.closed || this.socket === undefined) {
      return;
    }

    const secret = this.secret;

    for (const entry of entries) {
      const claims = {
        cluster: this.cluster,
        from: entry.id,
        addr: entry.addr,
        started: this.started,
      };

      this.socket.send(
        JSON.stringify({
          t: "advertise",
          cluster: claims.cluster,
          from: claims.from,
          addr: claims.addr,
          started: claims.started,
          secret: secret !== undefined,
          sig: secret !== undefined ? sign(secret, claims) : undefined,
        }),
        this.port,
        this.group,
      );
    }
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    if (this.retry !== undefined) {
      clearTimeout(this.retry);
      this.retry = undefined;
    }

    this.dropSocket();
  }

  private recover(socket: Socket) {
    if (this.closed || this.socket !== socket) {
      return;
    }

    this.dropSocket();
    this.retryLater();
  }

  private retryLater() {
    if (this.closed || this.retry !== undefined) {
      return;
    }

    const timer = setTimeout(() => {
      this.retry = undefined;

      if (this.closed) {
        return;
      }

      void this.open(this.local).catch(() => this.retryLater());
    }, this.interval);

    timer.unref();
    this.retry = timer;
  }

  private dropSocket() {
    if (this.timer !== undefined) {
      clearInterval(this.timer);
      this.timer = undefined;
    }

    const socket = this.socket;

    if (socket === undefined) {
      return;
    }

    this.socket = undefined;

    socket.removeAllListeners("message");
    socket.removeAllListeners("error");

    try {
      socket.close();
    } catch {
      // the socket may not have been bound
    }
  }

  private announce() {
    this.send(this.local());
  }

  private receive(data: Buffer, rinfo: RemoteInfo) {
    let message: unknown;

    try {
      message = JSON.parse(data.toString());
    } catch {
      return;
    }

    const value = message as {
      t?: unknown;
      cluster?: unknown;
      from?: unknown;
      addr?: unknown;
      started?: unknown;
      secret?: unknown;
      sig?: unknown;
    };

    if (
      value.t !== "advertise" ||
      typeof value.cluster !== "string" ||
      typeof value.from !== "string" ||
      typeof value.addr !== "string" ||
      typeof value.started !== "number" ||
      typeof value.secret !== "boolean" ||
      value.from === this.nodeId ||
      value.cluster !== this.cluster ||
      value.secret !== (this.secret !== undefined)
    ) {
      return;
    }

    if (this.secret !== undefined) {
      if (typeof value.sig !== "string") {
        return;
      }

      const claims = verify(this.secret, value.sig);

      if (
        claims === undefined ||
        claims.cluster !== value.cluster ||
        claims.from !== value.from ||
        claims.addr !== value.addr ||
        claims.started !== value.started
      ) {
        return;
      }
    }

    const host = hostname(value.addr);

    if (
      host === undefined ||
      (!isLoopback(rinfo.address) && isLoopback(host))
    ) {
      return;
    }

    this.events.emit("peers", [{ id: value.from, addr: value.addr }]);
  }
}

function hostname(addr: string) {
  try {
    return new URL(addr).hostname.replace(/^\[(.*)\]$/, "$1");
  } catch {
    return undefined;
  }
}
