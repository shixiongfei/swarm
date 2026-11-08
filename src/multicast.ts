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
import type { Socket } from "node:dgram";
import type { PeerEntry } from "./envelope.ts";

export type MulticastOptions = {
  group?: string;
  port?: number;
  interval?: number;
  interface?: string;
  ttl?: number;
};

export type MulticastConfig = MulticastOptions & {
  cluster: string;
  nodeId: string;
};

export type MulticastEvents = {
  peers: (entries: PeerEntry[]) => void;
};

const defaultGroup = "239.83.104.105";
const defaultPort = 8870;
const defaultInterval = 10000;

export class Multicast {
  readonly events = eventlite<MulticastEvents>({
    isolate: true,
  });

  private readonly cluster: string;
  private readonly nodeId: string;
  private readonly group: string;
  private readonly port: number;
  private readonly interval: number;
  private readonly iface: string | undefined;
  private readonly ttl: number;
  private local: () => PeerEntry[] = () => [];
  private socket: Socket | undefined;
  private timer: ReturnType<typeof setInterval> | undefined;
  private closed = false;

  constructor(config: MulticastConfig) {
    this.cluster = config.cluster;
    this.nodeId = config.nodeId;
    this.group = config.group !== undefined ? config.group : defaultGroup;
    this.port = config.port !== undefined ? config.port : defaultPort;
    this.interval =
      config.interval !== undefined ? config.interval : defaultInterval;
    this.iface = config.interface;
    this.ttl = config.ttl !== undefined ? config.ttl : 1;
  }

  async open(local: () => PeerEntry[]) {
    this.local = local;

    const socket = createSocket({ type: "udp4", reuseAddr: true });

    this.socket = socket;

    await new Promise<void>((resolve, reject) => {
      const onError = (err: Error) => {
        this.close();
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
          this.close();
          reject(err);
          return;
        }

        if (this.closed) {
          try {
            socket.close();
          } catch {
            // already closed
          }
          resolve();
          return;
        }

        socket.on("error", () => this.close());
        socket.on("message", (data) => this.receive(data));

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

    for (const entry of entries) {
      this.socket.send(
        JSON.stringify({
          t: "advertise",
          cluster: this.cluster,
          from: entry.id,
          addr: entry.addr,
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

    if (this.timer !== undefined) {
      clearInterval(this.timer);
    }

    if (this.socket !== undefined) {
      try {
        this.socket.close();
      } catch {
        // the socket may not have been bound
      }
    }
  }

  private announce() {
    this.send(this.local());
  }

  private receive(data: Buffer) {
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
    };

    if (
      value.t !== "advertise" ||
      value.cluster !== this.cluster ||
      typeof value.from !== "string" ||
      typeof value.addr !== "string" ||
      value.from === this.nodeId
    ) {
      return;
    }

    this.events.emit("peers", [{ id: value.from, addr: value.addr }]);
  }
}
