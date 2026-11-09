/*
 * nodelink.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { eventlite } from "eventlite.js";
import { SwarmError } from "./errors.ts";
import { protocolVersion } from "./envelope.ts";
import { PendingCalls } from "./pending.ts";
import { Registry } from "./registry.ts";
import { signToken, verifyToken } from "./token.ts";
import type { Link } from "./link.ts";
import type { Provider } from "./provider.ts";
import type { Envelope, PeerEntry } from "./envelope.ts";
import type { Instance, Observable, WatchHandler } from "./registry.ts";

type NodeLinkEvents = {
  open: (peerId: string) => void;
  peers: (members: PeerEntry[]) => void;
  bye: () => void;
  close: () => void;
};

const defaultHeartbeatInterval = 15000;
const defaultHeartbeatTimeout = 45000;
const defaultHandshakeTimeout = 15000;

type NodeLinkConfig = {
  provider: Provider;
  secret?: string;
  heartbeatInterval?: number;
  heartbeatTimeout?: number;
  handshakeTimeout?: number;
  maxInflight?: number;
};

export class NodeLink implements Observable {
  readonly events = eventlite<NodeLinkEvents>({
    allowDuplicate: true,
    isolate: true,
  });
  readonly secret?: string;

  private readonly createdAt = Date.now();
  private readonly registry = new Registry();
  private readonly pending = new PendingCalls();
  private readonly provider: Provider;
  private readonly link: Link;
  private readonly heartbeatInterval: number;
  private readonly heartbeatTimeout: number;
  private readonly handshakeTimeout: number;
  private readonly maxInflight: number | undefined;
  private readonly disposes: Array<() => void>;
  private peer?: string;
  private closed = false;
  private lastSeen = Date.now();
  private heartbeat?: ReturnType<typeof setInterval>;
  private handshake?: ReturnType<typeof setTimeout>;
  private pingAt?: number;
  private rtt?: number;
  private openedAt?: number;

  private readonly onEnvelope = (message: Envelope) => {
    this.lastSeen = Date.now();

    switch (message.t) {
      case "hello":
        if (!this.negotiate(message.version)) {
          return;
        }

        if (this.secret === undefined) {
          this.open(message.from, true);
          return;
        }

        this.authenticate(message.from, message.token, true);
        return;

      case "accept":
        if (!this.negotiate(message.version)) {
          return;
        }

        if (this.secret === undefined) {
          this.open(message.from, false);
          return;
        }

        this.authenticate(message.from, message.token, false);
        return;

      case "up":
        if (this.peer !== undefined && message.from !== this.link.nodeId) {
          this.registry.add(message.service, message.from, message.meta);
        }
        return;

      case "down":
        if (this.peer !== undefined && message.from !== this.link.nodeId) {
          this.registry.remove(message.service, message.from);
        }
        return;

      case "call":
        if (
          this.peer === undefined ||
          message.from !== this.peer ||
          message.to !== this.link.nodeId
        ) {
          return;
        }

        Promise.resolve()
          .then(() =>
            this.provider.invoke(
              message.service,
              message.method,
              message.args,
              message.timeout,
              message.ttl,
              this,
              message.target,
            ),
          )
          .then(
            (value) => {
              try {
                this.link.reply(message.id, message.from, value);
              } catch (err) {
                this.link.fail(
                  message.id,
                  message.from,
                  new SwarmError("failed", "reply is not cloneable"),
                );
              }
            },
            (error: unknown) => this.link.fail(message.id, message.from, error),
          );
        return;

      case "reply":
        if (
          this.peer !== undefined &&
          message.from === this.peer &&
          message.to === this.link.nodeId
        ) {
          this.pending.settle(message.id, message.value);
        }
        return;

      case "error":
        if (
          this.peer !== undefined &&
          message.from === this.peer &&
          message.to === this.link.nodeId
        ) {
          this.pending.reject(
            message.id,
            new SwarmError(message.kind, message.message),
          );
        }
        return;

      case "pub":
        if (
          this.peer !== undefined &&
          message.from === this.peer &&
          message.origin !== this.link.nodeId
        ) {
          this.provider.publish(
            message.topic,
            message.data,
            message.origin,
            message.from,
            message.id,
            message.ttl,
          );
        }
        return;

      case "ping":
        if (this.peer !== undefined && message.from === this.peer) {
          this.link.send({ t: "pong", from: this.link.nodeId });
        }
        return;

      case "pong":
        if (
          this.peer !== undefined &&
          message.from === this.peer &&
          this.pingAt !== undefined
        ) {
          const sample = Date.now() - this.pingAt;

          this.rtt =
            this.rtt === undefined ? sample : (this.rtt * 3 + sample) / 4;

          this.pingAt = undefined;
        }
        return;

      case "peers":
        if (this.peer !== undefined && message.from === this.peer) {
          this.events.emit("peers", message.members);
        }
        return;

      case "bye":
        if (this.peer !== undefined && message.from === this.peer) {
          this.events.emit("bye");
          this.close();
        }
        return;
    }
  };

  constructor(link: Link, config: NodeLinkConfig) {
    this.link = link;
    this.provider = config.provider;
    this.secret = config.secret;
    this.heartbeatInterval =
      config.heartbeatInterval !== undefined
        ? config.heartbeatInterval
        : defaultHeartbeatInterval;
    this.heartbeatTimeout =
      config.heartbeatTimeout !== undefined
        ? config.heartbeatTimeout
        : defaultHeartbeatTimeout;
    this.handshakeTimeout =
      config.handshakeTimeout !== undefined
        ? config.handshakeTimeout
        : defaultHandshakeTimeout;
    this.maxInflight = config.maxInflight;

    this.disposes = [
      link.events.on("envelope", this.onEnvelope),
      link.events.on("close", () => {
        this.dispose("link is closed");
      }),
    ];

    this.handshake = setTimeout(() => {
      this.handshake = undefined;

      if (this.peer === undefined) {
        this.dispose("link handshake is timed out");
        this.link.close();
      }
    }, this.handshakeTimeout);
  }

  get peerId() {
    return this.peer;
  }

  get latency() {
    return this.rtt;
  }

  get transport() {
    return this.link.transport;
  }

  get local() {
    return this.link.local;
  }

  get setup() {
    return this.openedAt !== undefined
      ? this.openedAt - this.createdAt
      : undefined;
  }

  get queue() {
    return this.pending.size;
  }

  hello() {
    this.link.hello(this.token());
  }

  register(instance: Instance) {
    this.link.register(instance);
  }

  unregister(instance: Instance) {
    this.link.unregister(instance);
  }

  lookup(service: string) {
    return this.registry.lookup(service);
  }

  instances() {
    return this.registry.snapshot();
  }

  has(service: string, nodeId?: string) {
    return this.registry.has(service, nodeId);
  }

  watch(handler: WatchHandler): () => void;
  watch(service: string, handler: WatchHandler): () => void;
  watch(service: string | WatchHandler, handler?: WatchHandler) {
    if (typeof service === "function") {
      return this.registry.watch(service);
    }

    if (handler === undefined) {
      throw new SwarmError("rejected", "watch handler is required");
    }

    return this.registry.watch(service, handler);
  }

  call(
    to: string,
    service: string,
    method: string,
    args: unknown[],
    timeout: number,
    ttl?: number,
    target?: string,
    signal?: AbortSignal,
  ) {
    if (this.closed) {
      return Promise.reject(new SwarmError("failed", "link is closed", true));
    }

    if (
      this.maxInflight !== undefined &&
      this.pending.size >= this.maxInflight
    ) {
      return Promise.reject(
        new SwarmError("rejected", "too many in-flight calls"),
      );
    }

    return this.pending.call(
      (id) =>
        this.link.call(id, to, service, method, args, timeout, ttl, target),
      timeout,
      () => new SwarmError("timeout", `call timed out: ${service}.${method}`),
      (cause) =>
        cause instanceof Error && cause.name === "DataCloneError"
          ? new SwarmError("failed", "arguments are not cloneable")
          : cause instanceof SwarmError
            ? cause
            : new SwarmError(
                "failed",
                cause instanceof Error ? cause.message : String(cause),
                true,
              ),
      signal,
    );
  }

  publish(
    topic: string,
    data: unknown,
    origin: string,
    id: string,
    ttl: number,
  ) {
    this.link.publish(topic, data, origin, id, ttl);
  }

  peers(members: PeerEntry[]) {
    this.link.send({ t: "peers", from: this.link.nodeId, members });
  }

  bye() {
    if (this.closed) {
      return;
    }

    try {
      this.link.send({ t: "bye", from: this.link.nodeId });
    } catch {
      // The link is closed or congested; goodbye is best-effort.
    }
  }

  flush() {
    return this.link.flush();
  }

  close() {
    this.dispose("node is closed");
    this.link.close();
  }

  private open(from: string, reply: boolean, token?: string) {
    if (this.closed || !this.connect(from)) {
      return;
    }

    if (reply) {
      this.link.accept(token);
    }

    if (this.handshake !== undefined) {
      clearTimeout(this.handshake);
      this.handshake = undefined;
    }

    this.lastSeen = Date.now();
    this.openedAt = Date.now();

    if (this.heartbeat === undefined) {
      this.heartbeat = setInterval(() => this.beat(), this.heartbeatInterval);
    }

    this.announce();
    this.events.emit("open", from);
  }

  private beat() {
    if (this.lastSeen + this.heartbeatTimeout < Date.now()) {
      this.dispose("link is timed out");
      this.link.close();
      return;
    }

    if (this.pingAt !== undefined) {
      if (Date.now() - this.pingAt < this.heartbeatInterval) {
        return;
      }

      this.pingAt = undefined;
    }

    this.pingAt = Date.now();

    try {
      this.link.send({ t: "ping", from: this.link.nodeId });
    } catch {
      this.pingAt = undefined;
    }
  }

  private authenticate(
    from: string,
    token: string | undefined,
    reply: boolean,
  ) {
    if (this.closed || this.reject(from, token)) {
      return;
    }

    this.open(from, reply, reply ? this.token() : undefined);
  }

  private token() {
    return this.secret === undefined
      ? undefined
      : signToken(this.secret, this.link.nodeId);
  }

  private negotiate(version: number) {
    if (version === protocolVersion) {
      return true;
    }

    this.dispose(
      `protocol version ${version} is incompatible with ${protocolVersion}`,
    );
    this.link.close();

    return false;
  }

  private reject(from: string, token: string | undefined) {
    if (this.secret === undefined) {
      return false;
    }

    if (verifyToken(token, this.secret, from)) {
      return false;
    }

    this.dispose("link is closed");
    this.link.close();

    return true;
  }

  private announce() {
    for (const instance of this.provider.registrations()) {
      if (instance.nodeId === this.peer) {
        continue;
      }

      this.link.register(instance);
    }
  }

  private connect(nodeId: string) {
    if (this.peer !== undefined) {
      if (this.peer === nodeId) {
        return true;
      }

      this.dispose("link is closed");
      this.link.close();

      return false;
    }

    this.peer = nodeId;

    return true;
  }

  private dispose(message: string) {
    if (this.closed) {
      return;
    }

    this.closed = true;

    if (this.handshake !== undefined) {
      clearTimeout(this.handshake);
      this.handshake = undefined;
    }

    if (this.heartbeat !== undefined) {
      clearInterval(this.heartbeat);
      this.heartbeat = undefined;
    }

    this.pending.clear(() => new SwarmError("failed", message));

    for (const dispose of this.disposes) {
      dispose();
    }

    this.events.emit("close");
    this.registry.close();
  }
}
