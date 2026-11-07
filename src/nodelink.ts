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
import { PendingCalls } from "./pending.ts";
import { Registry } from "./registry.ts";
import { signToken, verifyToken } from "./token.ts";
import type { Link } from "./link.ts";
import type { Provider } from "./provider.ts";
import type { Envelope } from "./envelope.ts";
import type { Instance, Observable, WatchHandler } from "./registry.ts";

type NodeLinkEvents = {
  close: () => void;
};

const heartbeatInterval = 15000;
const heartbeatTimeout = 45000;

type NodeLinkConfig = {
  provider: Provider;
  secret?: string;
};

export class NodeLink implements Observable {
  readonly events = eventlite<NodeLinkEvents>({
    allowDuplicate: true,
    isolate: true,
  });
  readonly secret?: string;

  private readonly registry = new Registry();
  private readonly pending = new PendingCalls();
  private readonly provider: Provider;
  private readonly link: Link;
  private readonly disposes: Array<() => void>;
  private handshake?: Promise<void>;
  private peer?: string;
  private closed = false;
  private lastSeen = Date.now();
  private heartbeat?: ReturnType<typeof setInterval>;
  private pingAt?: number;
  private rtt?: number;

  private readonly onEnvelope = (message: Envelope) => {
    this.lastSeen = Date.now();

    if (this.handshake !== undefined) {
      this.handshake.then(() => {
        if (!this.closed) {
          this.onEnvelope(message);
        }
      });
      return;
    }

    switch (message.t) {
      case "hello":
        if (this.secret === undefined) {
          this.open(message.from, true);
          return;
        }

        this.handshake = this.authenticate(message.from, message.token, true)
          .catch(() => this.dispose("link is closed"))
          .finally(() => {
            this.handshake = undefined;
          });
        return;

      case "accept":
        if (this.secret === undefined) {
          this.open(message.from, false);
          return;
        }

        this.handshake = this.authenticate(message.from, message.token, false)
          .catch(() => this.dispose("link is closed"))
          .finally(() => {
            this.handshake = undefined;
          });
        return;

      case "up":
        if (this.peer !== undefined && message.from === this.peer) {
          this.registry.add(message.service, message.from, message.meta);
        }
        return;

      case "down":
        if (this.peer !== undefined && message.from === this.peer) {
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
    }
  };

  constructor(link: Link, config: NodeLinkConfig) {
    this.link = link;
    this.provider = config.provider;
    this.secret = config.secret;

    this.disposes = [
      link.events.on("envelope", this.onEnvelope),
      link.events.on("close", () => {
        this.dispose("link is closed");
      }),
    ];
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

  hello() {
    if (this.closed) {
      this.dispose("link is closed");
      return;
    }

    if (this.secret === undefined) {
      this.link.hello();
      return;
    }

    this.token().then((token) => this.link.hello(token));
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
  ) {
    if (this.closed) {
      return Promise.reject(new SwarmError("failed", "link is closed", true));
    }

    return this.pending.call(
      (id) => this.link.call(id, to, service, method, args, timeout),
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
    );
  }

  publish(topic: string, data: unknown, origin?: string) {
    this.link.publish(topic, data, origin);
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

    this.lastSeen = Date.now();

    if (this.heartbeat === undefined) {
      this.heartbeat = setInterval(() => this.beat(), heartbeatInterval);
    }

    this.announce();
  }

  private beat() {
    if (this.lastSeen + heartbeatTimeout < Date.now()) {
      this.dispose("link is timed out");
      this.link.close();
      return;
    }

    if (this.pingAt !== undefined) {
      return;
    }

    this.pingAt = Date.now();
    this.link.send({ t: "ping", from: this.link.nodeId });
  }

  private async authenticate(
    from: string,
    token: string | undefined,
    reply: boolean,
  ) {
    if (this.closed || (await this.reject(from, token))) {
      return;
    }

    this.open(from, reply, reply ? await this.token() : undefined);
  }

  private async token() {
    return this.secret === undefined
      ? undefined
      : signToken(this.secret, this.link.nodeId);
  }

  private async reject(from: string, token: string | undefined) {
    if (this.secret === undefined) {
      return false;
    }

    if (await verifyToken(token, this.secret, from)) {
      return false;
    }

    this.dispose("link is closed");
    this.link.close();

    return true;
  }

  private announce() {
    for (const instance of this.provider.registrations()) {
      this.link.register({
        service: instance.service,
        nodeId: this.link.nodeId,
        meta: instance.meta,
      });
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
    this.handshake = undefined;

    if (this.heartbeat !== undefined) {
      clearInterval(this.heartbeat);
      this.heartbeat = undefined;
    }

    this.pending.clear(() => new SwarmError("failed", message, true));

    for (const dispose of this.disposes) {
      dispose();
    }

    this.events.emit("close");
    this.registry.close();
  }
}
