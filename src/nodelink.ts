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
  private peer?: string;
  private closed = false;

  private readonly onEnvelope = (message: Envelope) => {
    switch (message.t) {
      case "hello":
        if (this.reject(message)) {
          return;
        }

        if (this.connect(message.from)) {
          this.link.accept(this.token());
          this.announce();
        }
        return;

      case "accept":
        if (this.reject(message)) {
          return;
        }

        if (this.connect(message.from)) {
          this.announce();
        }
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
            this.provider.invoke(message.service, message.method, message.args),
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
    }
  };

  private readonly disposes: Array<() => void>;

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

  get transport() {
    return this.link.transport;
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
      (id) => this.link.call(id, to, service, method, args),
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

  private token() {
    return this.secret === undefined
      ? undefined
      : signToken(this.secret, this.link.nodeId);
  }

  private reject(message: { from: string; token?: string }) {
    if (
      this.secret !== undefined &&
      !verifyToken(message.token, this.secret, message.from)
    ) {
      this.dispose("link is closed");
      this.link.close();

      return true;
    }

    return false;
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

    this.pending.clear(() => new SwarmError("failed", message, true));

    for (const dispose of this.disposes) {
      dispose();
    }

    this.events.emit("close");
    this.registry.close();
  }
}
