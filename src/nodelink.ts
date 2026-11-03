/*
 * nodelink.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { eventlite } from "eventlite.js";
import { MusterError } from "./errors.ts";
import { PendingCalls } from "./pending.ts";
import { Registry } from "./registry.ts";
import type { Link } from "./link.ts";
import type { Provider } from "./provider.ts";
import type { Envelope, Service } from "./envelope.ts";
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
  private peerId?: string;
  private closed = false;

  private readonly onEnvelope = (message: Envelope) => {
    switch (message.t) {
      case "hello":
        if (this.secret !== undefined && message.secret !== this.secret) {
          this.dispose("link is closed");
          this.link.close();
          return;
        }

        if (this.connect(message.from, message.services)) {
          this.link.accept(this.provider.registrations(), this.secret);
        }
        return;

      case "accept":
        if (this.secret !== undefined && message.secret !== this.secret) {
          this.dispose("link is closed");
          this.link.close();
          return;
        }

        this.connect(message.from, message.services);
        return;

      case "up":
        if (this.peerId !== undefined && message.from === this.peerId) {
          this.registry.add(message.service, message.from, message.meta);
        }
        return;

      case "down":
        if (this.peerId !== undefined && message.from === this.peerId) {
          this.registry.remove(message.service, message.from);
        }
        return;

      case "call":
        if (
          this.peerId === undefined ||
          message.from !== this.peerId ||
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
                  new MusterError("failed", "reply is not cloneable"),
                );
              }
            },
            (error: unknown) => this.link.fail(message.id, message.from, error),
          );
        return;

      case "reply":
        if (
          this.peerId !== undefined &&
          message.from === this.peerId &&
          message.to === this.link.nodeId
        ) {
          this.pending.settle(message.id, message.value);
        }
        return;

      case "error":
        if (
          this.peerId !== undefined &&
          message.from === this.peerId &&
          message.to === this.link.nodeId
        ) {
          this.pending.reject(
            message.id,
            new MusterError(message.kind, message.message),
          );
        }
        return;

      case "pub":
        if (this.peerId !== undefined && message.from === this.peerId) {
          this.provider.publish(message.topic, message.data, message.from);
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

  hello() {
    this.link.hello(this.provider.registrations(), this.secret);
  }

  register(instance: Instance) {
    if (this.peerId === undefined) {
      return;
    }

    this.link.register(instance);
  }

  unregister(instance: Instance) {
    if (this.peerId === undefined) {
      return;
    }

    this.link.unregister(instance);
  }

  lookup(service: string) {
    return this.registry.lookup(service);
  }

  watch(service: string, handler: WatchHandler) {
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
      return Promise.reject(new MusterError("failed", "link is closed"));
    }

    return this.pending.call(
      (id) => this.link.call(id, to, service, method, args),
      timeout,
      () => new MusterError("timeout", `call timed out: ${service}.${method}`),
      (cause) =>
        cause instanceof Error && cause.name === "DataCloneError"
          ? new MusterError("failed", "arguments are not cloneable")
          : new MusterError(
              "failed",
              cause instanceof Error ? cause.message : String(cause),
            ),
    );
  }

  publish(topic: string, data: unknown) {
    if (this.peerId === undefined) {
      return;
    }

    this.link.publish(topic, data);
  }

  close() {
    this.dispose("node is closed");
    this.link.close();
  }

  private connect(nodeId: string, services: Service[]) {
    if (this.peerId !== undefined) {
      if (this.peerId === nodeId) {
        return true;
      }

      this.dispose("link is closed");
      this.link.close();

      return false;
    }

    this.peerId = nodeId;

    for (const service of services) {
      this.registry.add(service.name, nodeId, service.meta);
    }

    return true;
  }

  private dispose(message: string) {
    if (this.closed) {
      return;
    }

    this.closed = true;

    this.pending.clear(() => new MusterError("failed", message));

    for (const dispose of this.disposes) {
      dispose();
    }

    this.events.emit("close");
    this.registry.close();
  }
}
