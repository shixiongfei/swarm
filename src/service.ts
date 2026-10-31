/*
 * service.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { randomId } from "./random.ts";
import { MusterError } from "./errors.ts";
import { Local } from "./local.ts";
import { Link } from "./link.ts";
import type { ErrorKind } from "./errors.ts";
import type { Channel } from "./link.ts";
import type { Methods } from "./local.ts";
import type { WatchHandler } from "./registry.ts";

export type ServiceConfig = {
  nodeId: string;
};

type Call = {
  t: "call";
  id: string;
  service: string;
  method: string;
  args: unknown[];
  timeout: number;
};

type Envelope =
  | Call
  | { t: "reply"; id: string; value: unknown }
  | { t: "error"; id: string; kind: ErrorKind; message: string }
  | {
      t: "reg";
      nodeId: string;
      service: string;
      meta?: Record<string, unknown>;
    }
  | { t: "unreg"; nodeId: string; service: string }
  | { t: "pub"; from: string; topic: string; data: unknown };

type Peer = {
  link: Link;
  services: Set<string>;
};

export class Service {
  private readonly nodeId: string;
  private readonly local: Local;
  private readonly links = new Set<Link>();
  private readonly peers = new Map<string, Peer>();

  constructor(config: ServiceConfig) {
    this.nodeId = config.nodeId;
    this.local = new Local({ nodeId: config.nodeId });
  }

  register(service: string, methods: Methods, meta?: Record<string, unknown>) {
    this.local.register(service, methods, meta);
    this.notify(
      meta === undefined
        ? { t: "reg", nodeId: this.nodeId, service }
        : { t: "reg", nodeId: this.nodeId, service, meta },
    );
  }

  unregister(service: string) {
    this.local.unregister(service);
    this.notify({ t: "unreg", nodeId: this.nodeId, service });
  }

  attach(target: Channel) {
    const link = new Link(target, {
      receive: (message) => this.receive(link, message),
      close: () => this.forget(link),
    });

    this.links.add(link);

    return () => {
      this.links.delete(link);
      link.close();
    };
  }

  invoke(service: string, method: string, args: unknown[], timeout: number) {
    return this.dispatch(undefined, service, method, args, timeout);
  }

  private async dispatch(
    sender: Link | undefined,
    service: string,
    method: string,
    args: unknown[],
    timeout: number,
  ) {
    if (this.local.has(service)) {
      return this.local.invoke(service, method, args, timeout);
    }

    const link = this.resolve(service, sender);

    if (link === undefined) {
      throw new MusterError("not-found", `service not found: ${service}`);
    }

    const id = randomId();
    const call: Call = { t: "call", id, service, method, args, timeout };

    try {
      return await link.request(id, call, timeout);
    } catch (err) {
      if (err instanceof MusterError && err.kind === "timeout") {
        throw new MusterError(
          "timeout",
          `call timed out: ${service}.${method}`,
        );
      }

      throw err;
    }
  }

  lookup(service: string) {
    return this.local.lookup(service);
  }

  watch(service: string, handler: WatchHandler) {
    return this.local.watch(service, handler);
  }

  subscribe(topic: string, handler: (data: unknown, from: string) => void) {
    return this.local.subscribe(topic, handler);
  }

  publish(topic: string, data: unknown) {
    this.local.publish(topic, data, this.nodeId);
    this.notify({ t: "pub", from: this.nodeId, topic, data });
  }

  close() {
    for (const link of this.links) {
      link.close();
    }

    this.links.clear();
    this.peers.clear();
    this.local.close();
  }

  private notify(message: Envelope) {
    for (const link of this.links) {
      link.send(message);
    }
  }

  private forward(sender: Link, message: Envelope) {
    for (const link of this.links) {
      if (link !== sender) {
        link.send(message);
      }
    }
  }

  private receive(link: Link, message: unknown) {
    if (typeof message !== "object" || message === null) {
      return;
    }

    const envelope = message as Envelope;

    switch (envelope.t) {
      case "reg": {
        if (envelope.nodeId === this.nodeId) {
          return;
        }

        let peer = this.peers.get(envelope.nodeId);

        if (peer === undefined) {
          peer = { link, services: new Set() };
          this.peers.set(envelope.nodeId, peer);
        } else {
          peer.link = link;
        }

        if (!peer.services.has(envelope.service)) {
          peer.services.add(envelope.service);
          this.local.mirror(envelope.service, envelope.nodeId, envelope.meta);
        }

        return;
      }

      case "unreg": {
        const peer = this.peers.get(envelope.nodeId);

        if (peer === undefined) {
          return;
        }

        this.local.drop(envelope.service, envelope.nodeId);
        peer.services.delete(envelope.service);

        if (peer.services.size === 0) {
          this.peers.delete(envelope.nodeId);
        }

        return;
      }

      case "call":
        this.answer(link, envelope);
        return;

      case "reply":
        link.resolve(envelope.id, envelope.value);
        return;

      case "error":
        link.reject(envelope.id, envelope.kind, envelope.message);
        return;

      case "pub":
        this.local.publish(envelope.topic, envelope.data, envelope.from);
        this.forward(link, envelope);
        return;
    }
  }

  private answer(link: Link, call: Call) {
    this.dispatch(
      link,
      call.service,
      call.method,
      call.args,
      call.timeout,
    ).then(
      (value) => link.send({ t: "reply", id: call.id, value }),
      (err: unknown) => {
        const known = err instanceof MusterError;

        link.send({
          t: "error",
          id: call.id,
          kind: known ? err.kind : "failed",
          message: known
            ? err.message
            : err instanceof Error
              ? err.message
              : String(err),
        });
      },
    );
  }

  private resolve(service: string, sender: Link | undefined) {
    for (const instance of this.local.lookup(service)) {
      if (instance.owner === this.nodeId) {
        continue;
      }

      const peer = this.peers.get(instance.owner);

      if (peer !== undefined && peer.link !== sender) {
        return peer.link;
      }
    }

    let only: Link | undefined;

    for (const link of this.links) {
      if (link === sender) {
        continue;
      }

      if (only !== undefined) {
        return undefined;
      }

      only = link;
    }

    return only;
  }

  private forget(link: Link) {
    this.links.delete(link);

    for (const [nodeId, peer] of this.peers) {
      if (peer.link === link) {
        this.peers.delete(nodeId);
        this.local.dropOwner(nodeId);
      }
    }
  }
}
