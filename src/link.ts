/*
 * link.ts
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
import type { Instance } from "./registry.ts";
import type { Envelope, EnvelopeHandler, Enveloper } from "./envelope.ts";

export type LinkEvents = {
  envelope: EnvelopeHandler;
  close: () => void;
};

export type Transport = "port" | "socket" | "websocket";

export abstract class Link implements Enveloper {
  readonly nodeId: string;
  abstract readonly transport: Transport;
  abstract readonly local: boolean;
  readonly events = eventlite<LinkEvents>({
    allowDuplicate: true,
    isolate: true,
  });

  abstract send(message: Envelope): void;
  abstract close(): void;

  constructor(nodeId: string) {
    this.nodeId = nodeId;
  }

  hello(token?: string) {
    this.send(
      token === undefined
        ? { t: "hello", from: this.nodeId }
        : { t: "hello", from: this.nodeId, token },
    );
  }

  accept(token?: string) {
    this.send(
      token === undefined
        ? { t: "accept", from: this.nodeId }
        : { t: "accept", from: this.nodeId, token },
    );
  }

  register(instance: Instance) {
    const { nodeId, service, meta } = instance;

    this.send(
      meta === undefined
        ? { t: "up", from: nodeId, service }
        : { t: "up", from: nodeId, service, meta },
    );
  }

  unregister(instance: Instance) {
    this.send({ t: "down", from: instance.nodeId, service: instance.service });
  }

  call(
    id: string,
    to: string,
    service: string,
    method: string,
    args: unknown[],
    timeout?: number,
    ttl?: number,
    target?: string,
  ) {
    const message: Extract<Envelope, { t: "call" }> = {
      t: "call",
      id,
      from: this.nodeId,
      to,
      service,
      method,
      args,
    };

    if (target !== undefined) {
      message.target = target;
    }

    if (timeout !== undefined) {
      message.timeout = timeout;
    }

    if (ttl !== undefined) {
      message.ttl = ttl;
    }

    this.send(message);
  }

  reply(id: string, to: string, value: unknown) {
    this.send({ t: "reply", id, from: this.nodeId, to, value });
  }

  fail(id: string, to: string, error: unknown) {
    const kind = error instanceof SwarmError ? error.kind : "failed";
    const message = error instanceof Error ? error.message : String(error);

    this.send({ t: "error", id, from: this.nodeId, to, kind, message });
  }

  publish(
    topic: string,
    data: unknown,
    origin: string,
    id: string,
    ttl: number,
  ) {
    this.send({ t: "pub", from: this.nodeId, origin, topic, id, ttl, data });
  }
}
