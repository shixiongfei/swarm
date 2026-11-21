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
import { encode } from "./codec.ts";
import { protocolVersion } from "./envelope.ts";
import type { Instance } from "./registry.ts";
import type { Envelope, EnvelopeHandler, Enveloper } from "./envelope.ts";

export type LinkEvents = {
  envelope: EnvelopeHandler;
  close: () => void;
};

export type LinkTransport = "port" | "socket" | "websocket";

export type PubFrame = {
  readonly message: Extract<Envelope, { t: "pub" }>;
  bytes(): Uint8Array;
};

export function pubFrame(
  nodeId: string,
  topic: string,
  data: unknown,
  origin: string,
  id: string,
  ttl: number,
): PubFrame {
  const message: Extract<Envelope, { t: "pub" }> = {
    t: "pub",
    from: nodeId,
    origin,
    topic,
    id,
    ttl,
    data,
  };

  let payload: Uint8Array | undefined;

  return {
    message,
    bytes() {
      payload ??= encode(message);

      return payload;
    },
  };
}

export abstract class Link implements Enveloper {
  readonly nodeId: string;
  abstract readonly transport: LinkTransport;
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

  hello(startup: number, token: string) {
    this.handshake("hello", startup, token);
  }

  accept(startup: number, token: string) {
    this.handshake("accept", startup, token);
  }

  private handshake(t: "hello" | "accept", startup: number, token: string) {
    this.send({
      t,
      from: this.nodeId,
      version: protocolVersion,
      startup,
      token,
    } as Envelope);
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
    timeout: number,
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
      timeout,
    };

    if (target !== undefined) {
      message.target = target;
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
    const code = error instanceof SwarmError ? error.code : "FAILED";
    const retriable = error instanceof SwarmError ? error.retriable : false;
    const message = error instanceof Error ? error.message : String(error);

    this.send({
      t: "error",
      id,
      from: this.nodeId,
      to,
      kind,
      code,
      retriable,
      message,
    });
  }

  publish(frame: PubFrame) {
    this.sendPayload(frame);
  }

  protected sendPayload(frame: PubFrame) {
    this.send(frame.message);
  }
}
