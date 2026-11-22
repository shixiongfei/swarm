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
import { frame } from "./frame.ts";
import { protocolVersion } from "./envelope.ts";
import type { Frame } from "./frame.ts";
import type { Envelope, EnvelopeHandler } from "./envelope.ts";

export type LinkEvents = {
  envelope: EnvelopeHandler;
  drop: (error: SwarmError) => void;
  close: () => void;
};

export type LinkTransport = "port" | "socket" | "websocket";

export abstract class Link {
  readonly nodeId: string;
  abstract readonly transport: LinkTransport;
  abstract readonly local: boolean;
  readonly events = eventlite<LinkEvents>({
    allowDuplicate: true,
    isolate: true,
  });

  abstract send(frame: Frame): void;
  abstract close(): void;

  constructor(nodeId: string) {
    this.nodeId = nodeId;
  }

  protected drop(message: string) {
    this.events.emit(
      "drop",
      new SwarmError("failed", "FRAME_DROPPED", message, false),
    );
  }

  hello(startup: number, token: string) {
    this.handshake("hello", startup, token);
  }

  accept(startup: number, token: string) {
    this.handshake("accept", startup, token);
  }

  private handshake(t: "hello" | "accept", startup: number, token: string) {
    this.send(
      frame({
        t,
        from: this.nodeId,
        version: protocolVersion,
        startup,
        token,
      } as Envelope),
    );
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

    this.send(frame(message));
  }

  reply(id: string, to: string, value: unknown) {
    this.send(frame({ t: "reply", id, from: this.nodeId, to, value }));
  }

  fail(id: string, to: string, error: unknown) {
    const kind = error instanceof SwarmError ? error.kind : "failed";
    const code = error instanceof SwarmError ? error.code : "FAILED";
    const retriable = error instanceof SwarmError ? error.retriable : false;
    const message = error instanceof Error ? error.message : String(error);

    this.send(
      frame({
        t: "error",
        id,
        from: this.nodeId,
        to,
        kind,
        code,
        retriable,
        message,
      }),
    );
  }
}
