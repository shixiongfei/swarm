/*
 * linkws.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { Link } from "./link.ts";
import { SwarmError } from "./errors.ts";
import { decode, encode, maxEnvelopeSize } from "./codec.ts";
import type { Envelope } from "./envelope.ts";
import type { RawData, WebSocket } from "ws";

function toBytes(data: RawData): Uint8Array {
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }

  if (Array.isArray(data)) {
    const length = data.reduce((total, chunk) => total + chunk.byteLength, 0);
    const merged = new Uint8Array(length);
    let offset = 0;

    for (const chunk of data) {
      merged.set(chunk, offset);
      offset += chunk.byteLength;
    }

    return merged;
  }

  return data;
}

export class LinkWs extends Link {
  readonly transport = "socket";
  private readonly socket: WebSocket;
  private closed = false;

  private readonly onMessage = (data: RawData) => {
    if (this.closed) {
      return;
    }

    const message = decode(toBytes(data));

    if (message !== undefined) {
      this.events.emit("envelope", message);
    }
  };

  private readonly onError = () => {
    this.close();
    this.events.emit("close");
  };

  private readonly onClose = () => {
    this.close();
    this.events.emit("close");
  };

  constructor(socket: WebSocket, nodeId: string) {
    super(nodeId);

    this.socket = socket;

    socket.on("message", this.onMessage);
    socket.on("error", this.onError);
    socket.on("close", this.onClose);
  }

  send(message: Envelope) {
    if (this.closed) {
      return;
    }

    const payload = encode(message);

    if (payload.byteLength > maxEnvelopeSize) {
      throw new SwarmError("failed", "envelope is too large");
    }

    this.socket.send(payload, { binary: true });
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    this.socket.off("message", this.onMessage);
    this.socket.off("error", this.onError);
    this.socket.off("close", this.onClose);
    this.socket.terminate();
  }
}
