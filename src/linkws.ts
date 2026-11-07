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

function toBytes(data: unknown): Uint8Array | undefined {
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }

  if (Array.isArray(data)) {
    const length = data.reduce(
      (total, chunk) => total + (chunk as Uint8Array).byteLength,
      0,
    );

    const merged = new Uint8Array(length);
    let offset = 0;

    for (const chunk of data as Uint8Array[]) {
      merged.set(chunk, offset);
      offset += chunk.byteLength;
    }

    return merged;
  }

  if (data instanceof Uint8Array) {
    return data;
  }

  return undefined;
}

export class LinkWs extends Link {
  readonly transport = "socket";
  private readonly socket: WebSocket;
  private closed = false;

  private readonly onMessage = (event: unknown) => {
    if (this.closed) {
      return;
    }

    const data = toBytes((event as { data?: unknown }).data);

    if (data === undefined) {
      return;
    }

    const message = decode(data);

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
    this.socket.binaryType = "arraybuffer";

    socket.addEventListener("message", this.onMessage);
    socket.addEventListener("error", this.onError);
    socket.addEventListener("close", this.onClose);
  }

  send(message: Envelope) {
    if (this.closed) {
      return;
    }

    const payload = encode(message);

    if (payload.byteLength > maxEnvelopeSize) {
      throw new SwarmError("failed", "envelope is too large");
    }

    this.socket.send(payload as Uint8Array<ArrayBuffer>);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    this.socket.removeEventListener("message", this.onMessage);
    this.socket.removeEventListener("error", this.onError);
    this.socket.removeEventListener("close", this.onClose);
    this.socket.close();
  }
}
