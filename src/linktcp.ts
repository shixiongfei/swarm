/*
 * linktcp.ts
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
import type { Socket } from "node:net";
import type { Envelope } from "./envelope.ts";

const headerSize = 4;
const minBufferSize = 16;
const shrinkBufferSize = 64 * 1024;
const maxBufferSize = headerSize + maxEnvelopeSize;

export class LinkTcp extends Link {
  readonly transport = "socket";
  private readonly socket: Socket;
  private buffer: Uint8Array = new Uint8Array(0);
  private head = 0;
  private tail = 0;
  private closed = false;

  private readonly onData = (chunk: Uint8Array) => {
    if (this.closed) {
      return;
    }

    if (!this.append(chunk)) {
      this.abort();
      return;
    }

    this.drain();
  };

  private readonly onError = () => {
    this.abort();
  };

  private readonly onClose = () => {
    this.abort();
  };

  constructor(socket: Socket, nodeId: string) {
    super(nodeId);

    this.socket = socket;

    socket.setNoDelay(true);
    socket.on("data", this.onData);
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

    const frame = new Uint8Array(headerSize + payload.byteLength);

    new DataView(frame.buffer).setUint32(0, payload.byteLength, false);
    frame.set(payload, headerSize);

    this.socket.write(frame);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.buffer = new Uint8Array(0);
    this.head = 0;
    this.tail = 0;

    this.socket.off("data", this.onData);
    this.socket.off("error", this.onError);
    this.socket.off("close", this.onClose);
    this.socket.destroy();
  }

  private append(chunk: Uint8Array) {
    const pending = this.tail - this.head;
    const needed = pending + chunk.byteLength;

    if (needed > maxBufferSize) {
      return false;
    }

    if (needed > this.buffer.byteLength) {
      let capacity =
        this.buffer.byteLength === 0 ? minBufferSize : this.buffer.byteLength;

      while (capacity < needed) {
        capacity *= 2;
      }

      const buffer = new Uint8Array(Math.min(capacity, maxBufferSize));

      buffer.set(this.buffer.subarray(this.head, this.tail), 0);
      this.buffer = buffer;
      this.head = 0;
      this.tail = pending;
    } else if (this.tail + chunk.byteLength > this.buffer.byteLength) {
      this.buffer.copyWithin(0, this.head, this.tail);
      this.head = 0;
      this.tail = pending;
    }

    this.buffer.set(chunk, this.tail);
    this.tail += chunk.byteLength;

    return true;
  }

  private drain() {
    let pendingFrame = false;

    while (this.tail - this.head >= headerSize) {
      const view = new DataView(
        this.buffer.buffer,
        this.buffer.byteOffset + this.head,
        headerSize,
      );
      const length = view.getUint32(0, false);

      if (length > maxEnvelopeSize) {
        this.abort();
        return;
      }

      if (this.tail - this.head < headerSize + length) {
        pendingFrame = true;
        break;
      }

      const start = this.head + headerSize;
      const message = decode(this.buffer.subarray(start, start + length));

      this.head = start + length;

      if (message !== undefined) {
        this.events.emit("envelope", message);
      }
    }

    if (this.head === this.tail) {
      this.head = 0;
      this.tail = 0;
    }

    if (!pendingFrame) {
      this.shrink();
    }
  }

  private shrink() {
    const pending = this.tail - this.head;

    if (
      this.buffer.byteLength < shrinkBufferSize ||
      pending > this.buffer.byteLength / 4
    ) {
      return;
    }

    let capacity = minBufferSize;

    while (capacity < pending) {
      capacity *= 2;
    }

    const buffer = new Uint8Array(capacity);

    buffer.set(this.buffer.subarray(this.head, this.tail), 0);
    this.buffer = buffer;
    this.head = 0;
    this.tail = pending;
  }

  private abort() {
    this.close();
    this.events.emit("close");
  }
}
