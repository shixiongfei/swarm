/*
 * linkstream.ts
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
import { isLoopback } from "./address.ts";
import { decode, maxEnvelopeSize } from "./codec.ts";
import type { Socket } from "node:net";
import type { Frame } from "./frame.ts";

const headerSize = 4;
const minBufferSize = 16;
const shrinkBufferSize = 64 * 1024;
const maxBufferSize = headerSize + maxEnvelopeSize;
const maxQueueSize = maxEnvelopeSize * 2;
const closeDrainTimeout = 100;

export class LinkStream extends Link {
  readonly transport = "socket";
  readonly local: boolean;
  private readonly socket: Socket;
  private buffer: Uint8Array = new Uint8Array(0);
  private head = 0;
  private tail = 0;
  private closed = false;
  private writable = true;
  private queued = 0;
  private readonly queue: Uint8Array[] = [];

  private readonly onData = (chunk: Uint8Array) => {
    if (this.closed) {
      return;
    }

    let offset = 0;

    while (offset < chunk.byteLength) {
      const room = maxBufferSize - (this.tail - this.head);

      if (room <= 0) {
        this.abort();
        return;
      }

      const end = Math.min(offset + room, chunk.byteLength);

      if (!this.append(chunk.subarray(offset, end))) {
        this.abort();
        return;
      }

      offset = end;

      if (offset < chunk.byteLength) {
        this.drain();

        if (this.closed) {
          return;
        }
      }
    }

    this.drain();
  };

  private readonly onError = () => {
    this.abort();
  };

  private readonly onClose = () => {
    this.abort();
  };

  private readonly onDrain = () => {
    if (this.closed) {
      return;
    }

    this.writable = true;

    while (this.writable && this.queue.length > 0) {
      const packet = this.queue.shift();

      if (packet === undefined) {
        break;
      }

      this.queued -= packet.byteLength;
      this.writable = this.socket.write(packet) !== false;
    }
  };

  constructor(socket: Socket, nodeId: string, local?: boolean) {
    super(nodeId);

    this.socket = socket;
    this.local = local !== undefined ? local : isLoopback(socket.remoteAddress);

    socket.setNoDelay(true);
    socket.on("data", this.onData);
    socket.on("error", this.onError);
    socket.on("close", this.onClose);
    socket.on("drain", this.onDrain);
  }

  send(frame: Frame) {
    if (this.closed) {
      return;
    }

    this.write(frame.bytes());
  }

  private write(payload: Uint8Array) {
    if (payload.byteLength > maxEnvelopeSize) {
      throw new SwarmError(
        "failed",
        "ENVELOPE_INVALID",
        "envelope is too large",
        false,
      );
    }

    const packet = new Uint8Array(headerSize + payload.byteLength);

    new DataView(packet.buffer).setUint32(0, payload.byteLength, false);
    packet.set(payload, headerSize);

    if (this.writable === false) {
      if (this.queued + packet.byteLength > maxQueueSize) {
        throw new SwarmError("failed", "CONGESTED", "link is congested", true);
      }

      this.queued += packet.byteLength;
      this.queue.push(packet);
      return;
    }

    this.writable = this.socket.write(packet) !== false;
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
    this.socket.off("drain", this.onDrain);

    if (this.socket.destroyed) {
      this.queue.length = 0;
      this.queued = 0;
      return;
    }

    this.socket.once("error", () => this.socket.destroy());

    while (this.queue.length > 0) {
      const packet = this.queue.shift();

      if (packet !== undefined) {
        this.socket.write(packet);
      }
    }

    this.queued = 0;

    this.socket.end();

    const timer = setTimeout(() => this.socket.destroy(), closeDrainTimeout);

    this.socket.once("close", () => clearTimeout(timer));
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
