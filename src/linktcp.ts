/*
 * linktcp.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { Link } from "./link.ts";
import { MusterError } from "./errors.ts";
import { decode, encode } from "./codec.ts";
import type { Socket } from "node:net";
import type { Envelope } from "./envelope.ts";

const headerSize = 4;
const maxFrameSize = 8 * 1024 * 1024;

export class LinkTcp extends Link {
  private readonly socket: Socket;
  private buffer: Uint8Array = new Uint8Array(0);
  private closed = false;

  private readonly onData = (chunk: Uint8Array) => {
    if (this.closed) {
      return;
    }

    if (this.buffer.byteLength === 0) {
      this.buffer = chunk;
    } else {
      const merged = new Uint8Array(this.buffer.byteLength + chunk.byteLength);

      merged.set(this.buffer, 0);
      merged.set(chunk, this.buffer.byteLength);

      this.buffer = merged;
    }

    while (this.buffer.byteLength >= headerSize) {
      const view = new DataView(
        this.buffer.buffer,
        this.buffer.byteOffset,
        headerSize,
      );
      const length = view.getUint32(0, false);

      if (length > maxFrameSize) {
        this.close();
        this.events.emit("close");
        return;
      }

      if (this.buffer.byteLength < headerSize + length) {
        return;
      }

      const frame = this.buffer.subarray(headerSize, headerSize + length);

      this.buffer = this.buffer.subarray(headerSize + length);

      const message = decode(frame);

      if (message !== undefined) {
        this.events.emit("envelope", message);
      }
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

    if (payload.byteLength > maxFrameSize) {
      throw new MusterError("failed", "envelope is too large");
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

    this.socket.off("data", this.onData);
    this.socket.off("error", this.onError);
    this.socket.off("close", this.onClose);
    this.socket.destroy();
  }
}
