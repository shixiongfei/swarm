/*
 * linkport.ts
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
import { parseEnvelope } from "./envelope.ts";
import type { Envelope } from "./envelope.ts";

const linked = new WeakSet<MessagePort>();

export class LinkPort extends Link {
  private readonly port: MessagePort;

  private readonly dispose: () => void;
  private closed = false;

  constructor(port: MessagePort, nodeId: string) {
    super(nodeId);

    if (linked.has(port)) {
      throw new SwarmError("rejected", "port is already linked");
    }

    linked.add(port);

    this.port = port;

    const onMessage = (event: MessageEvent) => {
      const message = parseEnvelope(event.data);

      if (message === undefined) {
        return;
      }

      this.events.emit("envelope", message);
    };

    const onClose = () => {
      this.close();
      this.events.emit("close");
    };

    this.dispose = () => {
      this.port.removeEventListener("message", onMessage);
      this.port.removeEventListener("close", onClose);
    };

    this.port.addEventListener("message", onMessage);
    this.port.addEventListener("close", onClose);
    this.port.start();
  }

  send(message: Envelope) {
    if (this.closed) {
      return;
    }

    try {
      this.port.postMessage(message);
    } catch (err) {
      if (err instanceof Error && err.name === "DataCloneError") {
        throw err;
      }
    }
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.dispose();
    this.port.close();
  }
}
