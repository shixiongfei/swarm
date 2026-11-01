/*
 * link.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import { eventlite } from "eventlite.js";
import { MusterError } from "./errors.ts";
import type { Instance } from "./registry.ts";
import type {
  Envelope,
  EnvelopeHandler,
  Enveloper,
  Service,
} from "./envelope.ts";

export type LinkEvents = {
  envelope: EnvelopeHandler;
  close: () => void;
};

export abstract class Link implements Enveloper {
  readonly events = eventlite<LinkEvents>();
  nodeId?: string;

  abstract send(message: Envelope): void;
  abstract close(): void;

  setNodeId(nodeId: string) {
    this.nodeId = nodeId;
  }

  hello(nodeId: string, instances: Instance[], secret?: string) {
    const services: Service[] = instances.map((instance) =>
      instance.meta === undefined
        ? { name: instance.service }
        : { name: instance.service, meta: instance.meta },
    );

    this.send(
      secret === undefined
        ? { t: "hello", nodeId, services }
        : { t: "hello", nodeId, services, secret },
    );
  }

  accept(nodeId: string, instances: Instance[]) {
    const services: Service[] = instances.map((instance) =>
      instance.meta === undefined
        ? { name: instance.service }
        : { name: instance.service, meta: instance.meta },
    );

    this.send({ t: "accept", nodeId, services });
  }

  register(instance: Instance) {
    const { nodeId, service, meta } = instance;

    this.send(
      meta === undefined
        ? { t: "up", nodeId, service }
        : { t: "up", nodeId, service, meta },
    );
  }

  unregister(instance: Instance) {
    this.send({
      t: "down",
      nodeId: instance.nodeId,
      service: instance.service,
    });
  }

  call(id: string, service: string, method: string, args: unknown[]) {
    this.send({ t: "call", id, service, method, args });
  }

  reply(id: string, value: unknown) {
    this.send({ t: "reply", id, value });
  }

  fail(id: string, error: unknown) {
    const kind = error instanceof MusterError ? error.kind : "failed";
    const message = error instanceof Error ? error.message : String(error);

    this.send({ t: "error", id, kind, message });
  }

  publish(topic: string, data: unknown, nodeId: string) {
    this.send({ t: "pub", nodeId, topic, data });
  }
}
