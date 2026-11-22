/*
 * pubsub.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { randomId } from "./random.ts";
import { chunk } from "./chunk.ts";
import { maxEnvelopeSize } from "./codec.ts";
import { SwarmError } from "./errors.ts";
import { pubsFrame } from "./frame.ts";
import type { PubEnvelope } from "./envelope.ts";
import type { NodeLink } from "./nodelink.ts";

const maxSeenPubs = 1024;

export type PubSubOptions = {
  nodeId: string;
  maxHops: number;
  links: () => NodeLink[];
  local: (topic: string, data: unknown, origin: string) => void;
  drop: (error: SwarmError) => void;
};

type QueuedPub = { skip: string | undefined; item: PubEnvelope };

export class PubSub {
  private readonly nodeId: string;
  private readonly maxHops: number;
  private readonly links: PubSubOptions["links"];
  private readonly local: PubSubOptions["local"];
  private readonly drop: PubSubOptions["drop"];
  private readonly seen = new Set<string>();
  private queue: QueuedPub[] = [];
  private scheduled = false;

  constructor(options: PubSubOptions) {
    this.nodeId = options.nodeId;
    this.maxHops = options.maxHops;
    this.links = options.links;
    this.local = options.local;
    this.drop = options.drop;
  }

  publish(
    topic: string,
    data: unknown,
    origin?: string,
    from?: string,
    id?: string,
    ttl?: number,
  ) {
    const source = origin !== undefined ? origin : this.nodeId;
    const pubId = id !== undefined ? id : randomId();
    const remaining =
      ttl !== undefined ? Math.min(ttl, this.maxHops) : this.maxHops;

    if (
      typeof pubId !== "string" ||
      !Number.isInteger(remaining) ||
      remaining < 0
    ) {
      this.reportDrop(
        new SwarmError(
          "failed",
          "PUB_DROPPED",
          typeof pubId !== "string" ? "invalid id" : "invalid ttl",
          false,
        ),
      );
    } else {
      if (this.seen.has(pubId)) {
        return;
      }

      this.seen.add(pubId);

      if (this.seen.size > maxSeenPubs) {
        const oldest = this.seen.values().next().value;

        if (oldest !== undefined) {
          this.seen.delete(oldest);
        }
      }

      if (remaining > 0) {
        this.queue.push({
          skip: from,
          item: {
            t: "pub",
            from: this.nodeId,
            origin: source,
            topic,
            id: pubId,
            ttl: remaining - 1,
            data,
          },
        });

        if (!this.scheduled) {
          this.scheduled = true;
          queueMicrotask(() => this.flush());
        }
      }
    }

    this.local(topic, data, source);
  }

  close() {
    this.seen.clear();
    this.queue.length = 0;
  }

  private flush() {
    this.scheduled = false;

    if (this.queue.length === 0) {
      return;
    }

    const queue = this.queue;
    this.queue = [];

    const links = this.links();
    let limit = Infinity;

    for (const nodeLink of links) {
      if (nodeLink.transport !== "port") {
        limit = maxEnvelopeSize;
        break;
      }
    }

    const { chunks, dropped } = chunk(queue, (batch) =>
      pubsFrame(batch.map((queued) => queued.item)), limit,
    );

    for (const { error } of dropped) {
      this.reportDrop(error);
    }

    for (const batch of chunks) {
      for (const nodeLink of links) {
        const queued = batch.items.filter(
          (item) => item.skip === undefined || item.skip !== nodeLink.peerId,
        );

        if (queued.length === 0) {
          continue;
        }

        try {
          nodeLink.publish(
            queued.length === batch.items.length
              ? batch.frame
              : pubsFrame(queued.map((item) => item.item)),
          );
        } catch (err) {
          this.reportDrop(err);
        }
      }
    }
  }

  private reportDrop(error: unknown) {
    const message =
      error instanceof Error && error.name === "DataCloneError"
        ? "data is not cloneable"
        : error instanceof Error
          ? error.message
          : "publish failed";

    this.drop(new SwarmError("failed", "PUB_DROPPED", message, false));
  }
}
