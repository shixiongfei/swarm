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
import { SwarmError } from "./errors.ts";
import type { NodeLink } from "./nodelink.ts";

const maxSeenPubs = 1024;

export type PubSubOptions = {
  nodeId: string;
  maxHops: number;
  links: () => NodeLink[];
  local: (topic: string, data: unknown, origin: string) => void;
};

export class PubSub {
  private readonly nodeId: string;
  private readonly maxHops: number;
  private readonly links: PubSubOptions["links"];
  private readonly local: PubSubOptions["local"];
  private readonly seen = new Set<string>();

  constructor(options: PubSubOptions) {
    this.nodeId = options.nodeId;
    this.maxHops = options.maxHops;
    this.links = options.links;
    this.local = options.local;
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
    const remaining = ttl !== undefined ? ttl : this.maxHops;

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
      try {
        for (const nodeLink of this.links()) {
          if (from !== undefined && nodeLink.peerId === from) {
            continue;
          }

          nodeLink.publish(topic, data, source, pubId, remaining - 1);
        }
      } catch (err) {
        if (err instanceof Error && err.name === "DataCloneError") {
          throw new SwarmError("failed", "data is not cloneable");
        }

        throw err;
      }
    }

    this.local(topic, data, source);
  }

  close() {
    this.seen.clear();
  }
}
