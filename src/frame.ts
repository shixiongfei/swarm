/*
 * frame.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { encode } from "./codec.ts";
import type {
  Change,
  DownEnvelope,
  Envelope,
  PeerEntry,
  PubEnvelope,
  UpEnvelope,
} from "./envelope.ts";
import type { Instance } from "./registry.ts";

export type Frame = {
  readonly message: Envelope;
  bytes(): Uint8Array;
};

export function frame(message: Envelope) {
  let payload: Uint8Array | undefined;

  return {
    message,
    bytes() {
      payload ??= encode(message);

      return payload;
    },
  };
}

export function upChange(instance: Instance): UpEnvelope {
  const { nodeId, service, meta } = instance;

  return meta === undefined
    ? { t: "up", from: nodeId, service }
    : { t: "up", from: nodeId, service, meta };
}

export function downChange(instance: Instance): DownEnvelope {
  return { t: "down", from: instance.nodeId, service: instance.service };
}

export function changesFrame(items: Change[]) {
  return frame({ t: "changes", items });
}

export function peersFrame(from: string, members: PeerEntry[]) {
  return frame({ t: "peers", from, members });
}

export function pubsFrame(items: PubEnvelope[]) {
  return frame({ t: "pubs", items });
}
