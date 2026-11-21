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
import type { Envelope, PeerEntry } from "./envelope.ts";
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

export function upFrame(instance: Instance) {
  const { nodeId, service, meta } = instance;

  return frame(
    meta === undefined
      ? { t: "up", from: nodeId, service }
      : { t: "up", from: nodeId, service, meta },
  );
}

export function downFrame(instance: Instance) {
  return frame({ t: "down", from: instance.nodeId, service: instance.service });
}

export function peersFrame(from: string, members: PeerEntry[]) {
  return frame({ t: "peers", from, members });
}

export function pubFrame(
  nodeId: string,
  topic: string,
  data: unknown,
  origin: string,
  id: string,
  ttl: number,
) {
  return frame({ t: "pub", from: nodeId, origin, topic, id, ttl, data });
}
