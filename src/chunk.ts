/*
 * chunk.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { maxEnvelopeSize } from "./codec.ts";
import { SwarmError } from "./errors.ts";
import type { Frame } from "./frame.ts";

export const maxChunkItems = 1024;

export type Chunk<T> = { items: T[]; frame: Frame };
export type ChunkDrop<T> = { item: T; error: SwarmError };

export function chunk<T>(
  items: T[],
  frameOf: (items: T[]) => Frame,
  limit = maxEnvelopeSize,
) {
  const chunks: Chunk<T>[] = [];
  const dropped: ChunkDrop<T>[] = [];

  for (let at = 0; at < items.length; at += maxChunkItems) {
    split(items.slice(at, at + maxChunkItems), frameOf, limit, chunks, dropped);
  }

  return { chunks, dropped };
}

function split<T>(
  items: T[],
  frameOf: (items: T[]) => Frame,
  limit: number,
  chunks: Chunk<T>[],
  dropped: ChunkDrop<T>[],
) {
  if (items.length === 0) {
    return;
  }

  try {
    const frame = frameOf(items);

    if (limit === Infinity || frame.bytes().byteLength <= limit) {
      chunks.push({ items, frame });
      return;
    }

    if (items.length === 1) {
      dropped.push({
        item: items[0],
        error: new SwarmError(
          "failed",
          "ENVELOPE_INVALID",
          "envelope is too large",
          false,
        ),
      });

      return;
    }
  } catch (err) {
    if (items.length === 1) {
      dropped.push({
        item: items[0],
        error:
          err instanceof SwarmError
            ? err
            : new SwarmError(
                "failed",
                "ENVELOPE_INVALID",
                "envelope is not encodable",
                false,
              ),
      });

      return;
    }
  }

  const mid = items.length >> 1;

  split(items.slice(0, mid), frameOf, limit, chunks, dropped);
  split(items.slice(mid), frameOf, limit, chunks, dropped);
}
