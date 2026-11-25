/*
 * serial.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { SwarmError } from "./errors.ts";
import type { CallContext, Methods } from "./local.ts";

const cancelled = () =>
  new SwarmError("cancelled", "CANCELLED", "call was cancelled");

export function serial<M extends Methods<M>>(methods: M): M {
  let tail: Promise<unknown> = Promise.resolve();

  const wrapped: Record<string, unknown> = {};

  for (const [name, value] of Object.entries(methods)) {
    if (typeof value !== "function") {
      wrapped[name] = value;
      continue;
    }

    wrapped[name] = (ctx: CallContext, ...args: unknown[]) => {
      const run = () => {
        if (ctx.signal.aborted) {
          const reason = ctx.signal.reason;

          throw reason instanceof SwarmError ? reason : cancelled();
        }

        return value.call(methods, ctx, ...args);
      };

      const call = tail.then(run, run);

      tail = call.then(
        () => {},
        () => {},
      );

      return call;
    };
  }

  return wrapped as unknown as M;
}
