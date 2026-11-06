/*
 * random.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

function hex(bytes: Uint8Array) {
  let id = "";
  for (let i = 0; i < bytes.length; i += 1) {
    id += (bytes[i] + 0x100).toString(16).slice(1);
  }
  return id;
}

const randomBytes =
  typeof crypto !== "undefined" && crypto.getRandomValues !== undefined
    ? () => crypto.getRandomValues(new Uint8Array(16))
    : () => {
        const bytes = new Uint8Array(16);
        for (let i = 0; i < bytes.length; i += 1) {
          bytes[i] = Math.floor(Math.random() * 256);
        }
        return bytes;
      };

export const randomId =
  typeof crypto !== "undefined" && crypto.randomUUID !== undefined
    ? crypto.randomUUID.bind(crypto)
    : () => hex(randomBytes());
