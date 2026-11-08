/*
 * address.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

export function isAdvertisable(host: string) {
  return host !== "0.0.0.0" && host !== "::" && host !== "::ffff:0:0";
}

export function isLoopback(host: string | undefined) {
  if (host === undefined || host === "") {
    return false;
  }

  const name = host.toLowerCase();

  return (
    name === "localhost" ||
    name === "::1" ||
    name.startsWith("127.") ||
    name.startsWith("::ffff:127.")
  );
}
