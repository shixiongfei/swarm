/*
 * testutil.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { createServer } from "node:net";
import { createSocket } from "node:dgram";
import type { AddressInfo } from "node:net";

export async function freePort() {
  const server = createServer();

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));

  const port = (server.address() as AddressInfo).port;

  await new Promise<void>((resolve) => server.close(() => resolve()));

  return port;
}

export async function freeUdpPort(family: 4 | 6 = 4) {
  const socket = createSocket(family === 6 ? "udp6" : "udp4");

  await new Promise<void>((resolve) => socket.bind(0, resolve));

  const port = (socket.address() as AddressInfo).port;

  await new Promise<void>((resolve) => socket.close(resolve));

  return port;
}

export async function within<T>(promise: Promise<T>, ms: number) {
  let timer: ReturnType<typeof setTimeout> | undefined;

  const timeout = new Promise<undefined>((resolve) => {
    timer = setTimeout(() => resolve(undefined), ms);
  });

  const value = await Promise.race([promise, timeout]);

  if (timer !== undefined) {
    clearTimeout(timer);
  }

  return value;
}

export async function waitFor(
  check: () => boolean | Promise<boolean>,
  timeout = 2000,
) {
  const deadline = Date.now() + timeout;

  while (!(await check())) {
    if (Date.now() > deadline) {
      throw new Error("timed out");
    }

    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }

  return true;
}
