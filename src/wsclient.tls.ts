/*
 * wsclient.tls.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import type { WsTlsOptions } from "./wsclient.ts";

export async function openTls(url: string | URL, tls: WsTlsOptions) {
  const { WebSocket: WsSocket } = await import("ws");

  return new WsSocket(url, undefined, tls) as unknown as WebSocket;
}
