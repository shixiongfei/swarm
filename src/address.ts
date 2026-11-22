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

import { SwarmError } from "./errors.ts";

export type ParsedUrl = {
  protocol: "tcp:" | "ws:";
  secure: boolean;
  host: string;
  port: number;
  portOmitted: boolean;
  path: string;
  secret: string;
};

export function parseUrl(url: string | URL): ParsedUrl {
  let parsed: URL;

  try {
    parsed = new URL(url);
  } catch {
    throw new SwarmError("rejected", "REJECTED", "invalid url");
  }

  if (
    (parsed.protocol !== "tcp:" &&
      parsed.protocol !== "ws:" &&
      parsed.protocol !== "wss:") ||
    parsed.hostname === "" ||
    (parsed.protocol === "tcp:" && parsed.port === "")
  ) {
    throw new SwarmError(
      "rejected",
      "REJECTED",
      `invalid url: ${parsed.protocol}//${parsed.host}`,
    );
  }

  const portOmitted = parsed.port === "";
  const port = portOmitted
    ? parsed.protocol === "wss:"
      ? 443
      : 80
    : Number(parsed.port);

  if (!Number.isInteger(port) || port > 65535) {
    throw new SwarmError("rejected", "REJECTED", `invalid port: ${port}`);
  }

  const path = normalizePath(parsed.pathname);

  if (parsed.protocol === "tcp:" && path !== "/") {
    throw new SwarmError(
      "rejected",
      "REJECTED",
      "tcp url does not support a path",
    );
  }

  if (parsed.hash !== "") {
    throw new SwarmError(
      "rejected",
      "REJECTED",
      "url does not support a fragment",
    );
  }

  let secret = "";

  if (parsed.password !== "") {
    try {
      secret = decodeURIComponent(parsed.password);
    } catch {
      throw new SwarmError("rejected", "REJECTED", "invalid secret");
    }
  }

  return {
    host: parsed.hostname.replace(/^\[(.*)\]$/, "$1"),
    port,
    portOmitted,
    path,
    secret,
    protocol: parsed.protocol === "tcp:" ? "tcp:" : "ws:",
    secure: parsed.protocol === "wss:",
  };
}

export function normalizePath(path: string | undefined) {
  if (path === undefined || path === "") {
    return "/";
  }

  const value = path.startsWith("/") ? path : `/${path}`;

  return value.length > 1 && value.endsWith("/") ? value.slice(0, -1) : value;
}

export function isAdvertisable(host: string) {
  return host !== "0.0.0.0" && host !== "::" && host !== "::ffff:0:0";
}

export function isLoopback(host: string | undefined) {
  if (host === undefined || host === "") {
    return false;
  }

  const name = host.toLowerCase();

  if (
    name === "localhost" ||
    name === "::1" ||
    name.startsWith("127.") ||
    name.startsWith("::ffff:127.")
  ) {
    return true;
  }

  const mapped = /^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(name);

  if (mapped !== null) {
    return (
      (parseInt(mapped[1], 16) << 16 | parseInt(mapped[2], 16)) >>> 24 === 127
    );
  }

  if (/^(?:0x[0-9a-f]+|\d+)$/.test(name)) {
    const value = Number(name);

    return Number.isInteger(value) && value >= 0 && value <= 0xffffffff
      ? value >>> 24 === 127
      : false;
  }

  return false;
}
