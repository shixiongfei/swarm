/*
 * jwt.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { hmac } from "@noble/hashes/hmac.js";
import { sha256 } from "@noble/hashes/sha2.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function sign(secret: string, claims: Record<string, unknown>) {
  const header = encodeBase64Url(
    encoder.encode(JSON.stringify({ alg: "HS256", typ: "JWT" })),
  );
  const payload = encodeBase64Url(encoder.encode(JSON.stringify(claims)));
  const content = `${header}.${payload}`;
  const signature = hmac(
    sha256,
    encoder.encode(secret),
    encoder.encode(content),
  );

  return `${content}.${encodeBase64Url(signature)}`;
}

export function verify(secret: string, token: string) {
  const parts = token.split(".");

  if (parts.length !== 3) {
    return undefined;
  }

  const [header, payload, signature] = parts;
  const expected = hmac(
    sha256,
    encoder.encode(secret),
    encoder.encode(`${header}.${payload}`),
  );
  const actual = decodeBase64Url(signature);

  if (actual === undefined || actual.length !== expected.length) {
    return undefined;
  }

  let diff = 0;

  for (let i = 0; i < actual.length; i += 1) {
    diff |= actual[i] ^ expected[i];
  }

  if (diff !== 0) {
    return undefined;
  }

  const body = decodeBase64Url(payload);

  if (body === undefined) {
    return undefined;
  }

  try {
    const claims: unknown = JSON.parse(decoder.decode(body));

    return typeof claims === "object" && claims !== null
      ? (claims as Record<string, unknown>)
      : undefined;
  } catch {
    return undefined;
  }
}

const alphabet =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

function encodeBase64Url(bytes: Uint8Array) {
  let value = "";

  for (let i = 0; i < bytes.length; i += 3) {
    const a = bytes[i];
    const b = i + 1 < bytes.length ? bytes[i + 1] : 0;
    const c = i + 2 < bytes.length ? bytes[i + 2] : 0;

    value += alphabet[a >> 2];
    value += alphabet[((a & 3) << 4) | (b >> 4)];

    if (i + 1 < bytes.length) {
      value += alphabet[((b & 15) << 2) | (c >> 6)];
    }

    if (i + 2 < bytes.length) {
      value += alphabet[c & 63];
    }
  }

  return value;
}

function decodeBase64Url(value: string) {
  if (value.length === 0 || value.length % 4 === 1) {
    return undefined;
  }

  const bytes: number[] = [];

  for (let i = 0; i < value.length; i += 4) {
    const chunk = value.slice(i, i + 4).padEnd(4, "=");
    const indexes = [...chunk].map((char) =>
      char === "=" ? 0 : alphabet.indexOf(char),
    );

    if (indexes.some((index) => index < 0)) {
      return undefined;
    }

    const bits =
      (indexes[0] << 18) | (indexes[1] << 12) | (indexes[2] << 6) | indexes[3];

    bytes.push((bits >> 16) & 0xff);

    if (chunk[2] !== "=") {
      bytes.push((bits >> 8) & 0xff);
    }

    if (chunk[3] !== "=") {
      bytes.push(bits & 0xff);
    }
  }

  return new Uint8Array(bytes);
}
