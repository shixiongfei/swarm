/*
 * token.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const alphabet =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

const digits = new Map<string, number>();

for (let i = 0; i < alphabet.length; i += 1) {
  digits.set(alphabet[i], i);
}

const expiresIn = 60;

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
  const bytes = new Uint8Array(Math.floor((value.length * 3) / 4));
  let index = 0;

  for (let i = 0; i < value.length; i += 4) {
    const a = digit(value[i]);
    const b = digit(value[i + 1]);
    const c = i + 2 < value.length ? digit(value[i + 2]) : 0;
    const d = i + 3 < value.length ? digit(value[i + 3]) : 0;

    bytes[index] = (a << 2) | (b >> 4);
    index += 1;

    if (i + 2 < value.length) {
      bytes[index] = ((b & 15) << 4) | (c >> 2);
      index += 1;
    }

    if (i + 3 < value.length) {
      bytes[index] = ((c & 3) << 6) | d;
      index += 1;
    }
  }

  return bytes.subarray(0, index);
}

function digit(char: string) {
  const value = digits.get(char);

  if (value === undefined) {
    throw new Error("invalid base64url");
  }

  return value;
}

function hmacKey(secret: string) {
  if (typeof crypto === "undefined" || crypto.subtle === undefined) {
    throw new Error("webcrypto is unavailable");
  }

  return crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"],
  );
}

export async function signToken(secret: string, from: string) {
  const now = Math.floor(Date.now() / 1000);

  const header = encodeBase64Url(
    encoder.encode(JSON.stringify({ alg: "HS256", typ: "JWT" })),
  );
  const payload = encodeBase64Url(
    encoder.encode(JSON.stringify({ from, iat: now, exp: now + expiresIn })),
  );
  const content = `${header}.${payload}`;
  const signature = await crypto.subtle.sign(
    "HMAC",
    await hmacKey(secret),
    encoder.encode(content),
  );

  return `${content}.${encodeBase64Url(new Uint8Array(signature))}`;
}

export async function verifyToken(
  token: string | undefined,
  secret: string,
  from: string,
) {
  if (token === undefined) {
    return false;
  }

  try {
    const parts = token.split(".");

    if (parts.length !== 3) {
      return false;
    }

    const [header, payload, signature] = parts;
    const meta = JSON.parse(decoder.decode(decodeBase64Url(header)));

    if (typeof meta !== "object" || meta === null || meta.alg !== "HS256") {
      return false;
    }

    const valid = await crypto.subtle.verify(
      "HMAC",
      await hmacKey(secret),
      decodeBase64Url(signature),
      encoder.encode(`${header}.${payload}`),
    );

    if (!valid) {
      return false;
    }

    const claims = JSON.parse(decoder.decode(decodeBase64Url(payload)));

    return (
      typeof claims === "object" &&
      claims !== null &&
      claims.from === from &&
      typeof claims.exp === "number" &&
      claims.exp > Math.floor(Date.now() / 1000)
    );
  } catch {
    return false;
  }
}
