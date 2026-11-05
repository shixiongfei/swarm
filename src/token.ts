/*
 * token.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/muster
 */

import jwt from "jsonwebtoken";

const expiresIn = 60;

export function signToken(secret: string, from: string) {
  return jwt.sign({ from }, secret, {
    algorithm: "HS256",
    expiresIn,
  });
}

export function verifyToken(
  token: string | undefined,
  secret: string,
  from: string,
) {
  if (token === undefined) {
    return false;
  }

  try {
    const payload = jwt.verify(token, secret, {
      algorithms: ["HS256"],
    });

    return (
      typeof payload === "object" && payload !== null && payload.from === from
    );
  } catch {
    return false;
  }
}
