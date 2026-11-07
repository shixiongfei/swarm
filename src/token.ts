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

import { sign, verify } from "./jwt.ts";

export function signToken(secret: string, from: string) {
  const iat = Math.floor(Date.now() / 1000);
  return sign(secret, { from, iat, exp: iat + 60 });
}

export function verifyToken(
  token: string | undefined,
  secret: string,
  from: string,
) {
  if (token === undefined) {
    return false;
  }

  const claims = verify(secret, token);

  if (
    claims === undefined ||
    claims.from !== from ||
    typeof claims.iat !== "number" ||
    typeof claims.exp !== "number"
  ) {
    return false;
  }

  const now = Math.floor(Date.now() / 1000);

  return claims.exp - claims.iat < 120 && claims.exp > now;
}
