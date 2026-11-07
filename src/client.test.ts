/*
 * client.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { Client } from "./client.ts";

class StubClient extends Client<number> {
  private failures: number;

  constructor(failures: number) {
    super({ retryDelay: 10, maxRetryDelay: 10 });
    this.failures = failures;
  }

  protected async open() {
    if (this.failures > 0) {
      this.failures -= 1;
      throw new Error("boom");
    }

    this.markOnline(1);
  }

  protected destroy() {}
}

test(
  "retries after the initial connection fails",
  { timeout: 1000 },
  async () => {
    const client = new StubClient(2);
    const online = new Promise<number>((resolve) => {
      client.events.once("online", resolve);
    });

    await assert.rejects(client.connect(), /boom/);
    assert.equal(await online, 1);

    client.close();
  },
);
