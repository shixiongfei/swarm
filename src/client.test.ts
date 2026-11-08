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
import { spawnSync } from "node:child_process";
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

test("a throwing event handler crashes the process", () => {
  const script = `
import { Client } from "./client.ts";

class StubClient extends Client {
  open() {
    this.markOnline({});
    return Promise.resolve();
  }

  destroy() {}
}

const client = new StubClient();

client.events.on("online", () => {
  throw new Error("boom-client");
});

await client.connect();

setTimeout(() => console.log("survived"), 100);
`;

  const result = spawnSync(
    process.execPath,
    ["--input-type=module", "-e", script],
    {
      cwd: new URL(".", import.meta.url),
      encoding: "utf8",
      timeout: 10000,
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /boom-client/);
  assert.doesNotMatch(result.stdout, /survived/);
});

class HangingClient extends Client<number> {
  destroyed = false;

  protected open() {
    this.socket = 1;

    return new Promise<void>(() => {});
  }

  protected destroy() {
    this.destroyed = true;
  }
}

test("times out when the connection is not established", async () => {
  const client = new HangingClient({ connectTimeout: 50, reconnect: false });

  await assert.rejects(client.connect(), /connect timed out/);

  assert.equal(client.destroyed, true);
  assert.equal(client.socket, undefined);

  client.close();
});
