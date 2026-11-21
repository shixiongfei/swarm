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
import type { ClientOptions } from "./client.ts";

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

test("close cancels the retry scheduled by a rejected connect", async () => {
  let attempts = 0;

  class AlwaysFailingClient extends Client<number> {
    protected async open() {
      attempts += 1;
      throw new Error("boom");
    }

    protected destroy() {}
  }

  const client = new AlwaysFailingClient({
    retryDelay: 10,
    maxRetryDelay: 10,
  });

  await assert.rejects(client.connect(), /boom/);

  client.close();

  const attempted = attempts;

  await new Promise((resolve) => setTimeout(resolve, 50));

  assert.equal(attempts, attempted);
});

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

class SlowClient extends Client<number> {
  opened = 0;
  connects = 0;
  destroyed: number[] = [];
  delay = 100;

  constructor(options: ClientOptions = {}) {
    super(options);
  }

  async connect() {
    this.connects += 1;

    return super.connect();
  }

  protected async open() {
    this.opened += 1;

    if (this.delay > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.delay));
    }

    this.markOnline(this.opened);
  }

  protected destroy(socket: number) {
    this.destroyed.push(socket);
  }
}

test("discards a socket that settles after the timeout", async () => {
  const client = new SlowClient({ connectTimeout: 10, reconnect: false });
  let online = 0;

  client.events.on("online", () => {
    online += 1;
  });

  await assert.rejects(client.connect(), /connect timed out/);

  await new Promise((resolve) => setTimeout(resolve, 150));

  assert.equal(online, 0);
  assert.deepEqual(client.destroyed, [1]);
  assert.equal(client.socket, undefined);

  client.close();
});

test(
  "reconnects only after the abandoned attempt settles",
  { timeout: 3000 },
  async () => {
    const client = new SlowClient({
      connectTimeout: 10,
      retryDelay: 30,
      maxRetryDelay: 30,
    });
    const online = new Promise<number>((resolve) => {
      client.events.once("online", resolve);
    });

    await assert.rejects(client.connect(), /connect timed out/);

    assert.equal(client.opened, 1);

    client.delay = 0;

    await new Promise((resolve) => setTimeout(resolve, 60));

    assert.equal(client.opened, 1);
    assert.equal(await online, 2);
    assert.equal(client.opened, 2);
    assert.deepEqual(client.destroyed, [1]);

    client.close();
  },
);

test(
  "does not retry while the timed-out attempt is still settling",
  { timeout: 1000 },
  async () => {
    const client = new SlowClient({
      connectTimeout: 10,
      retryDelay: 20,
      maxRetryDelay: 20,
    });

    await assert.rejects(client.connect(), /connect timed out/);
    assert.equal(client.connects, 1);

    await new Promise((resolve) => setTimeout(resolve, 80));

    assert.equal(client.connects, 1);
    assert.equal(client.opened, 1);

    client.close();
  },
);
