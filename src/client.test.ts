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
import { describe, test } from "node:test";
import { Client } from "./client.ts";
import { SwarmError } from "./errors.ts";
import { waitFor } from "./testutil.ts";
import type { ClientOptions } from "./client.ts";

class StubClient extends Client<number> {
  private failures: number;

  constructor(failures: number, options: ClientOptions = {}) {
    super({ retryDelay: 10, maxRetryDelay: 10, ...options });
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

describe("Client", () => {
  test("rejects invalid connection delays", () => {
    for (const connectTimeout of [0, -1, NaN, Infinity]) {
      assert.throws(
        () => new StubClient(0, { connectTimeout }),
        (err: SwarmError) =>
          err.kind === "rejected" && err.code === "INVALID_TIMEOUT",
      );
    }

    for (const [retryDelay, maxRetryDelay] of [
      [0, 1000],
      [NaN, 1000],
      [Infinity, 1000],
      [1000, 0],
      [1000, NaN],
    ]) {
      assert.throws(
        () => new StubClient(0, { retryDelay, maxRetryDelay }),
        (err: SwarmError) =>
          err.kind === "rejected" && err.code === "INVALID_TIMEOUT",
      );
    }
  });

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

  test("grows the retry delay when a connection flaps", async () => {
    class FlapClient extends Client<number> {
      attempts: number[] = [];

      protected async open() {
        this.attempts.push(Date.now());

        const socket = this.attempts.length;

        this.markOnline(socket);

        queueMicrotask(() => this.markOffline(socket));
      }

      protected destroy() {}
    }

    const client = new FlapClient({ retryDelay: 20, maxRetryDelay: 80 });

    await client.connect();

    await waitFor(() => client.attempts.length >= 4);

    client.close();

    const elapsed = client.attempts[3] - client.attempts[0];

    assert.ok(elapsed >= 100, `elapsed ${elapsed}`);
    assert.ok(elapsed <= 1000, `elapsed ${elapsed}`);
  });

  test("spreads reconnects with jitter", () => {
    const client = new StubClient(0, { retryDelay: 1000, maxRetryDelay: 1000 });
    const backoff = (client as unknown as { backoff(): number }).backoff;
    const delays = Array.from({ length: 40 }, () => backoff.call(client));

    for (const delay of delays) {
      assert.ok(delay >= 800 && delay <= 1200, `delay ${delay}`);
    }

    assert.ok(new Set(delays).size > 1);

    client.close();
  });

  test("keeps jitter out of the stable connection window", (t) => {
    const random = Math.random;
    const schedule = globalThis.setTimeout;
    const delays: number[] = [];

    Math.random = () => 0.99;
    (globalThis as unknown as { setTimeout: unknown }).setTimeout = (
      _fn: () => void,
      ms: number,
    ) => {
      delays.push(ms);
      return 0;
    };

    t.after(() => {
      Math.random = random;
      (globalThis as unknown as { setTimeout: unknown }).setTimeout = schedule;
    });

    const client = new StubClient(0, {
      retryDelay: 1000,
      maxRetryDelay: 30000,
    }) as unknown as {
      markOnline(socket: number): void;
      markOffline(socket: number): void;
      attempt: number;
      onlineAt: number;
    };

    client.markOnline(1);
    client.attempt = 1;
    client.onlineAt = Date.now() - 1050;
    client.markOffline(1);

    assert.equal(client.attempt, 1);
    assert.ok(delays[0] < 1500, `delay ${delays[0]}`);
  });

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
});
