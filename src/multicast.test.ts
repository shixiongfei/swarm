/*
 * multicast.test.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { createSocket } from "node:dgram";
import { Multicast } from "./multicast.ts";
import { sign } from "./jwt.ts";
import type { PeerEntry } from "./envelope.ts";

async function freePort(family: 4 | 6 = 4) {
  const socket = createSocket({
    type: family === 6 ? "udp6" : "udp4",
    reuseAddr: true,
  });

  await new Promise<void>((resolve) => socket.bind(0, resolve));

  const port = socket.address().port;

  await new Promise<void>((resolve) => socket.close(resolve));

  return port;
}

async function within<T>(promise: Promise<T>, ms: number) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<undefined>((resolve) => {
    timer = setTimeout(() => resolve(undefined), ms);
  });
  const value = await Promise.race([promise, timeout]);

  if (timer !== undefined) {
    clearTimeout(timer);
  }

  return value;
}

describe("Multicast", () => {
  test("exchanges announcements within a cluster", async (t) => {
    const port = await freePort();
    const options = {
      started: 1,
      port,
      interval: 50,
      interface: "127.0.0.1",
    };

    const a = new Multicast({ cluster: "multicast", nodeId: "a", ...options });
    const b = new Multicast({ cluster: "multicast", nodeId: "b", ...options });

    t.after(() => {
      a.close();
      b.close();
    });

    const learned = new Promise<PeerEntry[]>((resolve) =>
      a.events.on("peers", (entries) => {
        if (entries[0].id === "b") {
          resolve(entries);
        }
      }),
    );

    try {
      await a.open(() => [{ id: "a", addr: "tcp://127.0.0.1:1" }]);
      await b.open(() => [{ id: "b", addr: "tcp://127.0.0.1:2" }]);
    } catch {
      t.skip("multicast is not available");
      return;
    }

    const entries = await within(learned, 1000);

    if (entries === undefined) {
      t.skip("multicast packets are not delivered");
      return;
    }

    assert.deepEqual(entries, [{ id: "b", addr: "tcp://127.0.0.1:2" }]);
  });

  test("ignores foreign and malformed announcements", async (t) => {
    const port = await freePort();
    const multicast = new Multicast({
      cluster: "multicast",
      nodeId: "a",
      started: 1,
      port,
      interval: 50,
      interface: "127.0.0.1",
    });
    const sender = createSocket("udp4");

    t.after(() => {
      multicast.close();
      sender.close();
    });

    await new Promise<void>((resolve) => sender.bind(0, "0.0.0.0", resolve));

    sender.setMulticastInterface("127.0.0.1");

    try {
      await multicast.open(() => []);
    } catch {
      t.skip("multicast is not available");
      return;
    }

    const probe = new Promise<PeerEntry[]>((resolve) =>
      multicast.events.on("peers", resolve),
    );

    sender.send(
      JSON.stringify({
        t: "advertise",
        cluster: "multicast",
        from: "probe",
        addr: "tcp://127.0.0.1:1",
        started: 1,
        secret: false,
      }),
      port,
      "239.83.104.105",
    );

    if ((await within(probe, 500)) === undefined) {
      t.skip("multicast packets are not delivered");
      return;
    }

    const leaked = new Promise<PeerEntry[]>((resolve) =>
      multicast.events.on("peers", resolve),
    );

    sender.send(
      JSON.stringify({
        t: "advertise",
        cluster: "other",
        from: "x",
        addr: "tcp://127.0.0.1:1",
        started: 1,
        secret: false,
      }),
      port,
      "239.83.104.105",
    );
    sender.send("not json", port, "239.83.104.105");

    assert.equal(await within(leaked, 300), undefined);

    const received = new Promise<PeerEntry[]>((resolve) =>
      multicast.events.on("peers", resolve),
    );

    sender.send(
      JSON.stringify({
        t: "advertise",
        cluster: "multicast",
        from: "x",
        addr: "tcp://127.0.0.1:1",
        started: 1,
        secret: false,
      }),
      port,
      "239.83.104.105",
    );

    assert.deepEqual(await within(received, 500), [
      { id: "x", addr: "tcp://127.0.0.1:1" },
    ]);
  });

  test("enforces the cluster secret", async (t) => {
    const port = await freePort();
    const multicast = new Multicast({
      cluster: "multicast",
      nodeId: "a",
      secret: "s3cret",
      started: 1,
      port,
      interval: 50,
      interface: "127.0.0.1",
    });
    const sender = createSocket("udp4");

    t.after(() => {
      multicast.close();
      sender.close();
    });

    await new Promise<void>((resolve) => sender.bind(0, "0.0.0.0", resolve));

    sender.setMulticastInterface("127.0.0.1");

    try {
      await multicast.open(() => []);
    } catch {
      t.skip("multicast is not available");
      return;
    }

    const claims = {
      cluster: "multicast",
      from: "x",
      addr: "tcp://127.0.0.1:1",
      started: 1,
    };
    const send = (addr: string, secret: string | undefined) =>
      sender.send(
        JSON.stringify({
          t: "advertise",
          cluster: claims.cluster,
          from: claims.from,
          addr,
          started: claims.started,
          secret: secret !== undefined,
          sig: secret !== undefined ? sign(secret, claims) : undefined,
        }),
        port,
        "239.83.104.105",
      );

    const accepted = new Promise<PeerEntry[]>((resolve) =>
      multicast.events.on("peers", resolve),
    );

    send(claims.addr, "s3cret");

    const entries = await within(accepted, 500);

    if (entries === undefined) {
      t.skip("multicast packets are not delivered");
      return;
    }

    assert.deepEqual(entries, [{ id: "x", addr: claims.addr }]);

    const rejected = new Promise<PeerEntry[]>((resolve) =>
      multicast.events.on("peers", resolve),
    );

    send("tcp://127.0.0.1:2", "s3cret");
    send(claims.addr, "other");
    send(claims.addr, undefined);

    assert.equal(await within(rejected, 300), undefined);
  });

  test("rejects a signed announcement when it has no secret", async (t) => {
    const port = await freePort();
    const multicast = new Multicast({
      cluster: "multicast",
      nodeId: "a",
      started: 1,
      port,
      interval: 50,
      interface: "127.0.0.1",
    });
    const sender = createSocket("udp4");

    t.after(() => {
      multicast.close();
      sender.close();
    });

    await new Promise<void>((resolve) => sender.bind(0, "0.0.0.0", resolve));

    sender.setMulticastInterface("127.0.0.1");

    try {
      await multicast.open(() => []);
    } catch {
      t.skip("multicast is not available");
      return;
    }

    const claims = {
      cluster: "multicast",
      from: "x",
      addr: "tcp://127.0.0.1:1",
      started: 1,
    };
    const send = (secret: boolean, sig?: string) =>
      sender.send(
        JSON.stringify({
          t: "advertise",
          cluster: claims.cluster,
          from: claims.from,
          addr: claims.addr,
          started: claims.started,
          secret,
          sig,
        }),
        port,
        "239.83.104.105",
      );

    const accepted = new Promise<PeerEntry[]>((resolve) =>
      multicast.events.on("peers", resolve),
    );

    send(false);

    const entries = await within(accepted, 500);

    if (entries === undefined) {
      t.skip("multicast packets are not delivered");
      return;
    }

    assert.deepEqual(entries, [{ id: "x", addr: claims.addr }]);

    const rejected = new Promise<PeerEntry[]>((resolve) =>
      multicast.events.on("peers", resolve),
    );

    send(true, sign("s3cret", claims));

    assert.equal(await within(rejected, 300), undefined);
  });

  test("exchanges announcements over IPv6", async (t) => {
    const port = await freePort(6);
    const options = {
      started: 1,
      family: 6 as const,
      port,
      interval: 50,
    };

    const a = new Multicast({ cluster: "multicast", nodeId: "a", ...options });
    const b = new Multicast({ cluster: "multicast", nodeId: "b", ...options });

    t.after(() => {
      a.close();
      b.close();
    });

    const learned = new Promise<PeerEntry[]>((resolve) =>
      a.events.on("peers", (entries) => {
        if (entries[0].id === "b") {
          resolve(entries);
        }
      }),
    );

    try {
      await a.open(() => [{ id: "a", addr: "tcp://[::1]:1" }]);
      await b.open(() => [{ id: "b", addr: "tcp://[::1]:2" }]);
    } catch {
      t.skip("IPv6 multicast is not available");
      return;
    }

    const entries = await within(learned, 1000);

    if (entries === undefined) {
      t.skip("IPv6 multicast packets are not delivered");
      return;
    }

    assert.deepEqual(entries, [{ id: "b", addr: "tcp://[::1]:2" }]);
  });

  test("close is idempotent", async (t) => {
    const port = await freePort();
    const multicast = new Multicast({
      cluster: "multicast",
      nodeId: "a",
      started: 1,
      port,
      interface: "127.0.0.1",
    });

    t.after(() => multicast.close());

    try {
      await multicast.open(() => []);
    } catch {
      t.skip("multicast is not available");
      return;
    }

    multicast.close();

    assert.doesNotThrow(() => multicast.close());
  });
});
