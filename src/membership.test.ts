/*
 * membership.test.ts
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
import { Membership } from "./membership.ts";
import type { MembershipOptions } from "./membership.ts";
import type { NodeMember } from "./membership.ts";

function makeMembership(overrides: Partial<MembershipOptions> = {}) {
  const up: NodeMember[] = [];
  const down: NodeMember[] = [];
  const dials: Array<{ addr: string; id: string }> = [];
  const drops: string[] = [];
  const dialed = new Set<string>();
  const linked = new Set<string>();
  let broadcasts = 0;

  const membership = new Membership({
    id: "self",
    memberTtl: 60000,
    dialTtl: 60000,
    linked: () => linked,
    dialing: (addr) => dialed.has(addr),
    dial: (addr, id) => {
      dialed.add(addr);
      dials.push({ addr, id });
    },
    drop: (addr) => {
      dialed.delete(addr);
      drops.push(addr);
    },
    broadcast: () => {
      broadcasts += 1;
    },
    onUp: (member) => up.push(member),
    onDown: (member) => down.push(member),
    ...overrides,
  });

  return {
    membership,
    up,
    down,
    dials,
    drops,
    dialed,
    linked,
    broadcasts: () => broadcasts,
  };
}

const remote = { id: "peer", addr: "tcp://10.0.0.9:9", startup: 1 };

describe("Membership", () => {
  test("learn skips its own id and loopback hosts from a remote link", (t) => {
    const { membership } = makeMembership();

    t.after(() => membership.close());

    membership.members.set("self", {
      id: "self",
      addr: "tcp://127.0.0.1:9",
      startup: 1,
    });

    membership.learn(
      [
        { id: "self", addr: "tcp://10.0.0.9:9", startup: 1 },
        { id: "local", addr: "tcp://127.0.0.1:9", startup: 1 },
      ],
      false,
    );

    assert.deepEqual([...membership.members.keys()], ["self"]);

    membership.learn([remote], false);

    assert.deepEqual([...membership.members.keys()], ["self", "peer"]);
  });

  test("learn dials a member that never dials back", (t) => {
    const { membership, dials } = makeMembership();

    t.after(() => membership.close());

    membership.upsertSelf({
      id: "self",
      addr: "tcp://10.0.0.1:1",
      startup: 1,
    });

    membership.learn([{ ...remote, passive: "leaf" }], false);

    assert.deepEqual(dials, [{ addr: remote.addr, id: remote.id }]);
  });

  test("learn announces and dials a new member", (t) => {
    const { membership, up, dials, broadcasts } = makeMembership();

    t.after(() => membership.close());

    membership.learn([remote], true);

    assert.deepEqual(membership.snapshot(), [remote]);
    assert.deepEqual(up, [remote]);
    assert.deepEqual(dials, [{ addr: remote.addr, id: remote.id }]);
    assert.equal(broadcasts(), 1);
  });

  test("learn keeps one member and updates the entry when it relocates", (t) => {
    const { membership, dials, broadcasts } = makeMembership();

    t.after(() => membership.close());

    const first = "tcp://10.0.0.9:9";
    const second = "tcp://10.0.0.9:10";

    membership.learn([{ id: remote.id, addr: first, startup: 1 }], true);
    membership.learn([{ id: remote.id, addr: second, startup: 2 }], true);

    assert.deepEqual(membership.snapshot(), [
      { id: remote.id, addr: second, startup: 2 },
    ]);
    assert.deepEqual(dials, [
      { addr: first, id: remote.id },
      { addr: second, id: remote.id },
    ]);
    assert.equal(broadcasts(), 2);
  });

  test("learn drops the old address after the relocation grace period", (t) => {
    t.mock.timers.enable({ apis: ["setTimeout", "Date"] });

    const { membership, dials, drops, dialed } = makeMembership({
      memberTtl: 30,
      dialTtl: 40,
    });

    t.after(() => membership.close());

    const first = "tcp://10.0.0.9:1000";
    const second = "tcp://10.0.0.9:2000";

    membership.learn([{ id: remote.id, addr: first, startup: 1 }], true);
    membership.learn([{ id: remote.id, addr: second, startup: 2 }], true);

    assert.deepEqual(
      dials.map((entry) => entry.addr),
      [first, second],
    );
    assert.deepEqual(drops, []);

    t.mock.timers.tick(41);

    assert.deepEqual(drops, [first]);
    assert.deepEqual([...dialed], [second]);
  });

  test("learn refreshes the startup of a member that restarted in place", (t) => {
    const { membership, broadcasts } = makeMembership();

    t.after(() => membership.close());

    membership.learn([{ id: remote.id, addr: remote.addr, startup: 1 }], true);
    membership.learn([{ id: remote.id, addr: remote.addr, startup: 2 }], true);

    assert.deepEqual(membership.snapshot(), [
      { id: remote.id, addr: remote.addr, startup: 2 },
    ]);
    assert.equal(broadcasts(), 2);
  });

  test("learn does not dial a member it is already linked to", (t) => {
    const { membership, linked, dials } = makeMembership();

    t.after(() => membership.close());

    linked.add(remote.id);

    membership.learn([remote], true);

    assert.deepEqual(membership.snapshot(), [remote]);
    assert.deepEqual(dials, []);
  });

  test("expires a member and drops its dial after the eviction window", (t) => {
    t.mock.timers.enable({ apis: ["setTimeout", "Date"] });

    const { membership, down, drops } = makeMembership({
      memberTtl: 30,
      dialTtl: 30,
    });

    t.after(() => membership.close());

    membership.learn([remote], true);

    t.mock.timers.tick(31);

    assert.deepEqual(membership.snapshot(), []);
    assert.deepEqual(down, [remote]);

    t.mock.timers.tick(31);

    assert.deepEqual(drops, [remote.addr]);
  });

  test("refreshes freshness when the link closes", (t) => {
    t.mock.timers.enable({ apis: ["setTimeout", "Date"] });

    const { membership } = makeMembership({ memberTtl: 120 });

    t.after(() => membership.close());

    membership.learn([remote], true);

    t.mock.timers.tick(90);

    membership.linkDown(remote.id);

    t.mock.timers.tick(90);

    assert.deepEqual(membership.snapshot(), [remote]);

    t.mock.timers.tick(60);

    assert.deepEqual(membership.snapshot(), []);
  });

  test("attach adds a member once and ignores an unknown peer", (t) => {
    const { membership, up } = makeMembership();

    t.after(() => membership.close());

    assert.equal(membership.attach(undefined, remote.addr), false);
    assert.equal(
      membership.attach(remote.id, remote.addr, remote.startup),
      true,
    );
    assert.equal(
      membership.attach(remote.id, remote.addr, remote.startup),
      false,
    );

    assert.deepEqual(membership.snapshot(), [remote]);
    assert.deepEqual(up, [remote]);
  });

  test("forgetByLink drops the member and its dial", (t) => {
    const { membership, down, drops } = makeMembership();

    t.after(() => membership.close());

    membership.attach(remote.id, remote.addr, remote.startup);
    membership.forgetByLink(remote.id);

    assert.deepEqual(membership.snapshot(), []);
    assert.deepEqual(down, [remote]);
    assert.deepEqual(drops, [remote.addr]);
  });

  test("leave forgets a known member and ignores an unknown one", (t) => {
    const { membership, down } = makeMembership();

    t.after(() => membership.close());

    membership.attach(remote.id, remote.addr, remote.startup);
    membership.leave("missing");
    membership.leave(remote.id);

    assert.deepEqual(membership.snapshot(), []);
    assert.deepEqual(down, [remote]);
  });

  test("leave keeps the member out when gossip repeats it", (t) => {
    const { membership, dials } = makeMembership();

    t.after(() => membership.close());

    membership.attach(remote.id, remote.addr, remote.startup);
    membership.leave(remote.id);
    membership.learn([remote], true);

    assert.deepEqual(membership.snapshot(), []);
    assert.deepEqual(dials, []);
  });

  test("leave keeps a bounded history of departed members", (t) => {
    const { membership } = makeMembership();

    t.after(() => membership.close());

    membership.leave(remote.id);

    for (let i = 0; i < 2000; i++) {
      membership.leave(`gone-${i}`);
    }

    membership.learn(
      [remote, { id: "gone-1999", addr: "tcp://10.0.0.8:8", startup: 1 }],
      true,
    );

    assert.deepEqual(
      membership.snapshot().map((member) => member.id),
      [remote.id],
    );
  });

  test("upsertSelf replaces the member in place and keeps one entry", (t) => {
    const { membership, broadcasts } = makeMembership();

    t.after(() => membership.close());

    const self = { id: "self", addr: "tcp://10.0.0.1:1", startup: 1 };

    membership.upsertSelf(self);
    membership.upsertSelf({ id: "self", addr: "tcp://10.0.0.1:2", startup: 1 });

    assert.deepEqual(membership.self(), [
      { id: "self", addr: "tcp://10.0.0.1:2", startup: 1 },
    ]);
    assert.equal(broadcasts(), 2);
  });

  test("upsertSelf hides itself once it moves to a loopback address", (t) => {
    const { membership } = makeMembership();

    t.after(() => membership.close());

    const first = { id: "self", addr: "tcp://10.0.0.1:1", startup: 1 };

    membership.upsertSelf(first);

    assert.deepEqual(membership.shared(false), [first]);

    membership.upsertSelf({ id: "self", addr: "tcp://127.0.0.1:1", startup: 1 });

    assert.deepEqual(membership.shared(false), []);
  });

  test("shared hides loopback members from a remote link", (t) => {
    const { membership } = makeMembership();

    t.after(() => membership.close());

    const local = { id: "local", addr: "tcp://127.0.0.1:9", startup: 1 };

    membership.learn([local, remote], true);

    assert.deepEqual(membership.shared(true), [local, remote]);
    assert.deepEqual(membership.shared(false), [remote]);
  });

  test("shared follows a member that relocates to a loopback address", (t) => {
    const { membership } = makeMembership();

    t.after(() => membership.close());

    membership.learn([{ ...remote }], true);
    membership.learn(
      [{ id: remote.id, addr: "tcp://127.0.0.1:9", startup: 2 }],
      true,
    );

    assert.deepEqual(membership.shared(false), []);
  });

  test("learn keeps the member host when it rejects a loopback relocation", (t) => {
    const { membership } = makeMembership();

    t.after(() => membership.close());

    membership.learn([{ ...remote }], true);
    membership.learn(
      [{ id: remote.id, addr: "tcp://127.0.0.1:9", startup: 2 }],
      false,
    );

    assert.deepEqual(membership.shared(false), [{ ...remote }]);
  });

  test("close clears the member table and stops the prune timer", (t) => {
    t.mock.timers.enable({ apis: ["setTimeout", "Date"] });

    const { membership, down } = makeMembership({ memberTtl: 20 });

    t.after(() => membership.close());

    membership.learn([remote], true);
    membership.close();

    t.mock.timers.tick(60);

    assert.deepEqual(membership.snapshot(), []);
    assert.deepEqual(down, []);
  });

  test("learn skips entries whose address cannot be parsed", (t) => {
    const { membership, dials } = makeMembership();

    t.after(() => membership.close());

    membership.learn(
      [{ id: "bad", addr: "not a url", startup: 1 }, remote],
      true,
    );

    assert.deepEqual(membership.snapshot(), [remote]);
    assert.deepEqual(dials, [{ addr: remote.addr, id: remote.id }]);
  });

  test("learn does not announce an unchanged member", (t) => {
    const { membership, up, broadcasts } = makeMembership();

    t.after(() => membership.close());

    membership.learn([remote], true);
    membership.learn([remote], true);

    assert.deepEqual(up, [remote]);
    assert.equal(broadcasts(), 1);
  });

  test("learn dials a new member only when its own id wins the tie-break", (t) => {
    const { membership, dials } = makeMembership();

    t.after(() => membership.close());

    membership.members.set("self", {
      id: "self",
      addr: "tcp://10.0.0.1:1",
      startup: 1,
    });

    membership.learn(
      [{ id: "peer", addr: "tcp://10.0.0.9:9", startup: 1 }],
      true,
    );

    assert.deepEqual(dials, []);

    membership.learn(
      [{ id: "zpeer", addr: "tcp://10.0.0.10:9", startup: 1 }],
      true,
    );

    assert.deepEqual(dials, [{ addr: "tcp://10.0.0.10:9", id: "zpeer" }]);
  });
});
