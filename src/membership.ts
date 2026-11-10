/*
 * membership.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { isAdvertisable, isLoopback, parseUrl } from "./address.ts";
import type { PeerEntry } from "./envelope.ts";

export type NodeMember = PeerEntry;

export type MembershipOptions = {
  id: string;
  memberTtl: number;
  dialTtl: number;
  linked: () => Set<string>;
  dialing: (addr: string) => boolean;
  dial: (addr: string, id: string) => void;
  drop: (addr: string) => void;
  broadcast: () => void;
  onUp: (member: NodeMember) => void;
  onDown: (member: NodeMember) => void;
};

export class Membership {
  readonly members: NodeMember[] = [];

  private readonly seen = new Map<string, number>();
  private readonly expiries = new Map<string, number>();
  private readonly id: string;
  private readonly memberTtl: number;
  private readonly dialTtl: number;
  private readonly linked: MembershipOptions["linked"];
  private readonly dialing: MembershipOptions["dialing"];
  private readonly dial: MembershipOptions["dial"];
  private readonly drop: MembershipOptions["drop"];
  private readonly broadcast: MembershipOptions["broadcast"];
  private readonly onUp: MembershipOptions["onUp"];
  private readonly onDown: MembershipOptions["onDown"];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private closed = false;

  constructor(options: MembershipOptions) {
    this.id = options.id;
    this.memberTtl = options.memberTtl;
    this.dialTtl = options.dialTtl;
    this.linked = options.linked;
    this.dialing = options.dialing;
    this.dial = options.dial;
    this.drop = options.drop;
    this.broadcast = options.broadcast;
    this.onUp = options.onUp;
    this.onDown = options.onDown;
  }

  learn(entries: PeerEntry[], local: boolean) {
    const now = Date.now();
    const advertised = this.members.some((member) => member.id === this.id);
    let changed = false;

    for (const entry of entries) {
      if (entry.id === this.id) {
        continue;
      }

      let host: string;

      try {
        host = parseUrl(entry.addr).host;
      } catch {
        continue;
      }

      if (!isAdvertisable(host) || (!local && isLoopback(host))) {
        continue;
      }

      this.seen.set(entry.id, now);
      this.expiries.delete(entry.addr);

      const existing = this.members.find((member) => member.id === entry.id);

      if (existing !== undefined) {
        if (
          existing.addr !== entry.addr &&
          !this.linked().has(entry.id) &&
          !this.dialing(entry.addr)
        ) {
          this.dial(entry.addr, entry.id);
        }

        continue;
      }

      this.members.push(entry);
      changed = true;
      this.onUp(entry);

      if ((!advertised || this.id < entry.id) && !this.linked().has(entry.id)) {
        this.dial(entry.addr, entry.id);
      }
    }

    this.schedule(now);

    if (changed) {
      this.broadcast();
    }
  }

  attach(peer?: string, addr?: string) {
    if (addr !== undefined) {
      this.expiries.delete(addr);
    }

    if (peer === undefined || addr === undefined) {
      return false;
    }

    this.seen.set(peer, Date.now());

    if (this.members.some((member) => member.id === peer)) {
      return false;
    }

    const member = { id: peer, addr };

    this.members.push(member);
    this.broadcast();
    this.onUp(member);

    return true;
  }

  upsertSelf(member: NodeMember) {
    const index = this.members.findIndex((item) => item.id === member.id);

    if (index >= 0) {
      this.members[index] = member;
    } else {
      this.members.push(member);
    }

    this.broadcast();
  }

  self() {
    return this.members.filter((member) => member.id === this.id);
  }

  forget(id: string, addr: string) {
    const index = this.members.findIndex((member) => member.id === id);

    if (index >= 0) {
      const [member] = this.members.splice(index, 1);

      this.seen.delete(id);
      this.broadcast();
      this.onDown(member);
    }

    this.expiries.delete(addr);
    this.drop(addr);
  }

  forgetByLink(peer: string | undefined) {
    if (peer === undefined) {
      return;
    }

    const member = this.members.find((item) => item.id === peer);

    if (member !== undefined) {
      this.forget(member.id, member.addr);
    }
  }

  leave(id: string) {
    const member = this.members.find((item) => item.id === id);

    if (member !== undefined) {
      this.forget(member.id, member.addr);
    }
  }

  linkDown(peer: string | undefined) {
    if (
      peer !== undefined &&
      this.members.some((member) => member.id === peer)
    ) {
      this.seen.set(peer, Date.now());
    }

    this.schedule(Date.now());
  }

  shared(local: boolean) {
    if (local) {
      return this.members;
    }

    return this.members.filter(
      (member) => !isLoopback(parseUrl(member.addr).host),
    );
  }

  snapshot() {
    return this.members.map((member) => ({
      id: member.id,
      addr: member.addr,
    }));
  }

  close() {
    this.closed = true;

    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }

    this.members.length = 0;
    this.seen.clear();
    this.expiries.clear();
  }

  private prune(now: number) {
    const linked = this.linked();
    let changed = false;

    for (let index = this.members.length - 1; index >= 0; index -= 1) {
      const member = this.members[index];

      if (member.id === this.id || linked.has(member.id)) {
        continue;
      }

      const seen = this.seen.get(member.id);

      if (seen === undefined || seen + this.memberTtl > now) {
        continue;
      }

      this.members.splice(index, 1);
      this.seen.delete(member.id);
      this.onDown(member);

      if (this.dialing(member.addr)) {
        this.expiries.set(member.addr, now + this.dialTtl);
      }

      changed = true;
    }

    for (const [addr, expires] of this.expiries) {
      if (expires <= now) {
        this.expiries.delete(addr);
        this.drop(addr);
      }
    }

    this.schedule(now);

    return changed;
  }

  private schedule(now: number) {
    if (this.closed) {
      return;
    }

    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }

    const linked = this.linked();
    let next: number | undefined;

    for (const member of this.members) {
      if (member.id === this.id || linked.has(member.id)) {
        continue;
      }

      const seen = this.seen.get(member.id);

      if (seen === undefined) {
        continue;
      }

      const deadline = seen + this.memberTtl;

      if (next === undefined || deadline < next) {
        next = deadline;
      }
    }

    for (const deadline of this.expiries.values()) {
      if (next === undefined || deadline < next) {
        next = deadline;
      }
    }

    if (next === undefined) {
      return;
    }

    this.timer = setTimeout(
      () => {
        this.timer = undefined;

        if (this.prune(Date.now())) {
          this.broadcast();
        }
      },
      Math.max(0, next - now),
    );
  }
}
