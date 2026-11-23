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

const maxDeparted = 1024;

export type MembershipOptions = {
  id: string;
  memberTtl: number;
  dialTtl: number;
  linked: () => Set<string>;
  dialing: (addr: string) => boolean;
  dial: (addr: string, id: string) => void;
  drop: (addr: string) => void;
  broadcast: (ups: NodeMember[], downs: string[]) => void;
  onUp: (member: NodeMember) => void;
  onDown: (member: NodeMember) => void;
};

export class Membership {
  readonly members = new Map<string, NodeMember>();

  private readonly seen = new Map<string, number>();
  private readonly hosts = new Map<string, string>();
  private readonly expiries = new Map<string, number>();
  private readonly departed = new Map<string, number>();
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
    const advertised = this.members.has(this.id);
    const linked = this.linked();
    const ups: NodeMember[] = [];

    for (const entry of entries) {
      if (entry.id === this.id) {
        continue;
      }

      const gone = this.departed.get(entry.id);

      if (gone !== undefined) {
        if (gone > now) {
          continue;
        }

        this.departed.delete(entry.id);
      }

      const existing = this.members.get(entry.id);
      let host: string;

      try {
        host =
          existing !== undefined && existing.addr === entry.addr
            ? this.hostOf(existing)
            : parseUrl(entry.addr).host;
      } catch {
        continue;
      }

      if (!isAdvertisable(host) || (!local && isLoopback(host))) {
        continue;
      }

      this.hosts.set(entry.id, host);
      this.seen.set(entry.id, now);
      this.expiries.delete(entry.addr);

      if (existing !== undefined) {
        if (existing.addr !== entry.addr) {
          if (this.dialing(existing.addr)) {
            this.expiries.set(existing.addr, now + this.dialTtl);
          }

          existing.addr = entry.addr;
          existing.startup = entry.startup;
          existing.passive = entry.passive === true;
          ups.push(existing);

          if (!linked.has(entry.id) && !this.dialing(entry.addr)) {
            this.dial(entry.addr, entry.id);
          }
        } else if (
          existing.startup !== entry.startup ||
          existing.passive !== (entry.passive === true)
        ) {
          existing.startup = entry.startup;
          existing.passive = entry.passive === true;
          ups.push(existing);
        }

        continue;
      }

      this.members.set(entry.id, entry);
      ups.push(entry);
      this.onUp(entry);

      if (
        (entry.passive || !advertised || this.id < entry.id) &&
        !linked.has(entry.id)
      ) {
        this.dial(entry.addr, entry.id);
      }
    }

    this.schedule(now);

    if (ups.length > 0) {
      this.broadcast(ups, []);
    }
  }

  attach(peer?: string, addr?: string, startup?: number, passive = false) {
    if (addr !== undefined) {
      this.expiries.delete(addr);
    }

    if (peer === undefined || addr === undefined || startup === undefined) {
      return false;
    }

    this.seen.set(peer, Date.now());
    this.departed.delete(peer);

    if (this.members.has(peer)) {
      return false;
    }

    const member = { id: peer, addr, startup, passive };

    this.members.set(peer, member);
    this.broadcast([member], []);
    this.onUp(member);

    return true;
  }

  upsertSelf(member: NodeMember) {
    this.hosts.delete(member.id);
    this.members.set(member.id, member);
    this.broadcast([member], []);
  }

  self() {
    const member = this.members.get(this.id);

    return member === undefined ? [] : [member];
  }

  forget(id: string, addr: string) {
    const member = this.members.get(id);

    if (member !== undefined) {
      this.members.delete(id);
      this.seen.delete(id);
      this.hosts.delete(id);
      this.broadcast([], [id]);
      this.onDown(member);
    }

    this.expiries.delete(addr);
    this.drop(addr);
  }

  forgetByLink(peer: string | undefined) {
    if (peer === undefined) {
      return;
    }

    const member = this.members.get(peer);

    if (member !== undefined) {
      this.forget(member.id, member.addr);
    }
  }

  leave(id: string) {
    if (id === this.id) {
      return;
    }

    this.departed.delete(id);
    this.departed.set(id, Date.now() + this.memberTtl);

    if (this.departed.size > maxDeparted) {
      const oldest = this.departed.keys().next().value;

      if (oldest !== undefined) {
        this.departed.delete(oldest);
      }
    }

    const member = this.members.get(id);

    if (member !== undefined) {
      this.forget(member.id, member.addr);
    }
  }

  linkDown(peer: string | undefined) {
    if (peer !== undefined && this.members.has(peer)) {
      this.seen.set(peer, Date.now());
    }

    this.schedule(Date.now());
  }

  shared(local: boolean) {
    const members = [...this.members.values()];

    if (local) {
      return members;
    }

    return members.filter((member) => !isLoopback(this.hostOf(member)));
  }

  snapshot() {
    return [...this.members.values()].map((member) => ({
      id: member.id,
      addr: member.addr,
      startup: member.startup,
      passive: member.passive,
    }));
  }

  close() {
    this.closed = true;

    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }

    this.members.clear();
    this.seen.clear();
    this.hosts.clear();
    this.expiries.clear();
    this.departed.clear();
  }

  private hostOf(member: NodeMember) {
    const cached = this.hosts.get(member.id);

    if (cached !== undefined) {
      return cached;
    }

    const host = parseUrl(member.addr).host;

    this.hosts.set(member.id, host);

    return host;
  }

  private prune(now: number) {
    const linked = this.linked();
    const downs: string[] = [];

    for (const member of this.members.values()) {
      if (member.id === this.id || linked.has(member.id)) {
        continue;
      }

      const seen = this.seen.get(member.id);

      if (seen === undefined || seen + this.memberTtl > now) {
        continue;
      }

      this.members.delete(member.id);
      this.seen.delete(member.id);
      this.hosts.delete(member.id);
      this.onDown(member);
      downs.push(member.id);

      if (this.dialing(member.addr)) {
        this.expiries.set(member.addr, now + this.dialTtl);
      }
    }

    for (const [addr, expires] of this.expiries) {
      if (expires <= now) {
        this.expiries.delete(addr);
        this.drop(addr);
      }
    }

    this.schedule(now);

    if (downs.length > 0) {
      this.broadcast([], downs);
    }
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

    for (const member of this.members.values()) {
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

        this.prune(Date.now());
      },
      Math.max(0, next - now),
    );
  }
}
