/*
 * dialer.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import type { Client } from "./client.ts";

export type DialerOptions = {
  seeds: Array<string | URL>;
  create: (addr: string, id?: string) => Promise<Client>;
  track: (client: Client) => void;
  untrack: (client: Client) => void;
  isClosed: () => boolean;
};

export class Dialer {
  private readonly seeds: Array<string | URL>;
  private readonly dials = new Map<string, Client | undefined>();
  private readonly create: DialerOptions["create"];
  private readonly track: DialerOptions["track"];
  private readonly untrack: DialerOptions["untrack"];
  private readonly isClosed: DialerOptions["isClosed"];

  constructor(options: DialerOptions) {
    this.seeds = options.seeds;
    this.create = options.create;
    this.track = options.track;
    this.untrack = options.untrack;
    this.isClosed = options.isClosed;
  }

  add(seed: string | URL) {
    if (this.seeds.some((existing) => String(existing) === String(seed))) {
      return;
    }

    this.seeds.push(seed);
    void this.dial(String(seed));
  }

  remove(seed: string | URL) {
    const target = String(seed);
    const index = this.seeds.findIndex(
      (existing) => String(existing) === target,
    );

    if (index >= 0) {
      this.seeds.splice(index, 1);
    }
  }

  list() {
    return this.seeds.map((seed) => String(seed));
  }

  has(addr: string) {
    return this.dials.has(addr);
  }

  discover() {
    for (const seed of this.seeds) {
      void this.dial(String(seed));
    }
  }

  drop(addr: string) {
    const client = this.dials.get(addr);

    this.dials.delete(addr);

    if (client === undefined) {
      return;
    }

    this.untrack(client);
    client.close();
  }

  close() {
    for (const addr of [...this.dials.keys()]) {
      this.drop(addr);
    }
  }

  async dial(addr: string, id?: string) {
    if (this.dials.has(addr)) {
      return;
    }

    this.dials.set(addr, undefined);

    let client: Client;

    try {
      client = await this.create(addr, id);
    } catch {
      this.dials.delete(addr);
      return;
    }

    if (this.isClosed() || !this.dials.has(addr)) {
      client.close();
      return;
    }

    this.dials.set(addr, client);
    this.track(client);

    try {
      await client.connect();
    } catch {
      // the client retries on its own
    }
  }
}
