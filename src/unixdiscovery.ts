/*
 * unixdiscovery.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { createHash } from "node:crypto";
import { chmod, mkdir, readdir, rmdir, unlink } from "node:fs/promises";
import { connect, createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { eventlite } from "eventlite.js";
import { randomId } from "./random.ts";
import type { Socket } from "node:net";

export type UnixDiscoveryConfig = {
  cluster: string;
  secret?: string;
  dir?: string;
  interval?: number;
};

export type UnixDiscoveryEvents = {
  accept: (socket: Socket) => void;
  connect: (socket: Socket) => void;
  error: (error: Error) => void;
};

const defaultInterval = 3000;
const dirMode = 0o700;
const socketMode = 0o600;
const suffix = ".sock";

export class UnixDiscovery {
  readonly events = eventlite<UnixDiscoveryEvents>({
    isolate: true,
  });

  readonly dir: string;

  private readonly ownsDir: boolean;
  private readonly token = randomId().slice(0, 12);
  private readonly interval: number;
  private readonly server = createServer((socket) => this.onAccept(socket));
  private readonly dials = new Map<string, Socket>();
  private timer: ReturnType<typeof setInterval> | undefined;
  private path: string | undefined;
  private closed = false;

  constructor(config: UnixDiscoveryConfig) {
    this.dir =
      config.dir !== undefined
        ? config.dir
        : discoveryDir(config.cluster, config.secret);
    this.ownsDir = config.dir === undefined;
    this.interval =
      config.interval !== undefined ? config.interval : defaultInterval;
  }

  get active() {
    return this.path !== undefined;
  }

  async open() {
    if (this.closed) {
      return;
    }

    await mkdir(this.dir, { recursive: true, mode: dirMode });

    const path = join(this.dir, `${this.token}${suffix}`);

    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => reject(error);

      this.server.once("error", onError);
      this.server.listen(path, () => {
        this.server.off("error", onError);
        this.server.on("error", (error) => this.events.emit("error", error));
        resolve();
      });
    });

    if (this.closed) {
      if (this.server.listening) {
        this.server.close();
      }

      await unlink(path).catch(ignore);
      return;
    }

    this.path = path;

    await chmod(path, socketMode).catch(ignore);
    await this.scan();

    this.timer = setInterval(() => void this.scan(), this.interval);
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    if (this.timer !== undefined) {
      clearInterval(this.timer);
      this.timer = undefined;
    }

    for (const socket of this.dials.values()) {
      socket.destroy();
    }

    this.dials.clear();

    if (this.server.listening) {
      this.server.close();
    }

    const path = this.path;
    this.path = undefined;

    if (path !== undefined) {
      if (this.ownsDir) {
        void unlink(path)
          .catch(ignore)
          .then(() => rmdir(this.dir))
          .catch(ignore);
      } else {
        void unlink(path).catch(ignore);
      }
    }
  }

  private onAccept(socket: Socket) {
    if (this.closed) {
      socket.destroy();
      return;
    }

    this.events.emit("accept", socket);
  }

  private async scan() {
    if (this.closed) {
      return;
    }

    let names: string[];

    try {
      names = await readdir(this.dir);
    } catch {
      return;
    }

    for (const name of names) {
      if (!name.endsWith(suffix) || name === `${this.token}${suffix}`) {
        continue;
      }

      // only the larger token dials, so a pair of nodes links exactly once
      if (name.slice(0, -suffix.length) >= this.token) {
        continue;
      }

      const path = join(this.dir, name);

      if (this.dials.has(path)) {
        continue;
      }

      this.dial(path);
    }
  }

  private dial(path: string) {
    const socket = connect(path);

    this.dials.set(path, socket);

    const forget = () => {
      if (this.dials.get(path) === socket) {
        this.dials.delete(path);
      }
    };

    const onError = (error: NodeJS.ErrnoException) => {
      forget();
      socket.destroy();

      if (error.code === "ECONNREFUSED" || error.code === "ENOTSOCK") {
        void unlink(path).catch(ignore);
      } else if (error.code !== "ENOENT") {
        this.events.emit("error", error);
      }
    };

    socket.once("error", onError);
    socket.once("close", forget);
    socket.once("connect", () => {
      socket.off("error", onError);

      if (this.closed) {
        forget();
        socket.destroy();
        return;
      }

      this.events.emit("connect", socket);
    });
  }
}

function discoveryDir(cluster: string, secret?: string) {
  const hash = createHash("sha256")
    .update(`${cluster}\u0000${secret !== undefined ? secret : ""}`)
    .digest("hex")
    .slice(0, 12);

  return join(tmpdir(), "swarm", hash);
}

function ignore() {}
