/*
 * app.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { SwarmError } from "./errors.ts";
import { Node } from "./node.ts";
import type { NodeConfig } from "./node.ts";
import type { Service } from "./service.ts";

export type AppOptions = {
  node: NodeConfig;
  listen?: Array<string | URL>;
  services?: Service[];
};

export type App = {
  readonly node: Node;
  start(): Promise<App>;
  stop(): Promise<void>;
};

class AppImpl implements App {
  readonly node: Node;

  private readonly listenUrls: Array<string | URL>;
  private readonly services: Service[];
  private started = false;
  private stopped = false;
  private starting?: Promise<App>;

  constructor(options: AppOptions) {
    this.node = new Node(options.node);
    this.listenUrls = options.listen === undefined ? [] : options.listen;
    this.services = options.services === undefined ? [] : options.services;

    this.node.events.on("close", () => {
      void this.stop();
    });
  }

  start(): Promise<App> {
    if (this.stopped) {
      return Promise.reject(
        new SwarmError("rejected", "APP_CLOSED", "app is stopped", false),
      );
    }

    if (this.started) {
      return Promise.resolve(this);
    }

    if (this.starting === undefined) {
      this.starting = this.boot();
    }

    return this.starting;
  }

  private async boot(): Promise<App> {
    try {
      for (const url of this.listenUrls) {
        await this.node.listen(url);
      }

      for (const service of this.services) {
        service.attach(this.node);
      }

      this.started = true;

      return this;
    } catch (err) {
      this.stopped = true;
      this.node.close();
      throw err;
    } finally {
      this.starting = undefined;
    }
  }

  async stop() {
    if (this.stopped) {
      return;
    }

    this.stopped = true;

    for (const service of this.services) {
      service.detach();
    }

    this.node.close();
  }
}

export function createApp(options: AppOptions): App {
  return new AppImpl(options);
}
