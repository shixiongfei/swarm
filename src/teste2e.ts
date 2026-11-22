/*
 * teste2e.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";

const entry = new URL("./index.ts", import.meta.url).href;

export type PeerMessage = Record<string, unknown>;

export class Peer {
  readonly child: ChildProcess;
  readonly messages: PeerMessage[] = [];
  private buffer = "";
  private stderr = "";
  private code: number | null | undefined;
  private notify: (() => void) | undefined;

  constructor(source: string) {
    this.child = spawn(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `import { createNode } from ${JSON.stringify(entry)};\n${source}`,
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );

    this.child.stdout?.setEncoding("utf8");
    this.child.stdout?.on("data", (chunk: string) => this.receive(chunk));

    this.child.stderr?.setEncoding("utf8");
    this.child.stderr?.on("data", (chunk: string) => {
      this.stderr += chunk;
    });

    this.child.on("exit", (code) => {
      this.code = code;
      this.wake();
    });
  }

  async waitFor(predicate: (message: PeerMessage) => boolean, timeout = 5000) {
    const deadline = Date.now() + timeout;

    for (;;) {
      const found = this.messages.find(predicate);

      if (found !== undefined) {
        return found;
      }

      if (this.code !== undefined) {
        throw new Error(`peer exited (${this.code})${this.output()}`);
      }

      const remaining = deadline - Date.now();

      if (remaining <= 0) {
        throw new Error(`peer did not report in ${timeout}ms${this.output()}`);
      }

      await this.sleep(remaining);
    }
  }

  async stop() {
    if (this.code !== undefined) {
      return;
    }

    this.child.kill("SIGKILL");

    while (this.code === undefined) {
      await this.sleep(5000);
    }
  }

  async waitForExit(timeout = 5000) {
    const deadline = Date.now() + timeout;

    while (this.code === undefined) {
      const remaining = deadline - Date.now();

      if (remaining <= 0) {
        throw new Error(`peer did not exit in ${timeout}ms${this.output()}`);
      }

      await this.sleep(remaining);
    }

    return this.code;
  }

  private output() {
    const text = this.stderr.trim();

    return text === "" ? "" : `: ${text}`;
  }

  private receive(chunk: string) {
    this.buffer += chunk;

    let index = this.buffer.indexOf("\n");

    while (index >= 0) {
      const line = this.buffer.slice(0, index).trim();

      this.buffer = this.buffer.slice(index + 1);

      if (line !== "") {
        try {
          this.messages.push(JSON.parse(line));
        } catch {
          this.stderr += `${line}\n`;
        }
      }

      index = this.buffer.indexOf("\n");
    }

    this.wake();
  }

  private sleep(ms: number) {
    return new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        this.notify = undefined;
        resolve();
      }, ms);

      this.notify = () => {
        clearTimeout(timer);
        this.notify = undefined;
        resolve();
      };
    });
  }

  private wake() {
    this.notify?.();
  }
}
