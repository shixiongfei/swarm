/*
 * nodelink.ts
 *
 * Copyright (c) 2026 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/swarm
 */

import { eventlite } from "eventlite.js";
import { SwarmError } from "./errors.ts";
import { protocolVersion } from "./envelope.ts";
import { PendingCalls } from "./pending.ts";
import { Registry } from "./registry.ts";
import { signToken, verifyToken } from "./token.ts";
import { changesFrame, frame, upChange } from "./frame.ts";
import { chunk } from "./chunk.ts";
import { maxEnvelopeSize } from "./codec.ts";
import type { ErrorCode } from "./errors.ts";
import type { Frame } from "./frame.ts";
import type { Link } from "./link.ts";
import type { Provider } from "./provider.ts";
import type { Envelope, PeerEntry, UpEnvelope } from "./envelope.ts";
import type { Instance, Observable, WatchHandler } from "./registry.ts";

type NodeLinkEvents = {
  open: (peerId: string) => void;
  peers: (members: PeerEntry[]) => void;
  down: (instance: Instance, direct: boolean) => void;
  bye: () => void;
  drop: (error: SwarmError) => void;
  close: () => void;
};

const defaultHandshakeTimeout = 15000;
const defaultMaxInboundTimeout = 60000;
const maxSendFailures = 3;

function isTransportFailure(error: unknown) {
  return error instanceof SwarmError && error.retriable;
}

function isDataFailure(error: unknown) {
  if (!(error instanceof Error)) {
    return false;
  }

  if (error.name === "DataCloneError") {
    return true;
  }

  return (
    error instanceof SwarmError &&
    (error.code === "ENVELOPE_INVALID" || error.code === "CLONE_FAILED")
  );
}

function isCongestion(error: unknown): error is SwarmError {
  return error instanceof SwarmError && error.code === "CONGESTED";
}

export type LinkCloseReason = "error" | "bye" | "timeout" | "dedupe" | "close";

export type NodeLinkConfig = {
  provider: Provider;
  startup: number;
  secret: string;
  cluster: string;
  heartbeatInterval: number;
  heartbeatTimeout: number;
  handshakeTimeout?: number;
  maxInflight?: number;
  maxInboundTimeout?: number;
};

export class NodeLink implements Observable {
  readonly events = eventlite<NodeLinkEvents>({
    allowDuplicate: true,
    isolate: true,
  });
  readonly secret: string;
  readonly cluster: string;

  private readonly registry = new Registry();
  private readonly pending = new PendingCalls();
  private readonly inbound = new Map<string, AbortController>();
  private readonly provider: Provider;
  private readonly startup: number;
  private readonly link: Link;
  private readonly heartbeatInterval: number;
  private readonly heartbeatTimeout: number;
  private readonly handshakeTimeout: number;
  private readonly maxInflight: number | undefined;
  private readonly maxInboundTimeout: number;
  private readonly disposes: Array<() => void>;
  private peer?: string;
  peerStartup?: number;
  peerCluster?: string;
  private closed = false;
  private opened = false;
  private reason: LinkCloseReason = "close";
  private reasonCode: ErrorCode = "CLOSED";
  private reasonMessage = "link is closed";
  private lastSeen = Date.now();
  private heartbeat?: ReturnType<typeof setInterval>;
  private handshake?: ReturnType<typeof setTimeout>;
  private pingAt?: number;
  private rtt?: number;
  private failures = 0;

  private readonly onEnvelope = (message: Envelope) => {
    this.lastSeen = Date.now();

    switch (message.t) {
      case "hello":
        if (!this.negotiate(message.version)) {
          return;
        }

        this.peerStartup = message.startup;
        this.peerCluster = message.cluster;
        this.authenticate(message.from, message.token, true);
        return;

      case "accept":
        if (!this.negotiate(message.version)) {
          return;
        }

        this.peerStartup = message.startup;
        this.peerCluster = message.cluster;
        this.authenticate(message.from, message.token, false);
        return;

      case "changes":
        if (this.peer === undefined) {
          return;
        }

        for (const change of message.items) {
          if (change.from === this.link.nodeId) {
            continue;
          }

          if (change.t === "up") {
            this.registry.add(change.service, change.from, change.meta);
            continue;
          }

          const instance = this.registry.remove(change.service, change.from);

          if (instance !== undefined) {
            this.events.emit("down", instance, this.peer === change.from);
          }
        }
        return;

      case "call": {
        if (
          this.peer === undefined ||
          message.from !== this.peer ||
          message.to !== this.link.nodeId
        ) {
          return;
        }

        const timeout =
          message.timeout === undefined
            ? this.maxInboundTimeout
            : Math.min(message.timeout, this.maxInboundTimeout);

        const controller = new AbortController();

        this.inbound.set(message.id, controller);

        Promise.resolve()
          .then(() =>
            this.provider.invoke(
              message.service,
              message.method,
              message.args,
              timeout,
              message.ttl,
              this,
              message.target,
              controller.signal,
            ),
          )
          .then(
            (value) => {
              if (this.inbound.delete(message.id)) {
                this.sendReply(message.id, message.from, value);
              }
            },
            (error: unknown) => {
              if (this.inbound.delete(message.id)) {
                this.sendFailure(message.id, message.from, error);
              }
            },
          );
        return;
      }

      case "cancel":
        if (
          this.peer !== undefined &&
          message.from === this.peer &&
          message.to === this.link.nodeId
        ) {
          const controller = this.inbound.get(message.id);

          if (controller !== undefined) {
            this.inbound.delete(message.id);
            controller.abort(
              new SwarmError("cancelled", "CANCELLED", "call was cancelled"),
            );
          }
        }
        return;

      case "reply":
        if (
          this.peer !== undefined &&
          message.from === this.peer &&
          message.to === this.link.nodeId
        ) {
          this.pending.settle(message.id, message.value);
        }
        return;

      case "error":
        if (
          this.peer !== undefined &&
          message.from === this.peer &&
          message.to === this.link.nodeId
        ) {
          this.pending.reject(
            message.id,
            new SwarmError(
              message.kind,
              message.code,
              message.message,
              message.retriable,
            ),
          );
        }
        return;

      case "pubs":
        if (this.peer !== undefined) {
          for (const item of message.items) {
            if (item.from === this.peer && item.origin !== this.link.nodeId) {
              this.provider.publish(
                item.topic,
                item.data,
                item.origin,
                item.from,
                item.id,
                item.ttl,
              );
            }
          }
        }
        return;

      case "ping":
        if (this.peer !== undefined && message.from === this.peer) {
          try {
            this.transmit(() =>
              this.link.send(frame({ t: "pong", from: this.link.nodeId })),
            );
          } catch {
            // The pong is best-effort; the link health tracks the failure.
          }
        }
        return;

      case "pong":
        if (
          this.peer !== undefined &&
          message.from === this.peer &&
          this.pingAt !== undefined
        ) {
          const sample = Date.now() - this.pingAt;

          this.rtt =
            this.rtt === undefined ? sample : (this.rtt * 3 + sample) / 4;

          this.pingAt = undefined;
        }
        return;

      case "peers":
        if (this.peer !== undefined && message.from === this.peer) {
          this.events.emit("peers", message.members);
        }
        return;

      case "bye":
        if (this.peer !== undefined && message.from === this.peer) {
          this.events.emit("bye");
          this.close("bye", "peer said goodbye");
        }
        return;
    }
  };

  constructor(link: Link, config: NodeLinkConfig) {
    this.link = link;
    this.provider = config.provider;
    this.startup = config.startup;
    this.secret = config.secret;
    this.cluster = config.cluster;
    this.heartbeatInterval = config.heartbeatInterval;
    this.heartbeatTimeout = config.heartbeatTimeout;
    this.handshakeTimeout =
      config.handshakeTimeout !== undefined
        ? config.handshakeTimeout
        : defaultHandshakeTimeout;
    this.maxInflight = config.maxInflight;
    this.maxInboundTimeout =
      config.maxInboundTimeout !== undefined
        ? config.maxInboundTimeout
        : defaultMaxInboundTimeout;

    this.disposes = [
      link.events.on("envelope", this.onEnvelope),
      link.events.on("drop", (error) => this.events.emit("drop", error)),
      link.events.on("close", () => {
        this.dispose("link is closed", "error", "CLOSED");
      }),
    ];

    this.handshake = setTimeout(() => {
      this.handshake = undefined;

      if (this.peer === undefined) {
        this.dispose(
          "link handshake is timed out",
          "timeout",
          "HANDSHAKE_TIMEOUT",
        );
        this.link.close();
      }
    }, this.handshakeTimeout);
  }

  get peerId() {
    return this.peer;
  }

  ready() {
    if (this.peer !== undefined) {
      return Promise.resolve();
    }

    if (this.closed) {
      return Promise.reject(this.closeError());
    }

    return new Promise<void>((resolve, reject) => {
      const offs = [
        this.events.on("open", () => {
          for (const off of offs) {
            off();
          }

          resolve();
        }),
        this.events.on("close", () => {
          for (const off of offs) {
            off();
          }

          reject(this.closeError());
        }),
      ];
    });
  }

  get closeReason() {
    return this.reason;
  }

  get closeCode() {
    return this.reasonCode;
  }

  get closeMessage() {
    return this.reasonMessage;
  }

  get latency() {
    return this.rtt;
  }

  get transport() {
    return this.link.transport;
  }

  get local() {
    return this.link.local;
  }

  get pendingCalls() {
    return this.pending.size;
  }

  get inboundCalls() {
    return this.inbound.size;
  }

  drainInbound() {
    for (const controller of this.inbound.values()) {
      controller.abort();
    }
  }

  hello() {
    this.transmit(() => this.link.hello(this.startup, this.token(), this.cluster));
  }

  notify(frame: Frame) {
    this.sync(() => this.link.send(frame));
  }

  forget(instance: Instance) {
    if (this.closed) {
      return;
    }

    this.registry.remove(instance.service, instance.nodeId);
  }

  lookup(service: string) {
    return this.registry.lookup(service);
  }

  instances() {
    return this.registry.snapshot();
  }

  has(service: string, nodeId?: string) {
    return this.registry.has(service, nodeId);
  }

  watch(handler: WatchHandler): () => void;
  watch(service: string, handler: WatchHandler): () => void;
  watch(service: string | WatchHandler, handler?: WatchHandler) {
    if (typeof service === "function") {
      return this.registry.watch(service);
    }

    if (handler === undefined) {
      throw new SwarmError("rejected", "REJECTED", "watch handler is required");
    }

    return this.registry.watch(service, handler);
  }

  call(
    to: string,
    service: string,
    method: string,
    args: unknown[],
    timeout: number,
    ttl?: number,
    target?: string,
    signal?: AbortSignal,
  ) {
    if (this.closed) {
      return Promise.reject(
        new SwarmError("failed", "CLOSED", "link is closed", true),
      );
    }

    if (
      this.maxInflight !== undefined &&
      this.pending.size >= this.maxInflight
    ) {
      return Promise.reject(
        new SwarmError(
          "rejected",
          "CONGESTED",
          "too many in-flight calls",
          true,
        ),
      );
    }

    return this.pending.call(
      (id) => {
        this.transmit(() =>
          this.link.call(id, to, service, method, args, timeout, ttl, target),
        );

        return () => this.sendCancel(id, to);
      },
      timeout,
      () =>
        new SwarmError(
          "timeout",
          "TIMEOUT",
          `call timed out: ${service}.${method}`,
        ),
      (cause) =>
        cause instanceof Error && cause.name === "DataCloneError"
          ? new SwarmError(
              "failed",
              "CLONE_FAILED",
              "arguments are not cloneable",
              false,
            )
          : cause instanceof SwarmError
            ? cause
            : new SwarmError(
                "failed",
                "CLOSED",
                cause instanceof Error ? cause.message : String(cause),
                true,
              ),
      signal,
    );
  }

  publish(frame: Frame) {
    if (this.peer === undefined && this.transport !== "port") {
      this.events.emit(
        "drop",
        new SwarmError(
          "failed",
          "PUB_DROPPED",
          "link has not authenticated",
          false,
        ),
      );
      return;
    }

    this.transmit(() => this.link.send(frame));
  }

  bye() {
    if (this.closed || this.peer === undefined) {
      return;
    }

    try {
      this.transmit(() =>
        this.link.send(frame({ t: "bye", from: this.link.nodeId })),
      );
    } catch {
      // The link is closed or congested; goodbye is best-effort.
    }
  }

  close(reason: LinkCloseReason = "close", message = "node is closed") {
    this.dispose(message, reason);
    this.link.close();
  }

  private open(from: string, reply: boolean) {
    if (this.closed || !this.connect(from)) {
      return;
    }

    if (this.opened) {
      return;
    }

    try {
      if (reply) {
        this.transmit(() => this.link.accept(this.startup, this.token(), this.cluster));
      }

      this.announce();
    } catch (err) {
      this.dispose(
        err instanceof Error ? err.message : String(err),
        "error",
        "HANDSHAKE_FAILED",
      );
      this.link.close();
      return;
    }

    if (this.closed) {
      return;
    }

    this.opened = true;

    if (this.handshake !== undefined) {
      clearTimeout(this.handshake);
      this.handshake = undefined;
    }

    this.lastSeen = Date.now();

    if (this.heartbeat === undefined) {
      this.heartbeat = setInterval(() => this.beat(), this.heartbeatInterval);
    }

    this.events.emit("open", from);
  }

  private beat() {
    if (this.lastSeen + this.heartbeatTimeout < Date.now()) {
      this.dispose("link is timed out", "timeout", "HEARTBEAT_TIMEOUT");
      this.link.close();
      return;
    }

    if (this.pingAt !== undefined) {
      if (Date.now() - this.pingAt < this.heartbeatInterval) {
        return;
      }

      this.pingAt = undefined;
    }

    this.pingAt = Date.now();

    try {
      this.transmit(() =>
        this.link.send(frame({ t: "ping", from: this.link.nodeId })),
      );
    } catch {
      this.pingAt = undefined;
    }
  }

  private authenticate(from: string, token: string, reply: boolean) {
    if (this.closed || this.reject(from, token)) {
      return;
    }

    this.open(from, reply);
  }

  private token() {
    return signToken(this.secret, this.link.nodeId);
  }

  private negotiate(version: number) {
    if (Math.floor(version / 100) === Math.floor(protocolVersion / 100)) {
      return true;
    }

    this.dispose(
      `protocol version ${version} is incompatible with ${protocolVersion}`,
      "error",
      "VERSION_MISMATCH",
    );
    this.link.close();

    return false;
  }

  private reject(from: string, token: string) {
    if (verifyToken(token, this.secret, from)) {
      return false;
    }

    this.dispose("link signature is invalid", "error", "SIGNATURE_INVALID");
    this.link.close();

    return true;
  }

  private announce() {
    const items: UpEnvelope[] = [];

    for (const instance of this.provider.registrations()) {
      if (instance.nodeId !== this.peer) {
        items.push(upChange(instance));
      }
    }

    const { chunks, dropped } = chunk(
      items,
      changesFrame,
      this.link.transport === "port" ? Infinity : maxEnvelopeSize,
    );

    for (const { error } of dropped) {
      this.events.emit("drop", error);
    }

    for (const { frame } of chunks) {
      this.notify(frame);
    }
  }

  private connect(nodeId: string) {
    if (nodeId === this.link.nodeId) {
      try {
        this.transmit(() => this.link.accept(this.startup, this.token(), this.cluster));
      } catch {
        // best effort: the initiator detects the self-connect from accept
      }

      this.dispose("link connects to itself", "error", "HANDSHAKE_REJECTED");
      this.link.close();

      return false;
    }

    if (this.peer !== undefined) {
      if (this.peer === nodeId) {
        return true;
      }

      this.dispose("link is closed", "error");
      this.link.close();

      return false;
    }

    this.peer = nodeId;

    return true;
  }

  private get sendCloseCode(): ErrorCode {
    return this.opened ? "CLOSED" : "HANDSHAKE_FAILED";
  }

  private transmit(update: () => void) {
    try {
      update();
      this.failures = 0;
    } catch (err) {
      if (isTransportFailure(err) && !isCongestion(err)) {
        this.failures += 1;

        if (this.failures >= maxSendFailures) {
          this.dispose("link is not writable", "error", this.sendCloseCode);
          this.link.close();
        }
      }

      throw err;
    }
  }

  private sync(update: () => void) {
    if (this.closed) {
      return;
    }

    try {
      this.transmit(update);
    } catch (err) {
      if (isDataFailure(err)) {
        return;
      }

      if (isCongestion(err) && this.opened) {
        this.events.emit("drop", err);
        return;
      }

      this.dispose("link is not writable", "error", this.sendCloseCode);
      this.link.close();
    }
  }

  private sendReply(id: string, to: string, value: unknown) {
    try {
      this.transmit(() => this.link.reply(id, to, value));
    } catch (err) {
      if (isTransportFailure(err)) {
        return;
      }

      try {
        this.transmit(() =>
          this.link.fail(
            id,
            to,
            new SwarmError(
              "failed",
              "CLONE_FAILED",
              "reply is not cloneable",
              false,
            ),
          ),
        );
      } catch {
        // The reply cannot be encoded or sent; the caller times out.
      }
    }
  }

  private sendFailure(id: string, to: string, error: unknown) {
    try {
      this.transmit(() => this.link.fail(id, to, error));
    } catch {
      // Delivering the failure is best-effort; the caller times out.
    }
  }

  private sendCancel(id: string, to: string) {
    try {
      this.transmit(() => this.link.cancel(id, to));
    } catch {
      // The peer detects the abandoned call by its own timeout.
    }
  }

  private dispose(
    message: string,
    reason: LinkCloseReason = "close",
    code: ErrorCode = "CLOSED",
  ) {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.reason = reason;
    this.reasonCode = code;
    this.reasonMessage = message;

    if (this.handshake !== undefined) {
      clearTimeout(this.handshake);
      this.handshake = undefined;
    }

    if (this.heartbeat !== undefined) {
      clearInterval(this.heartbeat);
      this.heartbeat = undefined;
    }

    this.pending.clear(() => new SwarmError("failed", code, message, false));

    for (const controller of this.inbound.values()) {
      controller.abort(new SwarmError("failed", code, message, false));
    }

    this.inbound.clear();

    for (const dispose of this.disposes) {
      dispose();
    }

    this.events.emit("close");
    this.registry.close();
  }

  private closeError() {
    const kind =
      this.reason === "timeout"
        ? "timeout"
        : this.reasonCode === "HANDSHAKE_REJECTED"
          ? "rejected"
          : "failed";

    return new SwarmError(kind, this.reasonCode, this.reasonMessage);
  }
}
