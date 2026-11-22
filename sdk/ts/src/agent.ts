/**
 * Agent — the reference worker runtime for a real node (PROTOCOL.md §5.3).
 *
 * One platform, one SDK: create an Agent in a Node/Deno/Bun/Electron process,
 * register command handlers, call run(). The Agent registers itself with a
 * platform descriptor, heartbeats, long-polls command.pull, executes handlers,
 * reports command.complete and unregisters on shutdown.
 *
 * This module is browser-safe at import time: no Node built-ins are imported
 * statically, and platform detection reads `globalThis` defensively.
 */
import { HubClient, SDK_VERSION } from './client';
import type {
  Command,
  HubClientOptions,
  Node,
  NodePlatform,
  NodeRegisterParams
} from './types';

/** Executes one command method. Throwing marks the command failed. */
export type AgentHandler = (
  params: Record<string, unknown>,
  ctx: { command: Command; signal: AbortSignal }
) => unknown | Promise<unknown>;

/** Handler error carrying a protocol error code/message. */
export class AgentHandlerError extends Error {
  readonly code: number;
  readonly data?: Record<string, unknown>;
  constructor(code: number, message: string, data?: Record<string, unknown>) {
    super(message);
    this.name = 'AgentHandlerError';
    this.code = code;
    this.data = data;
  }
}

export interface AgentOptions extends HubClientOptions {
  /** Reuse an existing client (it is not closed by the Agent). */
  client?: HubClient;
  /** Node id; the engine generates one when omitted. */
  nodeId?: string;
  name?: string;
  capabilities?: string[];
  tags?: string[];
  metadata?: Record<string, unknown>;
  /** Overrides auto-detection. */
  platform?: NodePlatform;
  /** Heartbeat interval; default 10000 ms (core watchdog is 15s). */
  heartbeatMs?: number;
  /** command.pull long-poll; default 5000 ms. */
  pullWaitMs?: number;
  /** Parallel pull loops; default 1. */
  concurrency?: number;
  /** Per-command handler timeout; 0 = none. */
  handlerTimeoutMs?: number;
  onError?: (err: unknown) => void;
}

type AnyGlobal = Record<string, any>;

/** Detects the platform of the current JavaScript runtime. */
export function detectPlatform(): NodePlatform {
  const g = globalThis as unknown as AnyGlobal;
  const p = g['process'];
  const nav = g['navigator'];
  const bun = p?.versions?.['bun'];
  const deno = g['Deno'];
  let os = 'unknown';
  let arch = 'unknown';
  let runtime = 'javascript';
  let runtimeVersion = '';

  if (bun) {
    runtime = 'bun';
    runtimeVersion = bun;
    os = p?.platform ?? os;
    arch = p?.arch ?? arch;
  } else if (p?.versions?.['node']) {
    runtime = p?.['versions']?.['electron'] ? 'electron' : 'node';
    runtimeVersion = p.versions.node;
    os = p.platform ?? os;
    arch = p.arch ?? arch;
  } else if (deno?.version?.deno) {
    runtime = 'deno';
    runtimeVersion = deno.version.deno;
    os = deno.build?.os ?? os;
    arch = deno.build?.arch ?? arch;
  } else if (nav) {
    runtime = 'browser';
    runtimeVersion = nav.userAgent ?? '';
    os = nav.platform ?? os;
  }
  return { os, arch, runtime, runtime_version: runtimeVersion, agent_version: SDK_VERSION };
}

const SLEEP_ABORT = Symbol('aborted');

/** Sleeps for ms, or rejects with SLEEP_ABORT when the signal aborts. */
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(SLEEP_ABORT);
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(SLEEP_ABORT);
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

function isAbort(err: unknown): boolean {
  return err === SLEEP_ABORT || (err instanceof Error && err.name === 'AbortError');
}

function isNodeGone(err: unknown): boolean {
  const code = (err as { code?: number } | undefined)?.code;
  return code === -32010 || code === -32011; // node.not_found / node.offline
}

function toCommandError(err: unknown): { code: number; message: string; data?: Record<string, unknown> } {
  if (err instanceof AgentHandlerError) {
    return err.data
      ? { code: err.code, message: err.message, data: err.data }
      : { code: err.code, message: err.message };
  }
  const code = (err as { code?: number } | undefined)?.code ?? -32603;
  return { code, message: err instanceof Error ? err.message : String(err) };
}

/** A registered node driven by the TypeScript SDK. */
export class Agent {
  private readonly client: HubClient;
  private readonly ownClient: boolean;
  private readonly nodeIdOpt?: string;
  private readonly name?: string;
  private readonly capabilities: string[];
  private readonly tags: string[];
  private readonly metadata?: Record<string, unknown>;
  private readonly platform: NodePlatform;
  private readonly heartbeatMs: number;
  private readonly pullWaitMs: number;
  private readonly concurrency: number;
  private readonly handlerTimeoutMs: number;
  private readonly onError?: (err: unknown) => void;
  private readonly handlers = new Map<string, AgentHandler>();
  private node?: Node;
  private running = false;
  private controller?: AbortController;
  private regPromise?: Promise<void>;

  constructor(options: AgentOptions = {}) {
    const { client, nodeId, name, capabilities, tags, metadata, platform, heartbeatMs,
      pullWaitMs, concurrency, handlerTimeoutMs, onError, ...clientOpts } = options;
    this.client = client ?? new HubClient(clientOpts);
    this.ownClient = !client;
    this.nodeIdOpt = nodeId;
    this.name = name;
    this.capabilities = capabilities ?? [];
    this.tags = tags ?? [];
    this.metadata = metadata;
    this.platform = platform ?? detectPlatform();
    this.heartbeatMs = heartbeatMs ?? 10_000;
    this.pullWaitMs = pullWaitMs ?? 5000;
    this.concurrency = Math.max(1, Math.floor(concurrency ?? 1));
    this.handlerTimeoutMs = handlerTimeoutMs ?? 0;
    this.onError = onError;

    this.handle('echo', (params) => params);
    this.handle('platform.info', () => ({
      node_id: this.nodeID,
      platform: this.platform,
      agent: { runtime: 'typescript', version: SDK_VERSION }
    }));
  }

  /** Registers a handler. Built-ins echo/platform.info can be overridden. */
  handle(method: string, handler: AgentHandler): this {
    this.handlers.set(method, handler);
    return this;
  }

  get clientRef(): HubClient {
    return this.client;
  }

  /** Current registered node id (undefined before registration). */
  get nodeID(): string | undefined {
    return this.node?.id ?? this.nodeIdOpt;
  }

  /** Snapshot of the registered node. */
  get nodeInfo(): Node | undefined {
    return this.node ? { ...this.node } : undefined;
  }

  /** Stops a running agent (run() resolves after cleanup). */
  stop(): void {
    this.controller?.abort();
  }

  /**
   * Registers and serves commands until the signal aborts (or stop() runs).
   * Initial registration retries with exponential backoff.
   */
  async run(signal?: AbortSignal): Promise<void> {
    if (this.running) throw new Error('agent is already running');
    this.running = true;
    const controller = new AbortController();
    this.controller = controller;
    const forward = () => controller.abort();
    signal?.addEventListener('abort', forward, { once: true });
    if (signal?.aborted) controller.abort();

    try {
      if (this.ownClient) await this.client.open();
      await this.registerWithRetry(controller.signal);
      if (controller.signal.aborted) return;

      const loops: Promise<void>[] = [this.heartbeatLoop(controller.signal)];
      for (let i = 0; i < this.concurrency; i++) loops.push(this.pullLoop(controller.signal));

      await new Promise<void>((resolve) => {
        if (controller.signal.aborted) return resolve();
        controller.signal.addEventListener('abort', () => resolve(), { once: true });
      });
      await Promise.allSettled(loops);
      await this.unregister();
    } finally {
      signal?.removeEventListener('abort', forward);
      this.running = false;
      this.controller = undefined;
      if (this.ownClient) await this.client.close();
    }
  }

  private report(err: unknown): void {
    if (this.onError && !isAbort(err)) this.onError(err);
  }

  private registerParams(): NodeRegisterParams {
    const p: NodeRegisterParams = { platform: this.platform };
    if (this.nodeID) p.node_id = this.nodeID;
    if (this.name) p.name = this.name;
    if (this.capabilities.length) p.capabilities = this.capabilities;
    if (this.tags.length) p.tags = this.tags;
    if (this.metadata) p.metadata = this.metadata;
    return p;
  }

  private async registerOnce(signal: AbortSignal): Promise<void> {
    if (signal.aborted) return;
    const res = await this.client.nodeRegister(this.registerParams());
    if (res.node) this.node = res.node;
  }

  private async registerWithRetry(signal: AbortSignal): Promise<void> {
    let delay = 1000;
    for (;;) {
      try {
        await this.registerOnce(signal);
        return;
      } catch (err) {
        if (signal.aborted) return;
        this.report(err);
        try {
          await sleep(delay, signal);
        } catch {
          return;
        }
        delay = Math.min(delay * 2, 30_000);
      }
    }
  }

  /** Single-flight re-registration after node.not_found / node.offline. */
  private ensureRegistered(signal: AbortSignal): Promise<void> {
    if (!this.regPromise) {
      this.regPromise = this.registerOnce(signal)
        .catch((err) => this.report(err))
        .finally(() => {
          this.regPromise = undefined;
        });
    }
    return this.regPromise;
  }

  private async heartbeatLoop(signal: AbortSignal): Promise<void> {
    for (;;) {
      try {
        await sleep(this.heartbeatMs, signal);
      } catch {
        return;
      }
      const id = this.nodeID;
      if (!id || signal.aborted) continue;
      try {
        await this.client.nodeHeartbeat(id);
      } catch (err) {
        if (signal.aborted) return;
        if (isNodeGone(err)) await this.ensureRegistered(signal);
        else this.report(err);
      }
    }
  }

  private async pullLoop(signal: AbortSignal): Promise<void> {
    let backoff = 1000;
    while (!signal.aborted) {
      const id = this.nodeID;
      if (!id) {
        try {
          await sleep(backoff, signal);
        } catch {
          return;
        }
        continue;
      }
      try {
        const res = await this.client.commandPull({ node_id: id, limit: 1, wait: this.pullWaitMs });
        backoff = 1000;
        const commands = res.commands ?? [];
        if (commands.length === 0) {
          try {
            await sleep(50, signal);
          } catch {
            return;
          }
          continue;
        }
        for (const command of commands) {
          await this.execute(command, signal);
        }
      } catch (err) {
        if (signal.aborted) return;
        if (isNodeGone(err)) {
          await this.ensureRegistered(signal);
          continue;
        }
        this.report(err);
        try {
          await sleep(backoff, signal);
        } catch {
          return;
        }
        backoff = Math.min(backoff * 2, 30_000);
      }
    }
  }

  private async execute(command: Command, signal: AbortSignal): Promise<void> {
    const handler = this.handlers.get(command.method ?? '');
    if (!handler) {
      await this.complete(command, signal, 'failed', undefined, {
        code: -32601,
        message: `method not supported: ${command.method ?? ''}`
      });
      return;
    }

    const exec = new AbortController();
    const forward = () => exec.abort();
    signal.addEventListener('abort', forward, { once: true });
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const invocation = Promise.resolve(
        handler((command.params as Record<string, unknown>) ?? {}, { command, signal: exec.signal })
      );
      let result: unknown;
      if (this.handlerTimeoutMs > 0) {
        const timeout = new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            exec.abort();
            reject(new AgentHandlerError(-32603, `handler timed out after ${this.handlerTimeoutMs}ms`));
          }, this.handlerTimeoutMs);
        });
        result = await Promise.race([invocation, timeout]);
      } else {
        result = await invocation;
      }
      await this.complete(command, signal, 'succeeded', result);
    } catch (err) {
      const error = toCommandError(err);
      await this.complete(command, signal, 'failed', undefined, error);
    } finally {
      if (timer) clearTimeout(timer);
      signal.removeEventListener('abort', forward);
    }
  }

  private async complete(
    command: Command,
    signal: AbortSignal,
    status: 'succeeded' | 'failed',
    result?: unknown,
    error?: { code: number; message: string; data?: Record<string, unknown> }
  ): Promise<void> {
    if (!command.id || !command.node_id) return;
    try {
      await this.client.commandComplete({
        node_id: command.node_id,
        command_id: command.id,
        status,
        ...(result !== undefined ? { result } : {}),
        ...(error ? { error } : {})
      });
    } catch (err) {
      if (!signal.aborted) this.report(err);
    }
  }

  private async unregister(): Promise<void> {
    const id = this.nodeID;
    if (!id) return;
    try {
      await this.client.nodeUnregister(id);
    } catch (err) {
      this.report(err);
    }
  }
}
