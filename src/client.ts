import { EventEmitter } from 'events';
import * as grpc from '@grpc/grpc-js';

import {
  ACQUIRE_STATUS_FROM_WIRE,
  ASSERT_STATUS_FROM_WIRE,
  buildCredentials,
  CYCLE_KIND_FROM_WIRE,
  EVENT_TYPE_FROM_WIRE,
  loadPathlockdProto,
  MODE_TO_WIRE,
  RENEW_STATUS_FROM_WIRE,
  STATE_TO_WIRE,
} from './proto';
import {
  AcquireParams,
  AcquireResult,
  AssertResult,
  CycleResult,
  HealthResult,
  LockEvent,
  LockEventType,
  PathlockdClientOptions,
  ReleaseRequest,
  RenewResult,
} from './types';

function unary<T>(client: any, method: string, request: unknown): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    client[method](request, (err: grpc.ServiceError | null, res: T) => {
      if (err) reject(err);
      else resolve(res);
    });
  });
}

function wireRelease(r: ReleaseRequest) {
  return { path: r.path, mode: MODE_TO_WIRE[r.mode ?? 'write'] };
}

/**
 * A live, per-owner subscription to the pathlockd lifecycle event stream.
 *
 * It is bound to a single owner id and only ever surfaces events for that owner
 * (its cooperative `revoke`, a forced `kill`, or its own `released`).
 *
 * Emits:
 *  - `event`  → {@link LockEvent}
 *  - `error`  → a gRPC stream error (attach a listener; EventEmitter throws otherwise)
 *  - `end`    → server ended the stream
 *  - `close`  → underlying stream closed
 */
export class PathlockdSubscription extends EventEmitter {
  constructor(private readonly stream: grpc.ClientReadableStream<any>) {
    super();
    stream.on('data', (msg: any) => {
      const type = (EVENT_TYPE_FROM_WIRE[msg.type] ?? 'released') as LockEventType;
      const event: LockEvent = { type, ownerId: msg.ownerId };
      this.emit('event', event);
    });
    stream.on('error', (err: Error) => this.emit('error', err));
    stream.on('end', () => this.emit('end'));
    stream.on('close', () => this.emit('close'));
  }

  on(event: 'event', listener: (e: LockEvent) => void): this;
  on(event: 'error', listener: (err: Error) => void): this;
  on(event: 'end' | 'close', listener: () => void): this;
  on(event: string, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  /** Cancel the stream. */
  close(): void {
    this.stream.cancel();
  }
}

/**
 * Typed, promise-based client for the pathlockd `PathLock` service.
 *
 * Every method forwards a single gRPC call and maps the wire representation to
 * the ergonomic types in {@link types}. The lock *orchestration* (renewal loop,
 * deadlock resolution, retry/wait) lives in the caller — this client only
 * exposes the primitives.
 */
export class PathlockdClient {
  private readonly client: any;

  constructor(opts: PathlockdClientOptions) {
    const ns = loadPathlockdProto();
    this.client = new ns.PathLock(
      opts.endpoint,
      buildCredentials(opts.tls ?? false),
      opts.channelOptions ?? {},
    );
  }

  /** Wait until the channel is ready (or reject after `timeoutMs`). */
  waitForReady(timeoutMs = 5000): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const deadline = new Date(Date.now() + timeoutMs);
      this.client.waitForReady(deadline, (err?: Error) => (err ? reject(err) : resolve()));
    });
  }

  async acquire(params: AcquireParams): Promise<AcquireResult> {
    const request = {
      ownerId: params.ownerId,
      ttlMs: params.ttlMs,
      requests: params.requests.map((r) => ({
        path: r.path,
        mode: MODE_TO_WIRE[r.mode ?? 'write'],
        state: STATE_TO_WIRE[r.state ?? 'new'],
      })),
      fencingToken: params.fencingToken,
      releaseRequests: (params.releaseRequests ?? []).map(wireRelease),
      emitRelease: params.emitRelease ?? false,
    };
    const res = await unary<any>(this.client, 'acquire', request);
    return {
      status: (ACQUIRE_STATUS_FROM_WIRE[res.status] ?? 'ok') as AcquireResult['status'],
      path: res.path ?? '',
      owner: res.owner ?? '',
      reason: res.reason ?? '',
    };
  }

  async release(ownerId: string, requests: ReleaseRequest[], delWaitKey = false): Promise<void> {
    await unary(this.client, 'release', {
      ownerId,
      requests: requests.map(wireRelease),
      delWaitKey,
    });
  }

  async releaseAll(ownerId: string, delWaitKey = false): Promise<void> {
    await unary(this.client, 'releaseAll', { ownerId, delWaitKey });
  }

  async renew(ownerId: string, ttlMs: number): Promise<RenewResult> {
    const res = await unary<any>(this.client, 'renew', { ownerId, ttlMs });
    return {
      status: (RENEW_STATUS_FROM_WIRE[res.status] ?? 'ok') as RenewResult['status'],
      path: res.path ?? '',
      reason: res.reason ?? '',
    };
  }

  async forceRelease(victimId: string): Promise<void> {
    await unary(this.client, 'forceRelease', { victimId });
  }

  async assertFencing(ownerId: string, fencingToken: number, paths: string[]): Promise<AssertResult> {
    const res = await unary<any>(this.client, 'assertFencing', { ownerId, fencingToken, paths });
    return {
      status: (ASSERT_STATUS_FROM_WIRE[res.status] ?? 'ok') as AssertResult['status'],
      path: res.path ?? '',
      reason: res.reason ?? '',
    };
  }

  async detectCycle(startOwnerId: string, maxDepth: number): Promise<CycleResult> {
    const res = await unary<any>(this.client, 'detectCycle', { startOwnerId, maxDepth });
    return {
      kind: (CYCLE_KIND_FROM_WIRE[res.kind] ?? 'none') as CycleResult['kind'],
      chain: res.chain ?? [],
    };
  }

  async isBlocking(conflictPath: string, conflictOwner: string, reason: string): Promise<boolean> {
    const res = await unary<any>(this.client, 'isBlocking', { conflictPath, conflictOwner, reason });
    return Boolean(res.blocking);
  }

  async incrFencingToken(): Promise<number> {
    const res = await unary<any>(this.client, 'incrFencingToken', {});
    return Number(res.token);
  }

  async setWaitEdge(ownerId: string, conflictOwner: string, ttlMs: number): Promise<void> {
    await unary(this.client, 'setWaitEdge', { ownerId, conflictOwner, ttlMs });
  }

  async clearWaitEdge(ownerId: string): Promise<void> {
    await unary(this.client, 'clearWaitEdge', { ownerId });
  }

  async isOwnerAlive(ownerId: string): Promise<boolean> {
    const res = await unary<any>(this.client, 'isOwnerAlive', { ownerId });
    return Boolean(res.alive);
  }

  async requestRevoke(ownerId: string): Promise<void> {
    await unary(this.client, 'requestRevoke', { ownerId });
  }

  /**
   * Open the per-owner event stream for `ownerId`. The returned subscription
   * only ever emits events for that owner (its `revoke`, `kill`, or own
   * `released`). Returns immediately; events arrive via the emitter.
   */
  subscribe(ownerId: string): PathlockdSubscription {
    const stream: grpc.ClientReadableStream<any> = this.client.subscribe({ ownerId });
    return new PathlockdSubscription(stream);
  }

  async health(): Promise<HealthResult> {
    const res = await unary<any>(this.client, 'health', {});
    return { ok: Boolean(res.ok), detail: res.detail ?? '' };
  }

  close(): void {
    this.client.close();
  }
}

// ---------------------------------------------------------------------------
// Debug client (PathLockDebug) — for tests / fault injection only.
// ---------------------------------------------------------------------------

export interface OwnedPathsResult {
  members: string[];
  alive: boolean;
}

export class PathlockdDebugClient {
  private readonly client: any;

  constructor(opts: PathlockdClientOptions) {
    const ns = loadPathlockdProto();
    this.client = new ns.PathLockDebug(
      opts.endpoint,
      buildCredentials(opts.tls ?? false),
      opts.channelOptions ?? {},
    );
  }

  waitForReady(timeoutMs = 5000): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const deadline = new Date(Date.now() + timeoutMs);
      this.client.waitForReady(deadline, (err?: Error) => (err ? reject(err) : resolve()));
    });
  }

  async flush(): Promise<number> {
    const res = await unary<any>(this.client, 'flush', {});
    return Number(res.deleted ?? 0);
  }

  async expireOwner(ownerId: string): Promise<void> {
    await unary(this.client, 'expireOwner', { ownerId });
  }

  async deleteLockKey(path: string, mode: 'write' | 'read', ownerId = ''): Promise<void> {
    await unary(this.client, 'deleteLockKey', { path, mode: MODE_TO_WIRE[mode], ownerId });
  }

  async setWriteOwner(path: string, ownerId: string): Promise<void> {
    await unary(this.client, 'setWriteOwner', { path, ownerId });
  }

  async getWriteOwner(path: string): Promise<string | null> {
    const res = await unary<any>(this.client, 'getWriteOwner', { path });
    return res.exists ? res.ownerId : null;
  }

  async setFence(path: string, value: number): Promise<void> {
    await unary(this.client, 'setFence', { path, value });
  }

  async getFence(path: string): Promise<number | null> {
    const res = await unary<any>(this.client, 'getFence', { path });
    return res.exists ? Number(res.value) : null;
  }

  async setFencingCounter(value: number): Promise<void> {
    await unary(this.client, 'setFencingCounter', { value });
  }

  async getFencingCounter(): Promise<number> {
    const res = await unary<any>(this.client, 'getFencingCounter', {});
    return Number(res.value ?? 0);
  }

  async ownedPaths(ownerId: string): Promise<OwnedPathsResult> {
    const res = await unary<any>(this.client, 'ownedPaths', { ownerId });
    return { members: res.members ?? [], alive: Boolean(res.alive) };
  }

  close(): void {
    this.client.close();
  }
}
