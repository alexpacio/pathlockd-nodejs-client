import { EventEmitter } from 'events';
import * as grpc from '@grpc/grpc-js';

import {
  ACQUIRE_STATUS_FROM_WIRE,
  ASSERT_STATUS_FROM_WIRE,
  buildCredentials,
  CYCLE_KIND_FROM_WIRE,
  decodeWireEnum,
  EVENT_TYPE_FROM_WIRE,
  loadPathlockdProto,
  MODE_TO_WIRE,
  PathLockDebugServiceClient,
  PathLockServiceClient,
  RENEW_STATUS_FROM_WIRE,
  STATE_TO_WIRE,
  bigintToWireInt64,
  toWireInt64,
  toWirePositiveUint64,
  UnaryMethod,
  WireEvent,
  WireReleaseRequest,
  WireRequestRevokeRequest,
  wireInt64ToBigInt,
  wireInt64ToSafeNumber,
} from './proto';
import {
  AcquireParams,
  AcquireResult,
  AssertResult,
  CycleResult,
  HealthResult,
  LockEvent,
  LockMode,
  PathlockdClientOptions,
  PreemptionClaim,
  ReleaseRequest,
  RenewResult,
  SetWaitEdgeMetadata,
} from './types';

/** The request type accepted by the unary method named `K` on client `C`. */
type RequestOf<C, K extends keyof C> = C[K] extends UnaryMethod<infer Req, infer _Res> ? Req : never;
/** The response type returned by the unary method named `K` on client `C`. */
type ResponseOf<C, K extends keyof C> = C[K] extends UnaryMethod<infer _Req, infer Res> ? Res : never;

/** Promisify a callback-style unary call, dispatched by method name on `client`. */
function unary<C, K extends keyof C>(
  client: C,
  method: K,
  request: RequestOf<C, K>,
): Promise<ResponseOf<C, K>> {
  return new Promise<ResponseOf<C, K>>((resolve, reject) => {
    const fn = client[method] as unknown as UnaryMethod<RequestOf<C, K>, ResponseOf<C, K>>;
    // Member dispatch is lost when the method is held in a local, so re-bind
    // `this` to the client (grpc-js client methods rely on it).
    fn.call(client, request, (err, response) => (err ? reject(err) : resolve(response)));
  });
}

function wireRelease(r: ReleaseRequest): WireReleaseRequest {
  return { path: r.path, mode: MODE_TO_WIRE[r.mode ?? 'write'] };
}

function hasWriteRequest(params: AcquireParams): boolean {
  return params.requests.some((r) => (r.mode ?? 'write') === 'write');
}

function assertPositiveFencingToken(value: bigint, fieldName: string): void {
  if (typeof value !== 'bigint' || value <= 0n) {
    throw new Error(`${fieldName} must be a positive bigint`);
  }
}

/** Event name → listener signature for {@link PathlockdSubscription}. */
interface SubscriptionEvents {
  event: (e: LockEvent) => void;
  error: (err: Error) => void;
  end: () => void;
  close: () => void;
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
  constructor(private readonly stream: grpc.ClientReadableStream<WireEvent>) {
    super();
    stream.on('data', (msg: WireEvent) => {
      try {
        const type = decodeWireEnum(EVENT_TYPE_FROM_WIRE, msg.type, 'Event.type');
        const event: LockEvent = { type, ownerId: msg.ownerId };
        this.emit('event', event);
      } catch (err) {
        this.emit('error', err instanceof Error ? err : new Error(String(err)));
      }
    });
    stream.on('error', (err: Error) => this.emit('error', err));
    stream.on('end', () => this.emit('end'));
    stream.on('close', () => this.emit('close'));
  }

  override on<E extends keyof SubscriptionEvents>(event: E, listener: SubscriptionEvents[E]): this {
    return super.on(event, listener as SubscriptionEvents[keyof SubscriptionEvents]);
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
  private readonly client: PathLockServiceClient;

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
    if (hasWriteRequest(params)) {
      assertPositiveFencingToken(params.fencingToken, 'Acquire.fencingToken');
    }
    const res = await unary(this.client, 'acquire', {
      ownerId: params.ownerId,
      ttlMs: toWirePositiveUint64(params.ttlMs, 'Acquire.ttlMs'),
      requests: params.requests.map((r) => ({
        path: r.path,
        mode: MODE_TO_WIRE[r.mode ?? 'write'],
        state: STATE_TO_WIRE[r.state ?? 'new'],
      })),
      fencingToken: bigintToWireInt64(params.fencingToken, 'Acquire.fencingToken'),
      releaseRequests: (params.releaseRequests ?? []).map(wireRelease),
      emitRelease: params.emitRelease ?? false,
    });
    return {
      status: decodeWireEnum(ACQUIRE_STATUS_FROM_WIRE, res.status, 'AcquireResponse.status'),
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
    const res = await unary(this.client, 'renew', {
      ownerId,
      ttlMs: toWirePositiveUint64(ttlMs, 'Renew.ttlMs'),
    });
    return {
      status: decodeWireEnum(RENEW_STATUS_FROM_WIRE, res.status, 'RenewResponse.status'),
      path: res.path ?? '',
      reason: res.reason ?? '',
    };
  }

  async forceRelease(victimId: string): Promise<void> {
    await unary(this.client, 'forceRelease', { victimId });
  }

  async assertFencing(ownerId: string, fencingToken: bigint, paths: string[]): Promise<AssertResult> {
    if (paths.length > 0) {
      assertPositiveFencingToken(fencingToken, 'AssertFencing.fencingToken');
    }
    const res = await unary(this.client, 'assertFencing', {
      ownerId,
      fencingToken: bigintToWireInt64(fencingToken, 'AssertFencing.fencingToken'),
      paths,
    });
    return {
      status: decodeWireEnum(ASSERT_STATUS_FROM_WIRE, res.status, 'AssertFencingResponse.status'),
      path: res.path ?? '',
      reason: res.reason ?? '',
    };
  }

  async detectCycle(startOwnerId: string, maxDepth: number): Promise<CycleResult> {
    const res = await unary(this.client, 'detectCycle', { startOwnerId, maxDepth });
    return {
      kind: decodeWireEnum(CYCLE_KIND_FROM_WIRE, res.kind, 'DetectCycleResponse.kind'),
      chain: res.chain ?? [],
    };
  }

  async isBlocking(conflictPath: string, conflictOwner: string, reason: string): Promise<boolean> {
    const res = await unary(this.client, 'isBlocking', { conflictPath, conflictOwner, reason });
    return Boolean(res.blocking);
  }

  async incrFencingToken(): Promise<bigint> {
    const res = await unary(this.client, 'incrFencingToken', {});
    return wireInt64ToBigInt(res.token, 'IncrFencingTokenResponse.token');
  }

  async setWaitEdge(
    ownerId: string,
    conflictOwner: string,
    ttlMs: number,
    metadata?: SetWaitEdgeMetadata,
  ): Promise<void> {
    if (metadata && (!metadata.conflictPath || !metadata.reason)) {
      throw new Error('SetWaitEdge metadata requires both conflictPath and reason');
    }
    await unary(this.client, 'setWaitEdge', {
      ownerId,
      conflictOwner,
      ttlMs: toWirePositiveUint64(ttlMs, 'SetWaitEdge.ttlMs'),
      conflictPath: metadata?.conflictPath ?? '',
      reason: metadata?.reason ?? '',
    });
  }

  async clearWaitEdge(ownerId: string): Promise<void> {
    await unary(this.client, 'clearWaitEdge', { ownerId });
  }

  async isOwnerAlive(ownerId: string): Promise<boolean> {
    const res = await unary(this.client, 'isOwnerAlive', { ownerId });
    return Boolean(res.alive);
  }

  /**
   * Publish a cooperative REVOKE for `ownerId`. When `claim` is supplied, the
   * daemon also reserves `claim.path` for `claim.claimantOwnerId` (for
   * `claim.ttlMs`, or a short default) before publishing, so the revoked victim
   * cannot re-acquire the path before the claimant does. Omitting `claim`
   * yields the legacy pure-notification behavior.
   */
  async requestRevoke(ownerId: string, claim?: PreemptionClaim): Promise<void> {
    const req: WireRequestRevokeRequest = { ownerId };
    if (claim) {
      req.claimPath = claim.path;
      req.claimantOwnerId = claim.claimantOwnerId;
      req.claimTtlMs = String(claim.ttlMs ?? 0);
    }
    await unary(this.client, 'requestRevoke', req);
  }

  /**
   * Open the per-owner event stream for `ownerId`. The returned subscription
   * only ever emits events for that owner (its `revoke`, `kill`, or own
   * `released`). Returns immediately; events arrive via the emitter.
   */
  subscribe(ownerId: string): PathlockdSubscription {
    const stream = this.client.subscribe({ ownerId });
    return new PathlockdSubscription(stream);
  }

  async health(): Promise<HealthResult> {
    const res = await unary(this.client, 'health', {});
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
  private readonly client: PathLockDebugServiceClient;

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
    const res = await unary(this.client, 'flush', {});
    return wireInt64ToSafeNumber(res.deleted ?? '0', 'FlushResponse.deleted');
  }

  async expireOwner(ownerId: string): Promise<void> {
    await unary(this.client, 'expireOwner', { ownerId });
  }

  async deleteLockKey(path: string, mode: LockMode, ownerId = ''): Promise<void> {
    await unary(this.client, 'deleteLockKey', { path, mode: MODE_TO_WIRE[mode], ownerId });
  }

  async setWriteOwner(path: string, ownerId: string): Promise<void> {
    await unary(this.client, 'setWriteOwner', { path, ownerId });
  }

  async getWriteOwner(path: string): Promise<string | null> {
    const res = await unary(this.client, 'getWriteOwner', { path });
    return res.exists ? res.ownerId : null;
  }

  async setFence(path: string, value: bigint): Promise<void> {
    await unary(this.client, 'setFence', {
      path,
      value: bigintToWireInt64(value, 'SetFence.value'),
    });
  }

  async getFence(path: string): Promise<bigint | null> {
    const res = await unary(this.client, 'getFence', { path });
    return res.exists ? wireInt64ToBigInt(res.value, 'GetFenceResponse.value') : null;
  }

  async setFencingCounter(value: number): Promise<void> {
    await unary(this.client, 'setFencingCounter', {
      value: toWireInt64(value, 'SetFencingCounter.value'),
    });
  }

  async getFencingCounter(): Promise<number> {
    const res = await unary(this.client, 'getFencingCounter', {});
    return wireInt64ToSafeNumber(res.value ?? '0', 'GetFencingCounterResponse.value');
  }

  async ownedPaths(ownerId: string): Promise<OwnedPathsResult> {
    const res = await unary(this.client, 'ownedPaths', { ownerId });
    return { members: res.members ?? [], alive: Boolean(res.alive) };
  }

  close(): void {
    this.client.close();
  }
}
