import { EventEmitter, once } from 'events';
import * as grpc from '@grpc/grpc-js';

import {
  ACQUIRE_STATUS_FROM_WIRE,
  ASSERT_STATUS_FROM_WIRE,
  buildCredentials,
  ClientStreamingMethod,
  CYCLE_KIND_FROM_WIRE,
  decodeWireEnum,
  EVENT_TYPE_FROM_WIRE,
  loadPathlockdProto,
  MODE_FROM_WIRE,
  MODE_TO_WIRE,
  PathLockServiceClient,
  RENEW_STATUS_FROM_WIRE,
  STATE_TO_WIRE,
  bigintToWireInt64,
  toWirePositiveUint64,
  toWireUint64,
  UnaryMethod,
  WireAcquireRequest,
  WireAcquireResponse,
  WireEvent,
  WireReleaseRequest,
  wireInt64ToBigInt,
} from './proto';
import {
  AcquireParams,
  AcquireResult,
  AssertResult,
  CycleResult,
  HealthResult,
  IdempotentRequestOptions,
  LockEntry,
  LockEvent,
  OwnedLockInfo,
  OwnerLocksResult,
  PathLockInfo,
  PathlockdClientOptions,
  ReleaseOptions,
  ReleaseRequest,
  RenewOptions,
  RenewResult,
  SetWaitEdgeMetadata,
} from './types';

/**
 * Safety cap for {@link PathlockdClient.dumpLocks} when the caller does not set
 * one: an unbounded cluster dump could exhaust memory, so collection stops and
 * throws past this many entries. Page manually with a higher cap if needed.
 */
const DUMP_DEFAULT_MAX_ENTRIES = 100_000;
const ACQUIRE_UNARY_MAX_PATHS = 1024;
const ACQUIRE_STREAM_CHUNK_PATHS = 1024;

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

function clientStreaming<Req, Res>(
  client: PathLockServiceClient,
  method: 'acquireStream',
  requests: Iterable<Req>,
): Promise<Res> {
  return new Promise<Res>((resolve, reject) => {
    let settled = false;
    const settleResolve = (response: Res) => {
      if (settled) return;
      settled = true;
      resolve(response);
    };
    const settleReject = (err: unknown) => {
      if (settled) return;
      settled = true;
      reject(err);
    };

    const fn = client[method] as unknown as ClientStreamingMethod<Req, Res>;
    const call = fn.call(client, (err, response) => (err ? settleReject(err) : settleResolve(response)));
    call.on('error', settleReject);

    void (async () => {
      try {
        for (const request of requests) {
          if (!call.write(request)) {
            await once(call, 'drain');
          }
        }
        call.end();
      } catch (err) {
        call.destroy();
        settleReject(err);
      }
    })();
  });
}

function wireRelease(r: ReleaseRequest): WireReleaseRequest {
  return { path: r.path, mode: MODE_TO_WIRE[r.mode ?? 'write'] };
}

function wireAcquireRequest(params: AcquireParams): WireAcquireRequest {
  return {
    ownerId: params.ownerId,
    ttlMs: toWirePositiveUint64(params.ttlMs, 'Acquire.ttlMs'),
    requests: params.requests.map((r) => ({
      path: r.path,
      mode: MODE_TO_WIRE[r.mode ?? 'write'],
      state: STATE_TO_WIRE[r.state ?? 'new'],
    })),
    fencingToken: bigintToWireInt64(params.fencingToken, 'Acquire.fencingToken'),
    releaseRequests: (params.releaseRequests ?? []).map(wireRelease),
    queueTtlMs: toWireUint64(params.queueTtlMs ?? 0, 'Acquire.queueTtlMs'),
    ...idempotencyFields(params),
  };
}

function decodeAcquireResponse(res: WireAcquireResponse): AcquireResult {
  return {
    status: decodeWireEnum(ACQUIRE_STATUS_FROM_WIRE, res.status, 'AcquireResponse.status'),
    path: res.path ?? '',
    owner: res.owner ?? '',
    reason: res.reason ?? '',
  };
}

function acquirePathCount(request: WireAcquireRequest): number {
  return request.requests.length + request.releaseRequests.length;
}

function* chunkAcquireRequest(request: WireAcquireRequest): Iterable<WireAcquireRequest> {
  let requestIndex = 0;
  let releaseIndex = 0;
  let first = true;

  while (requestIndex < request.requests.length || releaseIndex < request.releaseRequests.length) {
    const requests = request.requests.slice(requestIndex, requestIndex + ACQUIRE_STREAM_CHUNK_PATHS);
    requestIndex += requests.length;

    const remaining = ACQUIRE_STREAM_CHUNK_PATHS - requests.length;
    const releaseRequests = request.releaseRequests.slice(releaseIndex, releaseIndex + remaining);
    releaseIndex += releaseRequests.length;

    yield {
      ownerId: first ? request.ownerId : '',
      ttlMs: first ? request.ttlMs : 0,
      requests,
      fencingToken: first ? request.fencingToken : 0,
      releaseRequests,
      queueTtlMs: first ? request.queueTtlMs : 0,
      ...(first && request.idempotencyKey ? { idempotencyKey: request.idempotencyKey } : {}),
    };
    first = false;
  }
}

function hasWriteRequest(params: AcquireParams): boolean {
  return params.requests.some((r) => (r.mode ?? 'write') === 'write');
}

function assertPositiveFencingToken(value: bigint, fieldName: string): void {
  if (typeof value !== 'bigint' || value <= 0n) {
    throw new Error(`${fieldName} must be a positive bigint`);
  }
}

function idempotencyFields(options?: IdempotentRequestOptions): { idempotencyKey?: string } {
  return options?.idempotencyKey ? { idempotencyKey: options.idempotencyKey } : {};
}

function normalizeReleaseOptions(optionsOrDelWaitKey?: boolean | ReleaseOptions): ReleaseOptions {
  if (typeof optionsOrDelWaitKey === 'boolean') {
    return { delWaitKey: optionsOrDelWaitKey };
  }
  return optionsOrDelWaitKey ?? {};
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
 * (its cooperative `revoke` or a forced `kill`).
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
    const req = wireAcquireRequest(params);
    const res =
      acquirePathCount(req) <= ACQUIRE_UNARY_MAX_PATHS
        ? await unary(this.client, 'acquire', req)
        : await clientStreaming<WireAcquireRequest, WireAcquireResponse>(
            this.client,
            'acquireStream',
            chunkAcquireRequest(req),
          );
    return decodeAcquireResponse(res);
  }

  async release(ownerId: string, requests: ReleaseRequest[], delWaitKey?: boolean): Promise<void>;
  async release(ownerId: string, requests: ReleaseRequest[], options?: ReleaseOptions): Promise<void>;
  async release(
    ownerId: string,
    requests: ReleaseRequest[],
    optionsOrDelWaitKey: boolean | ReleaseOptions = false,
  ): Promise<void> {
    const options = normalizeReleaseOptions(optionsOrDelWaitKey);
    await unary(this.client, 'release', {
      ownerId,
      requests: requests.map(wireRelease),
      delWaitKey: options.delWaitKey ?? false,
      ...idempotencyFields(options),
    });
  }

  async releaseAll(ownerId: string, delWaitKey?: boolean): Promise<void>;
  async releaseAll(ownerId: string, options?: ReleaseOptions): Promise<void>;
  async releaseAll(
    ownerId: string,
    optionsOrDelWaitKey: boolean | ReleaseOptions = false,
  ): Promise<void> {
    const options = normalizeReleaseOptions(optionsOrDelWaitKey);
    await unary(this.client, 'releaseAll', {
      ownerId,
      delWaitKey: options.delWaitKey ?? false,
      ...idempotencyFields(options),
    });
  }

  async renew(ownerId: string, ttlMs: number, options: RenewOptions = {}): Promise<RenewResult> {
    const res = await unary(this.client, 'renew', {
      ownerId,
      ttlMs: toWirePositiveUint64(ttlMs, 'Renew.ttlMs'),
      domains: options.domains ?? [],
      ...idempotencyFields(options),
    });
    return {
      status: decodeWireEnum(RENEW_STATUS_FROM_WIRE, res.status, 'RenewResponse.status'),
      path: res.path ?? '',
      reason: res.reason ?? '',
    };
  }

  async forceRelease(victimId: string, options?: IdempotentRequestOptions): Promise<void> {
    await unary(this.client, 'forceRelease', { victimId, ...idempotencyFields(options) });
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

  async incrFencingToken(options?: IdempotentRequestOptions): Promise<bigint> {
    const res = await unary(this.client, 'incrFencingToken', idempotencyFields(options));
    return wireInt64ToBigInt(res.token, 'IncrFencingTokenResponse.token');
  }

  async setWaitEdge(
    ownerId: string,
    conflictOwner: string,
    ttlMs: number,
    metadata?: SetWaitEdgeMetadata,
    options?: IdempotentRequestOptions,
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
      ...idempotencyFields(options),
    });
  }

  async clearWaitEdge(ownerId: string, options?: IdempotentRequestOptions): Promise<void> {
    await unary(this.client, 'clearWaitEdge', { ownerId, ...idempotencyFields(options) });
  }

  async isOwnerAlive(ownerId: string): Promise<boolean> {
    const res = await unary(this.client, 'isOwnerAlive', { ownerId });
    return Boolean(res.alive);
  }

  /**
   * Read-only snapshot of the lock state at one exact path: live write owner,
   * live read owners, fence value and any preemption claim. Filtered by owner
   * liveness; never mutates daemon state.
   */
  async inspectPath(path: string): Promise<PathLockInfo> {
    const res = await unary(this.client, 'inspectPath', { path });
    return {
      writeOwner: res.writeOwner ? res.writeOwner : null,
      readOwners: res.readOwners ?? [],
      fence: res.hasFence ? wireInt64ToBigInt(res.fence, 'InspectPathResponse.fence') : null,
      claimOwner: res.claimOwner ? res.claimOwner : null,
    };
  }

  /**
   * Read-only listing of every lock recorded for one owner, plus whether its
   * liveness lease is still present. The owner-centric companion to
   * {@link inspectPath}.
   */
  async listOwnerLocks(ownerId: string): Promise<OwnerLocksResult> {
    const res = await unary(this.client, 'listOwnerLocks', { ownerId });
    const locks: OwnedLockInfo[] = (res.locks ?? []).map((l) => ({
      path: l.path,
      mode: decodeWireEnum(MODE_FROM_WIRE, l.mode, 'OwnedLock.mode'),
    }));
    return { alive: Boolean(res.alive), locks };
  }

  /**
   * Dump every live lock across the cluster, auto-paginating internally. Each
   * entry is one (owner, mode, path) holding with the fence for write locks.
   *
   * Best-effort observability: the daemon reads each owner in its own snapshot,
   * so the result is near-real-time, not a single global instant. To bound
   * memory, collection stops and throws once `maxEntries` entries are seen
   * (default {@link DUMP_DEFAULT_MAX_ENTRIES}); for very large clusters drive
   * {@link dumpLocksPages} directly and stream instead.
   */
  async dumpLocks(opts: { ownerPage?: number; maxEntries?: number } = {}): Promise<LockEntry[]> {
    const maxEntries = opts.maxEntries ?? DUMP_DEFAULT_MAX_ENTRIES;
    const out: LockEntry[] = [];
    for await (const page of this.dumpLocksPages(opts.ownerPage)) {
      for (const entry of page) {
        if (out.length >= maxEntries) {
          throw new Error(
            `dumpLocks exceeded maxEntries (${maxEntries}); raise the cap or page with dumpLocksPages`,
          );
        }
        out.push(entry);
      }
    }
    return out;
  }

  /**
   * Lower-level dump: an async generator yielding one decoded page of lock
   * entries per daemon round-trip. Lets callers stream an arbitrarily large
   * cluster without buffering it all. `ownerPage` sets how many owners the
   * daemon scans per page (0 / omitted uses the server default).
   */
  async *dumpLocksPages(ownerPage = 0): AsyncGenerator<LockEntry[]> {
    let cursor: Buffer | Uint8Array = Buffer.alloc(0);
    for (;;) {
      const res = await unary(this.client, 'dumpLocks', { cursor, ownerPage });
      const page: LockEntry[] = (res.entries ?? []).map((e) => ({
        owner: e.owner,
        path: e.path,
        mode: decodeWireEnum(MODE_FROM_WIRE, e.mode, 'LockEntry.mode'),
        fence: e.hasFence ? wireInt64ToBigInt(e.fence, 'LockEntry.fence') : null,
      }));
      if (page.length > 0) yield page;
      if (res.done) return;
      cursor = res.nextCursor;
    }
  }

  /**
   * Publish a cooperative REVOKE for `ownerId`: the daemon asks that owner to
   * release its locks (to break a detected deadlock cycle). The wait queue's
   * FIFO admission keeps the revoked victim queued behind the winner, so no
   * preemption reservation is needed.
   */
  async requestRevoke(ownerId: string): Promise<void> {
    await unary(this.client, 'requestRevoke', { ownerId });
  }

  /**
   * Open the per-owner event stream for `ownerId`. The returned subscription
   * only ever emits events for that owner (its `revoke` or `kill`). Returns
   * immediately; events arrive via the emitter.
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
