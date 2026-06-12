import { EventEmitter } from 'events';
import * as grpc from '@grpc/grpc-js';
import { WireEvent } from './proto';
import { AcquireParams, AcquireResult, AssertResult, CycleResult, HealthResult, IdempotentRequestOptions, LockEntry, LockEvent, OwnerLocksResult, PathLockInfo, PathlockdClientOptions, PreemptionClaim, ReleaseOptions, ReleaseRequest, RenewOptions, RenewResult, SetClaimOptions, SetClaimResult, SetWaitEdgeMetadata } from './types';
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
export declare class PathlockdSubscription extends EventEmitter {
    private readonly stream;
    constructor(stream: grpc.ClientReadableStream<WireEvent>);
    on<E extends keyof SubscriptionEvents>(event: E, listener: SubscriptionEvents[E]): this;
    /** Cancel the stream. */
    close(): void;
}
/**
 * Typed, promise-based client for the pathlockd `PathLock` service.
 *
 * Every method forwards a single gRPC call and maps the wire representation to
 * the ergonomic types in {@link types}. The lock *orchestration* (renewal loop,
 * deadlock resolution, retry/wait) lives in the caller — this client only
 * exposes the primitives.
 */
export declare class PathlockdClient {
    private readonly client;
    constructor(opts: PathlockdClientOptions);
    /** Wait until the channel is ready (or reject after `timeoutMs`). */
    waitForReady(timeoutMs?: number): Promise<void>;
    acquire(params: AcquireParams): Promise<AcquireResult>;
    release(ownerId: string, requests: ReleaseRequest[], delWaitKey?: boolean): Promise<void>;
    release(ownerId: string, requests: ReleaseRequest[], options?: ReleaseOptions): Promise<void>;
    releaseAll(ownerId: string, delWaitKey?: boolean): Promise<void>;
    releaseAll(ownerId: string, options?: ReleaseOptions): Promise<void>;
    renew(ownerId: string, ttlMs: number, options?: RenewOptions): Promise<RenewResult>;
    forceRelease(victimId: string, options?: IdempotentRequestOptions): Promise<void>;
    assertFencing(ownerId: string, fencingToken: bigint, paths: string[]): Promise<AssertResult>;
    detectCycle(startOwnerId: string, maxDepth: number): Promise<CycleResult>;
    isBlocking(conflictPath: string, conflictOwner: string, reason: string): Promise<boolean>;
    incrFencingToken(options?: IdempotentRequestOptions): Promise<bigint>;
    setWaitEdge(ownerId: string, conflictOwner: string, ttlMs: number, metadata?: SetWaitEdgeMetadata, options?: IdempotentRequestOptions): Promise<void>;
    clearWaitEdge(ownerId: string, options?: IdempotentRequestOptions): Promise<void>;
    /**
     * Plant an anti-starvation claim reserving `path` for `claimantOwnerId`.
     * Claim-if-absent: a live claim by another claimant is reported as `held`
     * (never overwritten); re-planting one's own claim re-arms its TTL. Claims
     * are TTL-governed only — the claimant needs no lease, so a pure waiter can
     * reserve the path it is queued for, and a crashed claimant's reservation
     * expires on its own. The claimant's own acquire consumes the claim
     * atomically on grant.
     */
    setClaim(path: string, claimantOwnerId: string, ttlMs?: number): Promise<SetClaimResult>;
    setClaim(path: string, claimantOwnerId: string, options?: SetClaimOptions): Promise<SetClaimResult>;
    /** Clear `claimantOwnerId`'s own claim on `path`; a foreign claim is untouched. */
    clearClaim(path: string, claimantOwnerId: string, options?: IdempotentRequestOptions): Promise<void>;
    isOwnerAlive(ownerId: string): Promise<boolean>;
    /**
     * Read-only snapshot of the lock state at one exact path: live write owner,
     * live read owners, fence value and any preemption claim. Filtered by owner
     * liveness; never mutates daemon state.
     */
    inspectPath(path: string): Promise<PathLockInfo>;
    /**
     * Read-only listing of every lock recorded for one owner, plus whether its
     * liveness lease is still present. The owner-centric companion to
     * {@link inspectPath}.
     */
    listOwnerLocks(ownerId: string): Promise<OwnerLocksResult>;
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
    dumpLocks(opts?: {
        ownerPage?: number;
        maxEntries?: number;
    }): Promise<LockEntry[]>;
    /**
     * Lower-level dump: an async generator yielding one decoded page of lock
     * entries per daemon round-trip. Lets callers stream an arbitrarily large
     * cluster without buffering it all. `ownerPage` sets how many owners the
     * daemon scans per page (0 / omitted uses the server default).
     */
    dumpLocksPages(ownerPage?: number): AsyncGenerator<LockEntry[]>;
    /**
     * Publish a cooperative REVOKE for `ownerId`. When `claim` is supplied, the
     * daemon also reserves `claim.path` for `claim.claimantOwnerId` (for
     * `claim.ttlMs`, or a short default) before publishing, so the revoked victim
     * cannot re-acquire the path before the claimant does. Omitting `claim`
     * yields the legacy pure-notification behavior.
     */
    requestRevoke(ownerId: string, claim?: PreemptionClaim): Promise<void>;
    /**
     * Open the per-owner event stream for `ownerId`. The returned subscription
     * only ever emits events for that owner (its `revoke`, `kill`, or own
     * `released`). Returns immediately; events arrive via the emitter.
     */
    subscribe(ownerId: string): PathlockdSubscription;
    health(): Promise<HealthResult>;
    close(): void;
}
export {};
//# sourceMappingURL=client.d.ts.map