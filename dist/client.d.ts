import { EventEmitter } from 'events';
import * as grpc from '@grpc/grpc-js';
import { WireEvent } from './proto';
import { AcquireParams, AcquireResult, AssertResult, CycleResult, HealthResult, IdempotentRequestOptions, LockAlgorithm, LockEntry, LockEvent, NamespacePolicyResult, OwnerLocksResult, OwnerReadOptions, PathLockInfo, PathlockdClientOptions, ReasonCode, ReleaseAllOptions, ReleaseOptions, ReleaseRequest, RenewOptions, RenewResult, SetWaitEdgeMetadata } from './types';
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
 * (its cooperative `revoke`, a forced `kill`, or a queued acquire `grant`).
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
    private readonly defaultCallTimeoutMs?;
    constructor(opts: PathlockdClientOptions);
    /**
     * Build grpc call options for one RPC: a deadline (per-call `deadlineMs`,
     * else the client default). The deadline spans any automatic transport
     * retries. Abort-signal cancellation is wired separately (grpc-js exposes it
     * through the call handle, not these options) — see {@link unary}.
     */
    private callOptions;
    /** Wait until the channel is ready (or reject after `timeoutMs`). */
    waitForReady(timeoutMs?: number): Promise<void>;
    acquire(params: AcquireParams): Promise<AcquireResult>;
    setNamespacePolicy(namespace: string, algorithm: LockAlgorithm, options?: IdempotentRequestOptions): Promise<void>;
    getNamespacePolicy(namespace: string): Promise<NamespacePolicyResult>;
    deleteNamespacePolicy(namespace: string, options?: IdempotentRequestOptions): Promise<void>;
    release(ownerId: string, requests: ReleaseRequest[], delWaitKey?: boolean): Promise<void>;
    release(ownerId: string, requests: ReleaseRequest[], options?: ReleaseOptions): Promise<void>;
    releaseAll(ownerId: string, delWaitKey?: boolean): Promise<void>;
    releaseAll(ownerId: string, options?: ReleaseAllOptions): Promise<void>;
    renew(ownerId: string, ttlMs: number, options?: RenewOptions): Promise<RenewResult>;
    forceRelease(victimId: string, options?: IdempotentRequestOptions): Promise<void>;
    assertFencing(ownerId: string, fencingToken: bigint, paths: string[]): Promise<AssertResult>;
    detectCycle(startOwnerId: string, maxDepth: number): Promise<CycleResult>;
    isBlocking(conflictPath: string, conflictOwner: string, reason: ReasonCode): Promise<boolean>;
    incrFencingToken(options?: IdempotentRequestOptions): Promise<bigint>;
    setWaitEdge(ownerId: string, conflictOwner: string, ttlMs: number, metadata?: SetWaitEdgeMetadata, options?: IdempotentRequestOptions): Promise<void>;
    clearWaitEdge(ownerId: string, options?: IdempotentRequestOptions): Promise<void>;
    isOwnerAlive(ownerId: string, options?: OwnerReadOptions): Promise<boolean>;
    /**
     * Read-only snapshot of the lock state at one exact path: live write owner,
     * live read owners, semaphore owners, and fence value. Filtered by owner
     * liveness; never mutates daemon state.
     */
    inspectPath(path: string): Promise<PathLockInfo>;
    /**
     * Read-only listing of every lock recorded for one owner, plus whether its
     * liveness lease is still present. The owner-centric companion to
     * {@link inspectPath}.
     */
    listOwnerLocks(ownerId: string, options?: OwnerReadOptions): Promise<OwnerLocksResult>;
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
     * Publish a cooperative REVOKE for `ownerId`: the daemon asks that owner to
     * release its locks (to break a detected deadlock cycle). The wait queue's
     * FIFO admission keeps the revoked victim queued behind the winner, so no
     * preemption reservation is needed.
     */
    requestRevoke(ownerId: string): Promise<void>;
    /**
     * Open the per-owner event stream for `ownerId`. The returned subscription
     * only ever emits events for that owner (its `revoke`, `kill`, or `grant`). Returns
     * immediately; events arrive via the emitter.
     */
    subscribe(ownerId: string): PathlockdSubscription;
    health(): Promise<HealthResult>;
    close(): void;
}
export {};
//# sourceMappingURL=client.d.ts.map