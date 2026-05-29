import { EventEmitter } from 'events';
import * as grpc from '@grpc/grpc-js';
import { WireEvent } from './proto';
import { AcquireParams, AcquireResult, AssertResult, CycleResult, HealthResult, LockEvent, LockMode, PathlockdClientOptions, ReleaseRequest, RenewResult } from './types';
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
    releaseAll(ownerId: string, delWaitKey?: boolean): Promise<void>;
    renew(ownerId: string, ttlMs: number): Promise<RenewResult>;
    forceRelease(victimId: string): Promise<void>;
    assertFencing(ownerId: string, fencingToken: number, paths: string[]): Promise<AssertResult>;
    detectCycle(startOwnerId: string, maxDepth: number): Promise<CycleResult>;
    isBlocking(conflictPath: string, conflictOwner: string, reason: string): Promise<boolean>;
    incrFencingToken(): Promise<number>;
    setWaitEdge(ownerId: string, conflictOwner: string, ttlMs: number): Promise<void>;
    clearWaitEdge(ownerId: string): Promise<void>;
    isOwnerAlive(ownerId: string): Promise<boolean>;
    requestRevoke(ownerId: string): Promise<void>;
    /**
     * Open the per-owner event stream for `ownerId`. The returned subscription
     * only ever emits events for that owner (its `revoke`, `kill`, or own
     * `released`). Returns immediately; events arrive via the emitter.
     */
    subscribe(ownerId: string): PathlockdSubscription;
    health(): Promise<HealthResult>;
    close(): void;
}
export interface OwnedPathsResult {
    members: string[];
    alive: boolean;
}
export declare class PathlockdDebugClient {
    private readonly client;
    constructor(opts: PathlockdClientOptions);
    waitForReady(timeoutMs?: number): Promise<void>;
    flush(): Promise<number>;
    expireOwner(ownerId: string): Promise<void>;
    deleteLockKey(path: string, mode: LockMode, ownerId?: string): Promise<void>;
    setWriteOwner(path: string, ownerId: string): Promise<void>;
    getWriteOwner(path: string): Promise<string | null>;
    setFence(path: string, value: number): Promise<void>;
    getFence(path: string): Promise<number | null>;
    setFencingCounter(value: number): Promise<void>;
    getFencingCounter(): Promise<number>;
    ownedPaths(ownerId: string): Promise<OwnedPathsResult>;
    close(): void;
}
export {};
//# sourceMappingURL=client.d.ts.map