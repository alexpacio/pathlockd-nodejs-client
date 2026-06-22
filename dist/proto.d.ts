import * as grpc from '@grpc/grpc-js';
import { AcquireStatus, AssertStatus, CycleKind, LockAlgorithm, LockEventType, LockMode, LockState, ReasonCode, RenewStatus } from './types';
export type WireMode = 'MODE_WRITE' | 'MODE_READ';
export type WireLockState = 'LOCK_STATE_NEW' | 'LOCK_STATE_HELD';
export type WireLockAlgorithm = 'LOCK_ALGORITHM_RECURSIVE_RW' | 'LOCK_ALGORITHM_POINT_RW' | 'LOCK_ALGORITHM_RECURSIVE_WRITE' | 'LOCK_ALGORITHM_POINT_WRITE' | 'LOCK_ALGORITHM_SEMAPHORE';
export type WireReasonCode = 'REASON_CODE_UNSPECIFIED' | 'REASON_CODE_ANCESTOR_LOCKED' | 'REASON_CODE_WRITE_LOCKED' | 'REASON_CODE_READ_LOCKED' | 'REASON_CODE_DESCENDANT_WRITE_LOCKED' | 'REASON_CODE_DESCENDANT_READ_LOCKED' | 'REASON_CODE_READ_LOCKS_DISABLED' | 'REASON_CODE_STALE_FENCING_TOKEN' | 'REASON_CODE_INVALID_PERMITS' | 'REASON_CODE_SEMAPHORE_FULL' | 'REASON_CODE_MISSING_SEMAPHORE' | 'REASON_CODE_MISSING_WRITE' | 'REASON_CODE_MISSING_READ' | 'REASON_CODE_MISSING_FENCE' | 'REASON_CODE_MISSING_ALIVE' | 'REASON_CODE_MISSING_OWNER_SET' | 'REASON_CODE_EMPTY_OWNER_SET' | 'REASON_CODE_QUEUED' | 'REASON_CODE_STALE_OWNER';
export interface WireLockRequest {
    path: string;
    mode: WireMode;
    state: WireLockState;
    permits: number;
}
export interface WireReleaseRequest {
    path: string;
    mode: WireMode;
}
export interface WireAcquireRequest {
    ownerId: string;
    ttlMs: number | string;
    requests: WireLockRequest[];
    fencingToken: number | string;
    releaseRequests: WireReleaseRequest[];
    queueTtlMs: number | string;
    idempotencyKey?: string;
}
export interface WireAcquireResponse {
    status: string;
    path: string;
    owner: string;
    reason: string;
    fencingToken: string;
    currentFencingToken: string;
    namespace: string;
}
export interface WireSetNamespacePolicyRequest {
    namespace: string;
    algorithm: WireLockAlgorithm;
    idempotencyKey?: string;
}
export type WireSetNamespacePolicyResponse = Record<string, never>;
export interface WireGetNamespacePolicyRequest {
    namespace: string;
}
export interface WireGetNamespacePolicyResponse {
    algorithm: string;
    explicit: boolean;
}
export interface WireDeleteNamespacePolicyRequest {
    namespace: string;
    idempotencyKey?: string;
}
export type WireDeleteNamespacePolicyResponse = Record<string, never>;
export interface WireReleaseLocksRequest {
    ownerId: string;
    requests: WireReleaseRequest[];
    delWaitKey: boolean;
    idempotencyKey?: string;
}
export interface WireReleaseAllRequest {
    ownerId: string;
    delWaitKey: boolean;
    idempotencyKey?: string;
    domains: string[];
}
export type WireReleaseResponse = Record<string, never>;
export interface WireRenewRequest {
    ownerId: string;
    ttlMs: number | string;
    domains: string[];
    idempotencyKey?: string;
}
export interface WireRenewResponse {
    status: string;
    path: string;
    reason: string;
    revokeRequested: boolean;
}
export interface WireForceReleaseRequest {
    victimId: string;
    idempotencyKey?: string;
}
export type WireForceReleaseResponse = Record<string, never>;
export interface WireAssertFencingRequest {
    ownerId: string;
    fencingToken: number | string;
    paths: string[];
}
export interface WireAssertFencingResponse {
    status: string;
    path: string;
    reason: string;
}
export interface WireDetectCycleRequest {
    startOwnerId: string;
    maxDepth: number;
}
export interface WireDetectCycleResponse {
    kind: string;
    chain: string[];
}
export interface WireIsBlockingRequest {
    conflictPath: string;
    conflictOwner: string;
    reason: WireReasonCode;
}
export interface WireIsBlockingResponse {
    blocking: boolean;
}
export interface WireIncrFencingTokenRequest {
    idempotencyKey?: string;
}
export interface WireIncrFencingTokenResponse {
    token: string;
}
export interface WireSetWaitEdgeRequest {
    ownerId: string;
    conflictOwner: string;
    ttlMs: number | string;
    conflictPath?: string;
    reason?: WireReasonCode;
    idempotencyKey?: string;
}
export type WireSetWaitEdgeResponse = Record<string, never>;
export interface WireClearWaitEdgeRequest {
    ownerId: string;
    idempotencyKey?: string;
}
export type WireClearWaitEdgeResponse = Record<string, never>;
export interface WireIsOwnerAliveRequest {
    ownerId: string;
    domains: string[];
}
export interface WireIsOwnerAliveResponse {
    alive: boolean;
}
export interface WireRequestRevokeRequest {
    ownerId: string;
}
export type WireRequestRevokeResponse = Record<string, never>;
export interface WireSubscribeRequest {
    ownerId: string;
}
export interface WireEvent {
    type: string;
    ownerId: string;
}
export type WireHealthRequest = Record<string, never>;
export interface WireHealthResponse {
    ok: boolean;
    detail: string;
}
export interface WireInspectPathRequest {
    path: string;
}
export interface WireInspectPathResponse {
    writeOwner: string;
    readOwners: string[];
    hasFence: boolean;
    fence: string;
    claimOwner: string;
    semaphoreOwners: string[];
}
export interface WireListOwnerLocksRequest {
    ownerId: string;
    domains: string[];
}
export interface WireOwnedLock {
    path: string;
    mode: string;
}
export interface WireListOwnerLocksResponse {
    alive: boolean;
    locks: WireOwnedLock[];
}
export interface WireDumpLocksRequest {
    cursor: Buffer | Uint8Array;
    ownerPage: number;
}
export interface WireLockEntry {
    owner: string;
    path: string;
    mode: string;
    hasFence: boolean;
    fence: string;
}
export interface WireDumpLocksResponse {
    entries: WireLockEntry[];
    nextCursor: Buffer;
    done: boolean;
}
/** A unary RPC method: callback-style request/response, returns the call handle. */
export type UnaryMethod<Req, Res> = (request: Req, options: grpc.CallOptions, callback: (err: grpc.ServiceError | null, response: Res) => void) => grpc.ClientUnaryCall;
/** A client-streaming RPC method: caller writes request chunks, callback receives one response. */
export type ClientStreamingMethod<Req, Res> = (options: grpc.CallOptions, callback: (err: grpc.ServiceError | null, response: Res) => void) => grpc.ClientWritableStream<Req>;
interface GrpcClientBase {
    waitForReady(deadline: grpc.Deadline, callback: (error?: Error) => void): void;
    close(): void;
}
export interface PathLockServiceClient extends GrpcClientBase {
    acquire: UnaryMethod<WireAcquireRequest, WireAcquireResponse>;
    setNamespacePolicy: UnaryMethod<WireSetNamespacePolicyRequest, WireSetNamespacePolicyResponse>;
    getNamespacePolicy: UnaryMethod<WireGetNamespacePolicyRequest, WireGetNamespacePolicyResponse>;
    deleteNamespacePolicy: UnaryMethod<WireDeleteNamespacePolicyRequest, WireDeleteNamespacePolicyResponse>;
    acquireStream: ClientStreamingMethod<WireAcquireRequest, WireAcquireResponse>;
    release: UnaryMethod<WireReleaseLocksRequest, WireReleaseResponse>;
    releaseAll: UnaryMethod<WireReleaseAllRequest, WireReleaseResponse>;
    renew: UnaryMethod<WireRenewRequest, WireRenewResponse>;
    forceRelease: UnaryMethod<WireForceReleaseRequest, WireForceReleaseResponse>;
    assertFencing: UnaryMethod<WireAssertFencingRequest, WireAssertFencingResponse>;
    detectCycle: UnaryMethod<WireDetectCycleRequest, WireDetectCycleResponse>;
    isBlocking: UnaryMethod<WireIsBlockingRequest, WireIsBlockingResponse>;
    incrFencingToken: UnaryMethod<WireIncrFencingTokenRequest, WireIncrFencingTokenResponse>;
    setWaitEdge: UnaryMethod<WireSetWaitEdgeRequest, WireSetWaitEdgeResponse>;
    clearWaitEdge: UnaryMethod<WireClearWaitEdgeRequest, WireClearWaitEdgeResponse>;
    isOwnerAlive: UnaryMethod<WireIsOwnerAliveRequest, WireIsOwnerAliveResponse>;
    requestRevoke: UnaryMethod<WireRequestRevokeRequest, WireRequestRevokeResponse>;
    inspectPath: UnaryMethod<WireInspectPathRequest, WireInspectPathResponse>;
    listOwnerLocks: UnaryMethod<WireListOwnerLocksRequest, WireListOwnerLocksResponse>;
    dumpLocks: UnaryMethod<WireDumpLocksRequest, WireDumpLocksResponse>;
    subscribe(request: WireSubscribeRequest): grpc.ClientReadableStream<WireEvent>;
    health: UnaryMethod<WireHealthRequest, WireHealthResponse>;
}
/** Constructor shape shared by proto-loader's generated service clients. */
export type ServiceClientConstructor<C> = new (address: string, credentials: grpc.ChannelCredentials, options?: Record<string, unknown>) => C;
/** The `pathlockd.v1` package namespace. */
export interface PathlockdPackage {
    PathLock: ServiceClientConstructor<PathLockServiceClient>;
}
/** Bundled proto, resolved relative to the compiled output (dist/ -> ../proto). */
export declare const PROTO_PATH: string;
/** Load (once) and return the `pathlockd.v1` package namespace. */
export declare function loadPathlockdProto(): PathlockdPackage;
export declare const MODE_TO_WIRE: Record<LockMode, WireMode>;
export declare const MODE_FROM_WIRE: Record<string, LockMode>;
export declare const STATE_TO_WIRE: Record<LockState, WireLockState>;
export declare const LOCK_ALGORITHM_TO_WIRE: Record<LockAlgorithm, WireLockAlgorithm>;
export declare const LOCK_ALGORITHM_FROM_WIRE: Record<string, LockAlgorithm>;
export declare const ACQUIRE_STATUS_FROM_WIRE: Record<string, AcquireStatus>;
export declare const REASON_TO_WIRE: Record<ReasonCode, WireReasonCode>;
export declare const REASON_FROM_WIRE: Record<string, ReasonCode>;
export declare const RENEW_STATUS_FROM_WIRE: Record<string, RenewStatus>;
export declare const ASSERT_STATUS_FROM_WIRE: Record<string, AssertStatus>;
export declare const CYCLE_KIND_FROM_WIRE: Record<string, CycleKind>;
export declare const EVENT_TYPE_FROM_WIRE: Record<string, LockEventType>;
export declare function decodeWireEnum<T extends string>(values: Record<string, T>, value: unknown, fieldName: string): T;
export declare function toWireUint64(value: number, fieldName: string): string;
export declare function toWirePositiveUint64(value: number, fieldName: string): string;
export declare function toWireUint32(value: number, fieldName: string): number;
/**
 * Decode a wire int64 (kept as a `string` by the proto loader, see `longs: String`)
 * into a `bigint`. Fence values can exceed `Number.MAX_SAFE_INTEGER`, so they
 * must not pass through `Number`.
 */
export declare function wireInt64ToBigInt(value: unknown, fieldName: string): bigint;
/** Encode a `bigint` as a wire int64 string, validating the full int64 range. */
export declare function bigintToWireInt64(value: bigint, fieldName: string): string;
export declare function buildCredentials(tls: boolean): grpc.ChannelCredentials;
/**
 * Reliability defaults applied to every channel, shallow-merged under any
 * caller-supplied `channelOptions` (caller keys win). Centralizing them here
 * means every consumer talks to the daemon the same robust way without having
 * to rediscover gRPC tuning.
 *
 * - **Keepalive.** Detect a half-open connection on a long-lived stream (the
 *   per-owner event subscription) promptly instead of waiting for the OS TCP
 *   timeout. The 30s ping interval sits above the daemon's own 20s server
 *   keepalive so the two don't fight; `permit_without_calls` stays off so an
 *   idle, call-less channel never risks a server ping-strike disconnect (the
 *   subscription is an active RPC, so its channel is still kept warm).
 * - **Automatic retry.** Transparently retry a call that fails because the
 *   daemon was momentarily `UNAVAILABLE` — a Raft leader election/failover, a
 *   load-shed, or a brief network blip. Every mutating RPC carries an
 *   idempotency key and every read is naturally idempotent, so a retry can
 *   never double-apply. The retry budget is bounded and backs off; the call
 *   deadline (if any) spans all attempts.
 * - **Receive limit.** `dumpLocks` pages can exceed the 4 MiB gRPC default on
 *   large clusters.
 */
export declare const DEFAULT_CHANNEL_OPTIONS: Readonly<Record<string, unknown>>;
/** Merge caller channel options over {@link DEFAULT_CHANNEL_OPTIONS} (caller wins). */
export declare function buildChannelOptions(overrides?: Record<string, unknown>): Record<string, unknown>;
export {};
//# sourceMappingURL=proto.d.ts.map