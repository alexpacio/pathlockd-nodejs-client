/** Lock mode. Treated as `write` wherever it is left optional. */
export type LockMode = 'write' | 'read';
/** Whether a request is a new acquisition or a re-validation of a held path. */
export type LockState = 'new' | 'held';
export type AcquireStatus = 'ok' | 'conflict' | 'lost' | 'queued';
export type RenewStatus = 'ok' | 'lost';
export type AssertStatus = 'ok' | 'fail';
export type CycleKind = 'none' | 'cycle' | 'truncated';
export type LockEventType = 'killed' | 'revoke' | 'grant';
export type LockAlgorithm = 'recursive_rw' | 'point_rw' | 'recursive_write' | 'point_write' | 'semaphore';
export type ReasonCode = 'unspecified' | 'ancestor_locked' | 'write_locked' | 'read_locked' | 'descendant_write_locked' | 'descendant_read_locked' | 'read_locks_disabled' | 'stale_fencing_token' | 'invalid_permits' | 'semaphore_full' | 'missing_semaphore' | 'missing_write' | 'missing_read' | 'missing_fence' | 'missing_alive' | 'missing_owner_set' | 'empty_owner_set' | 'queued' | 'stale_owner';
/**
 * Per-call transport controls accepted by every RPC. Both are optional; when
 * omitted the call uses the client-wide {@link PathlockdClientOptions.defaultCallTimeoutMs}
 * (if any) and is not tied to any abort signal.
 */
export interface CallOptions {
    /**
     * Deadline for this single call, in milliseconds from now. Bounds a hung
     * daemon round-trip so it cannot block the caller indefinitely. Overrides the
     * client-wide default for this call. The deadline spans any automatic
     * transport retries, not each attempt.
     */
    deadlineMs?: number;
    /**
     * Abort signal that cancels the in-flight RPC. Lets a caller reclaim a call
     * (and any resource it is holding, e.g. a serialized lock-update slot) the
     * instant its surrounding operation is aborted, instead of waiting for the
     * deadline.
     */
    signal?: AbortSignal;
}
/** Options shared by mutating RPCs that support apply-once retry keys. */
export interface IdempotentRequestOptions extends CallOptions {
    /** Optional apply-once key for safely retrying the same logical request. */
    idempotencyKey?: string;
}
export interface ReleaseOptions extends IdempotentRequestOptions {
    /** Fold the owner's wait-edge deletion into the same transaction. */
    delWaitKey?: boolean;
}
export interface ReleaseAllOptions extends ReleaseOptions {
    /**
     * Routing namespaces in which the owner holds locks (same form as
     * {@link RenewOptions.domains}). When set, the release targets only those
     * Raft groups instead of every lock group; locks in unlisted groups are left
     * to expire on their TTL. Omit for an unconditional "release everything".
     */
    domains?: string[];
}
/** Per-call options for the owner-scoped read RPCs (isOwnerAlive, listOwnerLocks). */
export interface OwnerReadOptions extends CallOptions {
    /**
     * Routing namespaces to query (same form as {@link RenewOptions.domains}).
     * When set, only those Raft groups are probed — an under-declared set can
     * report a false negative / omit locks, so leave it empty when an
     * authoritative cluster-wide answer is required.
     */
    domains?: string[];
}
export interface RenewOptions extends IdempotentRequestOptions {
    /**
     * Routing namespaces in which the owner holds locks. Supplying these lets the
     * daemon target renew fan-out instead of probing every Raft group.
     */
    domains?: string[];
}
/**
 * Fencing tokens are monotonic int64 values. They can exceed
 * `Number.MAX_SAFE_INTEGER`, so they are represented as `bigint` values.
 */
export type FencingToken = bigint;
/** A lock path is "<handlerType>:<normalizedPath>", e.g. "google_drive:/a/b". */
export interface LockRequest {
    path: string;
    mode?: LockMode;
    state?: LockState;
    /**
     * Required > 0 for semaphore write acquires. The first acquire for a semaphore
     * path establishes that path's capacity; later acquires must use the same value.
     */
    permits?: number;
}
export interface ReleaseRequest {
    path: string;
    mode?: LockMode;
}
export interface AcquireParams extends CallOptions {
    ownerId: string;
    ttlMs: number;
    requests: LockRequest[];
    /**
     * Optional caller-supplied fence for write acquires. Omit or pass `0n` to let
     * the daemon mint the next token. Reads and semaphore locks ignore it.
     */
    fencingToken?: FencingToken;
    /** Releases folded into the same atomic transaction (shadowing transitions). */
    releaseRequests?: ReleaseRequest[];
    /**
     * If this acquire is queued (contended), how long its wait-queue entry lives
     * without being granted — the caller's own acquire deadline. `0`/omitted
     * selects a server default. Lets an abandoned waiter self-evict at the
     * caller's threshold instead of a fixed server TTL.
     */
    queueTtlMs?: number;
    /** Optional apply-once key for safely retrying the same logical request. */
    idempotencyKey?: string;
}
export interface AcquireResult {
    status: AcquireStatus;
    /** CONFLICT: the conflicting path. LOST: the path whose key/fence vanished. */
    path: string;
    /** CONFLICT/QUEUED: the conflicting owner. Empty for LOST/OK. */
    owner: string;
    /** CONFLICT/LOST/QUEUED reason. */
    reason: ReasonCode;
    /** OK/QUEUED: the server-minted or caller-supplied fencing token, when any. */
    fencingToken: FencingToken | null;
    /** STALE_FENCING_TOKEN: the current persisted token for the path. */
    currentFencingToken: FencingToken | null;
    /**
     * OK: the routing namespace this acquire resolved to. Every path in one
     * acquire shares a single routing namespace, so this one value covers them
     * all. Pass the distinct set of these (across your held paths) as
     * {@link RenewOptions.domains} so renew targets only the groups where the
     * owner holds locks — no client-side routing logic, and explicit namespaces
     * are honored because the daemon resolved it. Empty string for non-OK results.
     */
    namespace: string;
}
export interface RenewResult {
    status: RenewStatus;
    path: string;
    reason: ReasonCode;
    /**
     * OK only: a cooperative revoke is pending for this owner (set by
     * {@link PathlockdClient.requestRevoke}). The holder should finish its current
     * unit of work and release. Rides the renew heartbeat, so a poll-only client
     * that holds no event subscription still learns it has been asked to yield.
     * Always `false` for a LOST renew.
     */
    revokeRequested: boolean;
}
export interface AssertResult {
    status: AssertStatus;
    path: string;
    reason: ReasonCode;
}
export interface CycleResult {
    kind: CycleKind;
    chain: string[];
}
export interface LockEvent {
    type: LockEventType;
    ownerId: string;
}
export interface SetWaitEdgeMetadata {
    /** The path returned by an Acquire conflict. */
    conflictPath: string;
    /** The reason returned by an Acquire conflict. */
    reason: ReasonCode;
}
export interface HealthResult {
    ok: boolean;
    detail: string;
}
export interface NamespacePolicyResult {
    algorithm: LockAlgorithm;
    /** False means the daemon is using the default recursive_rw policy. */
    explicit: boolean;
}
/**
 * A read-only snapshot of the lock state at one exact path
 * ({@link PathlockdClient.inspectPath}). Filtered by owner liveness, so it
 * reflects what would actually block; it never mutates daemon state.
 */
export interface PathLockInfo {
    /** Live write owner of this exact path, or `null` if none holds it. */
    writeOwner: string | null;
    /** Live read owners of this exact path. Reads are point-only, so ancestors and descendants are excluded. */
    readOwners: string[];
    /**
     * Current fencing token recorded for this path, or `null` if none. The fence
     * can outlive the lock, so this may be set even when `writeOwner` is `null`.
     */
    fence: FencingToken | null;
    /** Live semaphore owners of this exact path. Empty for non-semaphore paths. */
    semaphoreOwners: string[];
}
/** One lock held by an owner ({@link PathlockdClient.listOwnerLocks}). */
export interface OwnedLockInfo {
    path: string;
    mode: LockMode;
}
/** Result of {@link PathlockdClient.listOwnerLocks}. */
export interface OwnerLocksResult {
    /** Whether the owner's liveness lease is currently present. */
    alive: boolean;
    locks: OwnedLockInfo[];
}
/** One lock in a cluster-wide dump ({@link PathlockdClient.dumpLocks}). */
export interface LockEntry {
    owner: string;
    path: string;
    mode: LockMode;
    /** Fencing token for write locks; `null` for reads. */
    fence: FencingToken | null;
}
export interface PathlockdClientOptions {
    /** e.g. "localhost:50051". */
    endpoint: string;
    /**
     * Extra @grpc/grpc-js channel options. Shallow-merged over the client's
     * reliability defaults (keepalive, automatic UNAVAILABLE retry, a larger
     * receive limit), so any key set here wins. See `DEFAULT_CHANNEL_OPTIONS`.
     */
    channelOptions?: Record<string, unknown>;
    /** Use a TLS channel (defaults to insecure). */
    tls?: boolean;
    /**
     * Default deadline applied to every unary call, in milliseconds, unless the
     * call supplies its own {@link CallOptions.deadlineMs}. Leave unset for no
     * default deadline (a call then waits indefinitely for the daemon). Recommended
     * in production so a hung daemon round-trip cannot wedge a caller.
     */
    defaultCallTimeoutMs?: number;
}
//# sourceMappingURL=types.d.ts.map