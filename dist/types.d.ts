/** Lock mode. Treated as `write` wherever it is left optional. */
export type LockMode = 'write' | 'read';
/** Whether a request is a new acquisition or a re-validation of a held path. */
export type LockState = 'new' | 'held';
export type AcquireStatus = 'ok' | 'conflict' | 'lost';
export type RenewStatus = 'ok' | 'lost';
export type AssertStatus = 'ok' | 'fail';
export type CycleKind = 'none' | 'cycle' | 'truncated';
export type LockEventType = 'released' | 'killed' | 'revoke';
/**
 * A fencing token is the PD TSO version returned by {@link PathlockdClient.incrFencingToken}.
 * It is a packed i64 timestamp (`(physical_ms << 18) | logical`) that routinely exceeds
 * `Number.MAX_SAFE_INTEGER`, so it is represented as a `bigint` to stay exact and ordered.
 */
export type FencingToken = bigint;
/** A lock path is "<handlerType>:<normalizedPath>", e.g. "google_drive:/a/b". */
export interface LockRequest {
    path: string;
    mode?: LockMode;
    state?: LockState;
}
export interface ReleaseRequest {
    path: string;
    mode?: LockMode;
}
export interface AcquireParams {
    ownerId: string;
    ttlMs: number;
    requests: LockRequest[];
    fencingToken: FencingToken;
    /** Releases folded into the same atomic transaction (shadowing transitions). */
    releaseRequests?: ReleaseRequest[];
    /** Publish a RELEASED event for ownerId if an inline release was applied. */
    emitRelease?: boolean;
}
export interface AcquireResult {
    status: AcquireStatus;
    /** CONFLICT: the conflicting path. LOST: the path whose key/fence vanished. */
    path: string;
    /** CONFLICT: the conflicting owner (or the persisted fence value for stale tokens). */
    owner: string;
    /**
     * CONFLICT/LOST reason: ancestor_locked | write_locked | read_locked |
     * descendant_write_locked | descendant_read_locked | stale_fencing_token |
     * missing_write | missing_read | missing_fence | missing_alive.
     */
    reason: string;
}
export interface RenewResult {
    status: RenewStatus;
    path: string;
    reason: string;
}
export interface AssertResult {
    status: AssertStatus;
    path: string;
    /** stale_owner | stale_fencing_token */
    reason: string;
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
    reason: string;
}
export interface HealthResult {
    ok: boolean;
    detail: string;
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
    /** Live preemption claimant reserving this path for an in-flight revoke, or `null`. */
    claimOwner: string | null;
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
/**
 * An optional preemption claim passed to {@link PathlockdClient.requestRevoke}.
 * The daemon reserves `path` for `claimantOwnerId` until that owner acquires
 * it, so the revoked victim cannot re-grab the path first.
 */
export interface PreemptionClaim {
    /** The contended path to reserve (daemon `handler:path` form). */
    path: string;
    /** The owner the path is reserved for — typically the revoker itself. */
    claimantOwnerId: string;
    /** Reservation lifetime in ms; `0`/omitted lets the daemon pick a default. */
    ttlMs?: number;
}
export interface PathlockdClientOptions {
    /** e.g. "localhost:50051". */
    endpoint: string;
    /** Extra @grpc/grpc-js channel options. */
    channelOptions?: Record<string, unknown>;
    /** Use a TLS channel (defaults to insecure). */
    tls?: boolean;
}
//# sourceMappingURL=types.d.ts.map