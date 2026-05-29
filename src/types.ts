// Public, ergonomic types for the pathlockd client. These are friendlier than
// the raw protobuf shapes (string unions instead of SCREAMING_SNAKE enum names,
// camelCase fields); the client maps to/from the wire form internally.

/** Lock mode. Treated as `write` wherever it is left optional. */
export type LockMode = 'write' | 'read';

/** Whether a request is a new acquisition or a re-validation of a held path. */
export type LockState = 'new' | 'held';

export type AcquireStatus = 'ok' | 'conflict' | 'lost';
export type RenewStatus = 'ok' | 'lost';
export type AssertStatus = 'ok' | 'fail';
export type CycleKind = 'none' | 'cycle' | 'truncated';
export type LockEventType = 'released' | 'killed' | 'revoke';

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
  fencingToken: number;
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

export interface HealthResult {
  ok: boolean;
  detail: string;
}

export interface PathlockdClientOptions {
  /** e.g. "localhost:50051". */
  endpoint: string;
  /** Extra @grpc/grpc-js channel options. */
  channelOptions?: Record<string, unknown>;
  /** Use a TLS channel (defaults to insecure). */
  tls?: boolean;
}
