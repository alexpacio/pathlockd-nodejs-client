import * as path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

import {
  AcquireStatus,
  AssertStatus,
  CycleKind,
  LockAlgorithm,
  LockEventType,
  LockMode,
  LockState,
  ReasonCode,
  RenewStatus,
} from './types';

// ---------------------------------------------------------------------------
// Wire types
//
// These mirror the protobuf messages as decoded by @grpc/proto-loader with the
// options below: camelCase fields (keepCase:false), int64/uint64 as `string`
// (longs:String), enum values as their proto string names (enums:String), and
// all scalar/repeated fields populated (defaults:true). They are internal to
// the client and are not part of the public API.
//
// Enum fields on responses are typed as `string` rather than the wire unions
// because they cross an untrusted boundary; the *_FROM_WIRE maps decode them
// with a fallback. Outbound enum fields use the wire unions since we produce
// them ourselves.
// ---------------------------------------------------------------------------

export type WireMode = 'MODE_WRITE' | 'MODE_READ';
export type WireLockState = 'LOCK_STATE_NEW' | 'LOCK_STATE_HELD';
export type WireLockAlgorithm =
  | 'LOCK_ALGORITHM_RECURSIVE_RW'
  | 'LOCK_ALGORITHM_POINT_RW'
  | 'LOCK_ALGORITHM_RECURSIVE_WRITE'
  | 'LOCK_ALGORITHM_POINT_WRITE'
  | 'LOCK_ALGORITHM_SEMAPHORE';
export type WireReasonCode =
  | 'REASON_CODE_UNSPECIFIED'
  | 'REASON_CODE_ANCESTOR_LOCKED'
  | 'REASON_CODE_WRITE_LOCKED'
  | 'REASON_CODE_READ_LOCKED'
  | 'REASON_CODE_DESCENDANT_WRITE_LOCKED'
  | 'REASON_CODE_DESCENDANT_READ_LOCKED'
  | 'REASON_CODE_READ_LOCKS_DISABLED'
  | 'REASON_CODE_STALE_FENCING_TOKEN'
  | 'REASON_CODE_INVALID_PERMITS'
  | 'REASON_CODE_SEMAPHORE_FULL'
  | 'REASON_CODE_MISSING_SEMAPHORE'
  | 'REASON_CODE_MISSING_WRITE'
  | 'REASON_CODE_MISSING_READ'
  | 'REASON_CODE_MISSING_FENCE'
  | 'REASON_CODE_MISSING_ALIVE'
  | 'REASON_CODE_MISSING_OWNER_SET'
  | 'REASON_CODE_EMPTY_OWNER_SET'
  | 'REASON_CODE_QUEUED'
  | 'REASON_CODE_STALE_OWNER';

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

// Inspection / dump messages.

export interface WireInspectPathRequest {
  path: string;
}

export interface WireInspectPathResponse {
  writeOwner: string;
  readOwners: string[];
  hasFence: boolean;
  fence: string; // int64 kept as string (longs:String)
  claimOwner: string;
  semaphoreOwners: string[];
}

export interface WireListOwnerLocksRequest {
  ownerId: string;
  domains: string[];
}

export interface WireOwnedLock {
  path: string;
  mode: string; // wire enum name (untrusted; decoded via MODE_FROM_WIRE)
}

export interface WireListOwnerLocksResponse {
  alive: boolean;
  locks: WireOwnedLock[];
}

export interface WireDumpLocksRequest {
  // proto `bytes`: proto-loader accepts a Buffer/Uint8Array on the wire. Empty
  // for the first page; echoed back from the previous response to continue.
  cursor: Buffer | Uint8Array;
  ownerPage: number;
}

export interface WireLockEntry {
  owner: string;
  path: string;
  mode: string;
  hasFence: boolean;
  fence: string; // int64 kept as string (longs:String)
}

export interface WireDumpLocksResponse {
  entries: WireLockEntry[];
  // proto `bytes` is decoded as a Buffer; empty Buffer once `done` is true.
  nextCursor: Buffer;
  done: boolean;
}

// ---------------------------------------------------------------------------
// Typed gRPC clients
//
// @grpc/proto-loader produces these constructors at runtime with no static
// types; the interfaces below describe the subset of the generated surface the
// client uses. A single `as unknown as` cast in loadPathlockdProto() bridges
// the dynamically loaded namespace to these types.
// ---------------------------------------------------------------------------

/** A unary RPC method: callback-style request/response, returns the call handle. */
export type UnaryMethod<Req, Res> = (
  request: Req,
  options: grpc.CallOptions,
  callback: (err: grpc.ServiceError | null, response: Res) => void,
) => grpc.ClientUnaryCall;

/** A client-streaming RPC method: caller writes request chunks, callback receives one response. */
export type ClientStreamingMethod<Req, Res> = (
  options: grpc.CallOptions,
  callback: (err: grpc.ServiceError | null, response: Res) => void,
) => grpc.ClientWritableStream<Req>;

interface GrpcClientBase {
  waitForReady(deadline: grpc.Deadline, callback: (error?: Error) => void): void;
  close(): void;
}

export interface PathLockServiceClient extends GrpcClientBase {
  acquire: UnaryMethod<WireAcquireRequest, WireAcquireResponse>;
  setNamespacePolicy: UnaryMethod<
    WireSetNamespacePolicyRequest,
    WireSetNamespacePolicyResponse
  >;
  getNamespacePolicy: UnaryMethod<WireGetNamespacePolicyRequest, WireGetNamespacePolicyResponse>;
  deleteNamespacePolicy: UnaryMethod<
    WireDeleteNamespacePolicyRequest,
    WireDeleteNamespacePolicyResponse
  >;
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
export type ServiceClientConstructor<C> = new (
  address: string,
  credentials: grpc.ChannelCredentials,
  options?: Record<string, unknown>,
) => C;

/** The `pathlockd.v1` package namespace. */
export interface PathlockdPackage {
  PathLock: ServiceClientConstructor<PathLockServiceClient>;
}

/** Bundled proto, resolved relative to the compiled output (dist/ -> ../proto). */
export const PROTO_PATH = path.join(__dirname, '..', 'proto', 'pathlockd.proto');

let cached: PathlockdPackage | undefined;

/** Load (once) and return the `pathlockd.v1` package namespace. */
export function loadPathlockdProto(): PathlockdPackage {
  if (cached) return cached;
  const def = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false, // camelCase fields: owner_id -> ownerId
    longs: String, // keep int64 exact; client validates before exposing as number
    enums: String, // enum values as their proto names
    defaults: true,
    oneofs: true,
  });
  const pkg = grpc.loadPackageDefinition(def);
  // proto-loader output is untyped; bridge it to PathlockdPackage at this boundary.
  cached = (pkg.pathlockd as grpc.GrpcObject).v1 as unknown as PathlockdPackage;
  return cached;
}

// --- enum <-> wire mappings (enums arrive/depart as their proto string names) ---
//
// Outbound maps are keyed by the public union (exhaustive). Inbound maps are
// keyed by `string`: the wire value is untrusted, so callers fall back to a
// default for any value not listed here.

export const MODE_TO_WIRE: Record<LockMode, WireMode> = {
  write: 'MODE_WRITE',
  read: 'MODE_READ',
};

export const MODE_FROM_WIRE: Record<string, LockMode> = {
  MODE_WRITE: 'write',
  MODE_READ: 'read',
};

export const STATE_TO_WIRE: Record<LockState, WireLockState> = {
  new: 'LOCK_STATE_NEW',
  held: 'LOCK_STATE_HELD',
};

export const LOCK_ALGORITHM_TO_WIRE: Record<LockAlgorithm, WireLockAlgorithm> = {
  recursive_rw: 'LOCK_ALGORITHM_RECURSIVE_RW',
  point_rw: 'LOCK_ALGORITHM_POINT_RW',
  recursive_write: 'LOCK_ALGORITHM_RECURSIVE_WRITE',
  point_write: 'LOCK_ALGORITHM_POINT_WRITE',
  semaphore: 'LOCK_ALGORITHM_SEMAPHORE',
};

export const LOCK_ALGORITHM_FROM_WIRE: Record<string, LockAlgorithm> = {
  LOCK_ALGORITHM_RECURSIVE_RW: 'recursive_rw',
  LOCK_ALGORITHM_POINT_RW: 'point_rw',
  LOCK_ALGORITHM_RECURSIVE_WRITE: 'recursive_write',
  LOCK_ALGORITHM_POINT_WRITE: 'point_write',
  LOCK_ALGORITHM_SEMAPHORE: 'semaphore',
};

export const ACQUIRE_STATUS_FROM_WIRE: Record<string, AcquireStatus> = {
  ACQUIRE_STATUS_OK: 'ok',
  ACQUIRE_STATUS_CONFLICT: 'conflict',
  ACQUIRE_STATUS_LOST: 'lost',
  ACQUIRE_STATUS_QUEUED: 'queued',
};

export const REASON_TO_WIRE: Record<ReasonCode, WireReasonCode> = {
  unspecified: 'REASON_CODE_UNSPECIFIED',
  ancestor_locked: 'REASON_CODE_ANCESTOR_LOCKED',
  write_locked: 'REASON_CODE_WRITE_LOCKED',
  read_locked: 'REASON_CODE_READ_LOCKED',
  descendant_write_locked: 'REASON_CODE_DESCENDANT_WRITE_LOCKED',
  descendant_read_locked: 'REASON_CODE_DESCENDANT_READ_LOCKED',
  read_locks_disabled: 'REASON_CODE_READ_LOCKS_DISABLED',
  stale_fencing_token: 'REASON_CODE_STALE_FENCING_TOKEN',
  invalid_permits: 'REASON_CODE_INVALID_PERMITS',
  semaphore_full: 'REASON_CODE_SEMAPHORE_FULL',
  missing_semaphore: 'REASON_CODE_MISSING_SEMAPHORE',
  missing_write: 'REASON_CODE_MISSING_WRITE',
  missing_read: 'REASON_CODE_MISSING_READ',
  missing_fence: 'REASON_CODE_MISSING_FENCE',
  missing_alive: 'REASON_CODE_MISSING_ALIVE',
  missing_owner_set: 'REASON_CODE_MISSING_OWNER_SET',
  empty_owner_set: 'REASON_CODE_EMPTY_OWNER_SET',
  queued: 'REASON_CODE_QUEUED',
  stale_owner: 'REASON_CODE_STALE_OWNER',
};

export const REASON_FROM_WIRE: Record<string, ReasonCode> = {
  REASON_CODE_UNSPECIFIED: 'unspecified',
  REASON_CODE_ANCESTOR_LOCKED: 'ancestor_locked',
  REASON_CODE_WRITE_LOCKED: 'write_locked',
  REASON_CODE_READ_LOCKED: 'read_locked',
  REASON_CODE_DESCENDANT_WRITE_LOCKED: 'descendant_write_locked',
  REASON_CODE_DESCENDANT_READ_LOCKED: 'descendant_read_locked',
  REASON_CODE_READ_LOCKS_DISABLED: 'read_locks_disabled',
  REASON_CODE_STALE_FENCING_TOKEN: 'stale_fencing_token',
  REASON_CODE_INVALID_PERMITS: 'invalid_permits',
  REASON_CODE_SEMAPHORE_FULL: 'semaphore_full',
  REASON_CODE_MISSING_SEMAPHORE: 'missing_semaphore',
  REASON_CODE_MISSING_WRITE: 'missing_write',
  REASON_CODE_MISSING_READ: 'missing_read',
  REASON_CODE_MISSING_FENCE: 'missing_fence',
  REASON_CODE_MISSING_ALIVE: 'missing_alive',
  REASON_CODE_MISSING_OWNER_SET: 'missing_owner_set',
  REASON_CODE_EMPTY_OWNER_SET: 'empty_owner_set',
  REASON_CODE_QUEUED: 'queued',
  REASON_CODE_STALE_OWNER: 'stale_owner',
};

export const RENEW_STATUS_FROM_WIRE: Record<string, RenewStatus> = {
  RENEW_STATUS_OK: 'ok',
  RENEW_STATUS_LOST: 'lost',
};

export const ASSERT_STATUS_FROM_WIRE: Record<string, AssertStatus> = {
  ASSERT_STATUS_OK: 'ok',
  ASSERT_STATUS_FAIL: 'fail',
};

export const CYCLE_KIND_FROM_WIRE: Record<string, CycleKind> = {
  CYCLE_KIND_NONE: 'none',
  CYCLE_KIND_FOUND: 'cycle',
  CYCLE_KIND_TRUNCATED: 'truncated',
};

export const EVENT_TYPE_FROM_WIRE: Record<string, LockEventType> = {
  EVENT_TYPE_KILLED: 'killed',
  EVENT_TYPE_REVOKE: 'revoke',
  EVENT_TYPE_GRANT: 'grant',
};

export function decodeWireEnum<T extends string>(
  values: Record<string, T>,
  value: unknown,
  fieldName: string,
): T {
  if (typeof value !== 'string') {
    throw new Error(`Unknown ${fieldName} enum value: ${String(value)}`);
  }

  const decoded = values[value];
  if (decoded === undefined) {
    throw new Error(`Unknown ${fieldName} enum value: ${JSON.stringify(value)}`);
  }

  return decoded;
}

export function toWireUint64(value: number, fieldName: string): string {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${fieldName} must be a non-negative safe integer`);
  }
  return String(value);
}

export function toWirePositiveUint64(value: number, fieldName: string): string {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${fieldName} must be a positive safe integer`);
  }
  return String(value);
}

const UINT32_MAX = 4294967295;

export function toWireUint32(value: number, fieldName: string): number {
  if (!Number.isSafeInteger(value) || value < 0 || value > UINT32_MAX) {
    throw new Error(`${fieldName} must be a uint32`);
  }
  return value;
}

const INT64_MAX = 9223372036854775807n;
const INT64_MIN = -9223372036854775808n;

/**
 * Decode a wire int64 (kept as a `string` by the proto loader, see `longs: String`)
 * into a `bigint`. Fence values can exceed `Number.MAX_SAFE_INTEGER`, so they
 * must not pass through `Number`.
 */
export function wireInt64ToBigInt(value: unknown, fieldName: string): bigint {
  try {
    if (typeof value === 'bigint') return value;
    if (typeof value === 'number' && Number.isInteger(value)) return BigInt(value);
    if (typeof value === 'string' && /^-?\d+$/.test(value)) return BigInt(value);
  } catch {
    // fall through to the thrown error below
  }
  throw new Error(`${fieldName} is not a valid int64: ${String(value)}`);
}

/** Encode a `bigint` as a wire int64 string, validating the full int64 range. */
export function bigintToWireInt64(value: bigint, fieldName: string): string {
  if (typeof value !== 'bigint' || value < INT64_MIN || value > INT64_MAX) {
    throw new Error(`${fieldName} must be an int64`);
  }
  return value.toString();
}

export function buildCredentials(tls: boolean): grpc.ChannelCredentials {
  return tls ? grpc.credentials.createSsl() : grpc.credentials.createInsecure();
}

/** Fully-qualified gRPC service name, used to scope the default retry policy. */
const PATHLOCK_SERVICE = 'pathlockd.v1.PathLock';

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
export const DEFAULT_CHANNEL_OPTIONS: Readonly<Record<string, unknown>> = Object.freeze({
  'grpc.keepalive_time_ms': 30_000,
  'grpc.keepalive_timeout_ms': 10_000,
  'grpc.keepalive_permit_without_calls': 0,
  'grpc.enable_retries': 1,
  'grpc.service_config': JSON.stringify({
    methodConfig: [
      {
        name: [{ service: PATHLOCK_SERVICE }],
        retryPolicy: {
          maxAttempts: 5,
          initialBackoff: '0.1s',
          maxBackoff: '2s',
          backoffMultiplier: 2,
          retryableStatusCodes: ['UNAVAILABLE'],
        },
      },
    ],
  }),
  'grpc.max_receive_message_length': 64 * 1024 * 1024,
});

/** Merge caller channel options over {@link DEFAULT_CHANNEL_OPTIONS} (caller wins). */
export function buildChannelOptions(overrides?: Record<string, unknown>): Record<string, unknown> {
  return { ...DEFAULT_CHANNEL_OPTIONS, ...(overrides ?? {}) };
}
