import * as path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

import {
  AcquireStatus,
  AssertStatus,
  CycleKind,
  LockEventType,
  LockMode,
  LockState,
  RenewStatus,
  SetClaimStatus,
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

export interface WireLockRequest {
  path: string;
  mode: WireMode;
  state: WireLockState;
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
  emitRelease: boolean;
  idempotencyKey?: string;
}

export interface WireAcquireResponse {
  status: string;
  path: string;
  owner: string;
  reason: string;
}

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
  reason: string;
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
  reason?: string;
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
}

export interface WireIsOwnerAliveResponse {
  alive: boolean;
}

export interface WireSetClaimRequest {
  path: string;
  claimantOwnerId: string;
  // uint64 encoded as string (longs:String); "0" selects the daemon default.
  ttlMs: string;
  idempotencyKey?: string;
}

export interface WireSetClaimResponse {
  status: string;
  claimOwner: string;
}

export interface WireClearClaimRequest {
  path: string;
  claimantOwnerId: string;
  idempotencyKey?: string;
}

export type WireClearClaimResponse = Record<string, never>;

export interface WireRequestRevokeRequest {
  ownerId: string;
  // Optional preemption claim: reserve claimPath for claimantOwnerId until the
  // winner acquires it, so the revoked victim can't re-grab it first. uint64 is
  // decoded/encoded as string by proto-loader (longs:String).
  claimPath?: string;
  claimantOwnerId?: string;
  claimTtlMs?: string;
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
}

export interface WireListOwnerLocksRequest {
  ownerId: string;
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
  callback: (err: grpc.ServiceError | null, response: Res) => void,
) => grpc.ClientUnaryCall;

/** A client-streaming RPC method: caller writes request chunks, callback receives one response. */
export type ClientStreamingMethod<Req, Res> = (
  callback: (err: grpc.ServiceError | null, response: Res) => void,
) => grpc.ClientWritableStream<Req>;

interface GrpcClientBase {
  waitForReady(deadline: grpc.Deadline, callback: (error?: Error) => void): void;
  close(): void;
}

export interface PathLockServiceClient extends GrpcClientBase {
  acquire: UnaryMethod<WireAcquireRequest, WireAcquireResponse>;
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
  setClaim: UnaryMethod<WireSetClaimRequest, WireSetClaimResponse>;
  clearClaim: UnaryMethod<WireClearClaimRequest, WireClearClaimResponse>;
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

export const ACQUIRE_STATUS_FROM_WIRE: Record<string, AcquireStatus> = {
  ACQUIRE_STATUS_OK: 'ok',
  ACQUIRE_STATUS_CONFLICT: 'conflict',
  ACQUIRE_STATUS_LOST: 'lost',
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
  EVENT_TYPE_RELEASED: 'released',
  EVENT_TYPE_KILLED: 'killed',
  EVENT_TYPE_REVOKE: 'revoke',
};

export const SET_CLAIM_STATUS_FROM_WIRE: Record<string, SetClaimStatus> = {
  SET_CLAIM_STATUS_OK: 'ok',
  SET_CLAIM_STATUS_HELD: 'held',
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

const INT64_MAX = 9223372036854775807n;
const INT64_MIN = -9223372036854775808n;

/**
 * Decode a wire int64 (kept as a `string` by the proto loader, see `longs: String`)
 * into a `bigint`. Fence values and fencing tokens are PD TSO timestamps that
 * routinely exceed `Number.MAX_SAFE_INTEGER`, so they must not pass through `Number`.
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
