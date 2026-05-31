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
} from './types';

// ---------------------------------------------------------------------------
// Wire types
//
// These mirror the protobuf messages as decoded by @grpc/proto-loader with the
// options below: camelCase fields (keepCase:false), int64/uint64 as `number`
// (longs:Number), enum values as their proto string names (enums:String), and
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
  ttlMs: number;
  requests: WireLockRequest[];
  fencingToken: number;
  releaseRequests: WireReleaseRequest[];
  emitRelease: boolean;
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
}

export interface WireReleaseAllRequest {
  ownerId: string;
  delWaitKey: boolean;
}

export type WireReleaseResponse = Record<string, never>;

export interface WireRenewRequest {
  ownerId: string;
  ttlMs: number;
}

export interface WireRenewResponse {
  status: string;
  path: string;
  reason: string;
}

export interface WireForceReleaseRequest {
  victimId: string;
}

export type WireForceReleaseResponse = Record<string, never>;

export interface WireAssertFencingRequest {
  ownerId: string;
  fencingToken: number;
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

export type WireIncrFencingTokenRequest = Record<string, never>;

export interface WireIncrFencingTokenResponse {
  token: number;
}

export interface WireSetWaitEdgeRequest {
  ownerId: string;
  conflictOwner: string;
  ttlMs: number;
}

export type WireSetWaitEdgeResponse = Record<string, never>;

export interface WireClearWaitEdgeRequest {
  ownerId: string;
}

export type WireClearWaitEdgeResponse = Record<string, never>;

export interface WireIsOwnerAliveRequest {
  ownerId: string;
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

// Debug service messages.

export type WireFlushRequest = Record<string, never>;

export interface WireFlushResponse {
  deleted: number;
}

export interface WireExpireOwnerRequest {
  ownerId: string;
}

export interface WireDeleteLockKeyRequest {
  path: string;
  mode: WireMode;
  ownerId: string;
}

export interface WireSetWriteOwnerRequest {
  path: string;
  ownerId: string;
}

export interface WireGetWriteOwnerRequest {
  path: string;
}

export interface WireGetWriteOwnerResponse {
  exists: boolean;
  ownerId: string;
}

export interface WireSetFenceRequest {
  path: string;
  value: number;
}

export interface WireGetFenceRequest {
  path: string;
}

export interface WireGetFenceResponse {
  exists: boolean;
  value: number;
}

export interface WireSetFencingCounterRequest {
  value: number;
}

export type WireGetFencingCounterRequest = Record<string, never>;

export interface WireGetFencingCounterResponse {
  value: number;
}

export interface WireOwnedPathsRequest {
  ownerId: string;
}

export interface WireOwnedPathsResponse {
  members: string[];
  alive: boolean;
}

export type WireDebugAck = Record<string, never>;

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

interface GrpcClientBase {
  waitForReady(deadline: grpc.Deadline, callback: (error?: Error) => void): void;
  close(): void;
}

export interface PathLockServiceClient extends GrpcClientBase {
  acquire: UnaryMethod<WireAcquireRequest, WireAcquireResponse>;
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
  subscribe(request: WireSubscribeRequest): grpc.ClientReadableStream<WireEvent>;
  health: UnaryMethod<WireHealthRequest, WireHealthResponse>;
}

export interface PathLockDebugServiceClient extends GrpcClientBase {
  flush: UnaryMethod<WireFlushRequest, WireFlushResponse>;
  expireOwner: UnaryMethod<WireExpireOwnerRequest, WireDebugAck>;
  deleteLockKey: UnaryMethod<WireDeleteLockKeyRequest, WireDebugAck>;
  setWriteOwner: UnaryMethod<WireSetWriteOwnerRequest, WireDebugAck>;
  getWriteOwner: UnaryMethod<WireGetWriteOwnerRequest, WireGetWriteOwnerResponse>;
  setFence: UnaryMethod<WireSetFenceRequest, WireDebugAck>;
  getFence: UnaryMethod<WireGetFenceRequest, WireGetFenceResponse>;
  setFencingCounter: UnaryMethod<WireSetFencingCounterRequest, WireDebugAck>;
  getFencingCounter: UnaryMethod<WireGetFencingCounterRequest, WireGetFencingCounterResponse>;
  ownedPaths: UnaryMethod<WireOwnedPathsRequest, WireOwnedPathsResponse>;
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
  PathLockDebug: ServiceClientConstructor<PathLockDebugServiceClient>;
}

/** Bundled proto, resolved relative to the compiled output (dist/ -> ../proto). */
export const PROTO_PATH = path.join(__dirname, '..', 'proto', 'pathlockd.proto');

let cached: PathlockdPackage | undefined;

/** Load (once) and return the `pathlockd.v1` package namespace. */
export function loadPathlockdProto(): PathlockdPackage {
  if (cached) return cached;
  const def = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false, // camelCase fields: owner_id -> ownerId
    longs: Number, // int64 (fencing token) as JS number — safe well past 2^53 here
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

export function buildCredentials(tls: boolean): grpc.ChannelCredentials {
  return tls ? grpc.credentials.createSsl() : grpc.credentials.createInsecure();
}
