import * as grpc from '@grpc/grpc-js';
import { AcquireStatus, AssertStatus, CycleKind, LockEventType, LockMode, LockState, RenewStatus } from './types';
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
/** A unary RPC method: callback-style request/response, returns the call handle. */
export type UnaryMethod<Req, Res> = (request: Req, callback: (err: grpc.ServiceError | null, response: Res) => void) => grpc.ClientUnaryCall;
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
export type ServiceClientConstructor<C> = new (address: string, credentials: grpc.ChannelCredentials, options?: Record<string, unknown>) => C;
/** The `pathlockd.v1` package namespace. */
export interface PathlockdPackage {
    PathLock: ServiceClientConstructor<PathLockServiceClient>;
    PathLockDebug: ServiceClientConstructor<PathLockDebugServiceClient>;
}
/** Bundled proto, resolved relative to the compiled output (dist/ -> ../proto). */
export declare const PROTO_PATH: string;
/** Load (once) and return the `pathlockd.v1` package namespace. */
export declare function loadPathlockdProto(): PathlockdPackage;
export declare const MODE_TO_WIRE: Record<LockMode, WireMode>;
export declare const STATE_TO_WIRE: Record<LockState, WireLockState>;
export declare const ACQUIRE_STATUS_FROM_WIRE: Record<string, AcquireStatus>;
export declare const RENEW_STATUS_FROM_WIRE: Record<string, RenewStatus>;
export declare const ASSERT_STATUS_FROM_WIRE: Record<string, AssertStatus>;
export declare const CYCLE_KIND_FROM_WIRE: Record<string, CycleKind>;
export declare const EVENT_TYPE_FROM_WIRE: Record<string, LockEventType>;
export declare function buildCredentials(tls: boolean): grpc.ChannelCredentials;
export {};
//# sourceMappingURL=proto.d.ts.map