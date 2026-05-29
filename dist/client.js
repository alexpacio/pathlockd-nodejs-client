"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PathlockdDebugClient = exports.PathlockdClient = exports.PathlockdSubscription = void 0;
const events_1 = require("events");
const proto_1 = require("./proto");
/** Promisify a callback-style unary call, dispatched by method name on `client`. */
function unary(client, method, request) {
    return new Promise((resolve, reject) => {
        const fn = client[method];
        // Member dispatch is lost when the method is held in a local, so re-bind
        // `this` to the client (grpc-js client methods rely on it).
        fn.call(client, request, (err, response) => (err ? reject(err) : resolve(response)));
    });
}
function wireRelease(r) {
    return { path: r.path, mode: proto_1.MODE_TO_WIRE[r.mode ?? 'write'] };
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
class PathlockdSubscription extends events_1.EventEmitter {
    constructor(stream) {
        super();
        this.stream = stream;
        stream.on('data', (msg) => {
            const type = proto_1.EVENT_TYPE_FROM_WIRE[msg.type] ?? 'released';
            const event = { type, ownerId: msg.ownerId };
            this.emit('event', event);
        });
        stream.on('error', (err) => this.emit('error', err));
        stream.on('end', () => this.emit('end'));
        stream.on('close', () => this.emit('close'));
    }
    on(event, listener) {
        return super.on(event, listener);
    }
    /** Cancel the stream. */
    close() {
        this.stream.cancel();
    }
}
exports.PathlockdSubscription = PathlockdSubscription;
/**
 * Typed, promise-based client for the pathlockd `PathLock` service.
 *
 * Every method forwards a single gRPC call and maps the wire representation to
 * the ergonomic types in {@link types}. The lock *orchestration* (renewal loop,
 * deadlock resolution, retry/wait) lives in the caller — this client only
 * exposes the primitives.
 */
class PathlockdClient {
    constructor(opts) {
        const ns = (0, proto_1.loadPathlockdProto)();
        this.client = new ns.PathLock(opts.endpoint, (0, proto_1.buildCredentials)(opts.tls ?? false), opts.channelOptions ?? {});
    }
    /** Wait until the channel is ready (or reject after `timeoutMs`). */
    waitForReady(timeoutMs = 5000) {
        return new Promise((resolve, reject) => {
            const deadline = new Date(Date.now() + timeoutMs);
            this.client.waitForReady(deadline, (err) => (err ? reject(err) : resolve()));
        });
    }
    async acquire(params) {
        const res = await unary(this.client, 'acquire', {
            ownerId: params.ownerId,
            ttlMs: params.ttlMs,
            requests: params.requests.map((r) => ({
                path: r.path,
                mode: proto_1.MODE_TO_WIRE[r.mode ?? 'write'],
                state: proto_1.STATE_TO_WIRE[r.state ?? 'new'],
            })),
            fencingToken: params.fencingToken,
            releaseRequests: (params.releaseRequests ?? []).map(wireRelease),
            emitRelease: params.emitRelease ?? false,
        });
        return {
            status: proto_1.ACQUIRE_STATUS_FROM_WIRE[res.status] ?? 'ok',
            path: res.path ?? '',
            owner: res.owner ?? '',
            reason: res.reason ?? '',
        };
    }
    async release(ownerId, requests, delWaitKey = false) {
        await unary(this.client, 'release', {
            ownerId,
            requests: requests.map(wireRelease),
            delWaitKey,
        });
    }
    async releaseAll(ownerId, delWaitKey = false) {
        await unary(this.client, 'releaseAll', { ownerId, delWaitKey });
    }
    async renew(ownerId, ttlMs) {
        const res = await unary(this.client, 'renew', { ownerId, ttlMs });
        return {
            status: proto_1.RENEW_STATUS_FROM_WIRE[res.status] ?? 'ok',
            path: res.path ?? '',
            reason: res.reason ?? '',
        };
    }
    async forceRelease(victimId) {
        await unary(this.client, 'forceRelease', { victimId });
    }
    async assertFencing(ownerId, fencingToken, paths) {
        const res = await unary(this.client, 'assertFencing', { ownerId, fencingToken, paths });
        return {
            status: proto_1.ASSERT_STATUS_FROM_WIRE[res.status] ?? 'ok',
            path: res.path ?? '',
            reason: res.reason ?? '',
        };
    }
    async detectCycle(startOwnerId, maxDepth) {
        const res = await unary(this.client, 'detectCycle', { startOwnerId, maxDepth });
        return {
            kind: proto_1.CYCLE_KIND_FROM_WIRE[res.kind] ?? 'none',
            chain: res.chain ?? [],
        };
    }
    async isBlocking(conflictPath, conflictOwner, reason) {
        const res = await unary(this.client, 'isBlocking', { conflictPath, conflictOwner, reason });
        return Boolean(res.blocking);
    }
    async incrFencingToken() {
        const res = await unary(this.client, 'incrFencingToken', {});
        return Number(res.token);
    }
    async setWaitEdge(ownerId, conflictOwner, ttlMs) {
        await unary(this.client, 'setWaitEdge', { ownerId, conflictOwner, ttlMs });
    }
    async clearWaitEdge(ownerId) {
        await unary(this.client, 'clearWaitEdge', { ownerId });
    }
    async isOwnerAlive(ownerId) {
        const res = await unary(this.client, 'isOwnerAlive', { ownerId });
        return Boolean(res.alive);
    }
    async requestRevoke(ownerId) {
        await unary(this.client, 'requestRevoke', { ownerId });
    }
    /**
     * Open the per-owner event stream for `ownerId`. The returned subscription
     * only ever emits events for that owner (its `revoke`, `kill`, or own
     * `released`). Returns immediately; events arrive via the emitter.
     */
    subscribe(ownerId) {
        const stream = this.client.subscribe({ ownerId });
        return new PathlockdSubscription(stream);
    }
    async health() {
        const res = await unary(this.client, 'health', {});
        return { ok: Boolean(res.ok), detail: res.detail ?? '' };
    }
    close() {
        this.client.close();
    }
}
exports.PathlockdClient = PathlockdClient;
class PathlockdDebugClient {
    constructor(opts) {
        const ns = (0, proto_1.loadPathlockdProto)();
        this.client = new ns.PathLockDebug(opts.endpoint, (0, proto_1.buildCredentials)(opts.tls ?? false), opts.channelOptions ?? {});
    }
    waitForReady(timeoutMs = 5000) {
        return new Promise((resolve, reject) => {
            const deadline = new Date(Date.now() + timeoutMs);
            this.client.waitForReady(deadline, (err) => (err ? reject(err) : resolve()));
        });
    }
    async flush() {
        const res = await unary(this.client, 'flush', {});
        return Number(res.deleted ?? 0);
    }
    async expireOwner(ownerId) {
        await unary(this.client, 'expireOwner', { ownerId });
    }
    async deleteLockKey(path, mode, ownerId = '') {
        await unary(this.client, 'deleteLockKey', { path, mode: proto_1.MODE_TO_WIRE[mode], ownerId });
    }
    async setWriteOwner(path, ownerId) {
        await unary(this.client, 'setWriteOwner', { path, ownerId });
    }
    async getWriteOwner(path) {
        const res = await unary(this.client, 'getWriteOwner', { path });
        return res.exists ? res.ownerId : null;
    }
    async setFence(path, value) {
        await unary(this.client, 'setFence', { path, value });
    }
    async getFence(path) {
        const res = await unary(this.client, 'getFence', { path });
        return res.exists ? Number(res.value) : null;
    }
    async setFencingCounter(value) {
        await unary(this.client, 'setFencingCounter', { value });
    }
    async getFencingCounter() {
        const res = await unary(this.client, 'getFencingCounter', {});
        return Number(res.value ?? 0);
    }
    async ownedPaths(ownerId) {
        const res = await unary(this.client, 'ownedPaths', { ownerId });
        return { members: res.members ?? [], alive: Boolean(res.alive) };
    }
    close() {
        this.client.close();
    }
}
exports.PathlockdDebugClient = PathlockdDebugClient;
//# sourceMappingURL=client.js.map