"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PathlockdClient = exports.PathlockdSubscription = void 0;
const events_1 = require("events");
const proto_1 = require("./proto");
/**
 * Safety cap for {@link PathlockdClient.dumpLocks} when the caller does not set
 * one: an unbounded cluster dump could exhaust memory, so collection stops and
 * throws past this many entries. Page manually with a higher cap if needed.
 */
const DUMP_DEFAULT_MAX_ENTRIES = 100_000;
const ACQUIRE_UNARY_MAX_PATHS = 1024;
const ACQUIRE_STREAM_CHUNK_PATHS = 1024;
/** Promisify a callback-style unary call, dispatched by method name on `client`. */
function unary(client, method, request, options = {}, signal) {
    return new Promise((resolve, reject) => {
        const fn = client[method];
        // Member dispatch is lost when the method is held in a local, so re-bind
        // `this` to the client (grpc-js client methods rely on it).
        const call = fn.call(client, request, options, (err, response) => {
            if (signal)
                signal.removeEventListener('abort', onAbort);
            return err ? reject(err) : resolve(response);
        });
        // grpc-js has no AbortSignal in CallOptions; cancel through the call handle.
        // A cancelled call surfaces as a CANCELLED gRPC error to the callback above.
        const onAbort = () => call.cancel();
        if (signal) {
            if (signal.aborted)
                call.cancel();
            else
                signal.addEventListener('abort', onAbort, { once: true });
        }
    });
}
function clientStreaming(client, method, requests, options = {}, signal) {
    return new Promise((resolve, reject) => {
        let settled = false;
        const settleResolve = (response) => {
            if (settled)
                return;
            settled = true;
            if (signal)
                signal.removeEventListener('abort', onAbort);
            resolve(response);
        };
        const settleReject = (err) => {
            if (settled)
                return;
            settled = true;
            if (signal)
                signal.removeEventListener('abort', onAbort);
            reject(err);
        };
        const fn = client[method];
        const call = fn.call(client, options, (err, response) => (err ? settleReject(err) : settleResolve(response)));
        // grpc-js has no AbortSignal in CallOptions; cancel through the call handle.
        const onAbort = () => call.cancel();
        if (signal) {
            if (signal.aborted)
                call.cancel();
            else
                signal.addEventListener('abort', onAbort, { once: true });
        }
        call.on('error', settleReject);
        void (async () => {
            try {
                for (const request of requests) {
                    if (!call.write(request)) {
                        await (0, events_1.once)(call, 'drain');
                    }
                }
                call.end();
            }
            catch (err) {
                call.destroy();
                settleReject(err);
            }
        })();
    });
}
function wireRelease(r) {
    return { path: r.path, mode: proto_1.MODE_TO_WIRE[r.mode ?? 'write'] };
}
function wireAcquireRequest(params) {
    return {
        ownerId: params.ownerId,
        ttlMs: (0, proto_1.toWirePositiveUint64)(params.ttlMs, 'Acquire.ttlMs'),
        requests: params.requests.map((r) => ({
            path: r.path,
            mode: proto_1.MODE_TO_WIRE[r.mode ?? 'write'],
            state: proto_1.STATE_TO_WIRE[r.state ?? 'new'],
            permits: (0, proto_1.toWireUint32)(r.permits ?? 0, `Acquire.requests[${r.path}].permits`),
        })),
        fencingToken: (0, proto_1.bigintToWireInt64)(params.fencingToken ?? 0n, 'Acquire.fencingToken'),
        releaseRequests: (params.releaseRequests ?? []).map(wireRelease),
        queueTtlMs: (0, proto_1.toWireUint64)(params.queueTtlMs ?? 0, 'Acquire.queueTtlMs'),
        ...idempotencyFields(params),
    };
}
function decodeAcquireResponse(res) {
    return {
        status: (0, proto_1.decodeWireEnum)(proto_1.ACQUIRE_STATUS_FROM_WIRE, res.status, 'AcquireResponse.status'),
        path: res.path ?? '',
        owner: res.owner ?? '',
        reason: (0, proto_1.decodeWireEnum)(proto_1.REASON_FROM_WIRE, res.reason, 'AcquireResponse.reason'),
        fencingToken: decodeOptionalFencingToken(res.fencingToken, 'AcquireResponse.fencingToken'),
        currentFencingToken: decodeOptionalFencingToken(res.currentFencingToken, 'AcquireResponse.currentFencingToken'),
        namespace: res.namespace ?? '',
    };
}
function decodeOptionalFencingToken(value, fieldName) {
    const token = (0, proto_1.wireInt64ToBigInt)(value, fieldName);
    return token === 0n ? null : token;
}
function acquirePathCount(request) {
    return request.requests.length + request.releaseRequests.length;
}
function* chunkAcquireRequest(request) {
    let requestIndex = 0;
    let releaseIndex = 0;
    let first = true;
    while (requestIndex < request.requests.length || releaseIndex < request.releaseRequests.length) {
        const requests = request.requests.slice(requestIndex, requestIndex + ACQUIRE_STREAM_CHUNK_PATHS);
        requestIndex += requests.length;
        const remaining = ACQUIRE_STREAM_CHUNK_PATHS - requests.length;
        const releaseRequests = request.releaseRequests.slice(releaseIndex, releaseIndex + remaining);
        releaseIndex += releaseRequests.length;
        yield {
            ownerId: first ? request.ownerId : '',
            ttlMs: first ? request.ttlMs : 0,
            requests,
            fencingToken: first ? request.fencingToken : 0,
            releaseRequests,
            queueTtlMs: first ? request.queueTtlMs : 0,
            ...(first && request.idempotencyKey ? { idempotencyKey: request.idempotencyKey } : {}),
        };
        first = false;
    }
}
function assertPositiveFencingToken(value, fieldName) {
    if (typeof value !== 'bigint' || value <= 0n) {
        throw new Error(`${fieldName} must be a positive bigint`);
    }
}
function assertNonNegativeFencingToken(value, fieldName) {
    if (typeof value !== 'bigint' || value < 0n) {
        throw new Error(`${fieldName} must be a non-negative bigint`);
    }
}
function idempotencyFields(options) {
    return options?.idempotencyKey ? { idempotencyKey: options.idempotencyKey } : {};
}
function normalizeReleaseOptions(optionsOrDelWaitKey) {
    if (typeof optionsOrDelWaitKey === 'boolean') {
        return { delWaitKey: optionsOrDelWaitKey };
    }
    return optionsOrDelWaitKey ?? {};
}
/**
 * A live, per-owner subscription to the pathlockd lifecycle event stream.
 *
 * It is bound to a single owner id and only ever surfaces events for that owner
 * (its cooperative `revoke`, a forced `kill`, or a queued acquire `grant`).
 *
 * Emits:
 *  - `event`  → {@link LockEvent}
 *  - `error`  → a gRPC stream error (attach a listener; EventEmitter throws otherwise)
 *  - `end`    → server ended the stream
 *  - `close`  → underlying stream closed
 */
class PathlockdSubscription extends events_1.EventEmitter {
    stream;
    constructor(stream) {
        super();
        this.stream = stream;
        stream.on('data', (msg) => {
            try {
                const type = (0, proto_1.decodeWireEnum)(proto_1.EVENT_TYPE_FROM_WIRE, msg.type, 'Event.type');
                const event = { type, ownerId: msg.ownerId };
                this.emit('event', event);
            }
            catch (err) {
                this.emit('error', err instanceof Error ? err : new Error(String(err)));
            }
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
    client;
    defaultCallTimeoutMs;
    constructor(opts) {
        const ns = (0, proto_1.loadPathlockdProto)();
        this.defaultCallTimeoutMs = opts.defaultCallTimeoutMs;
        this.client = new ns.PathLock(opts.endpoint, (0, proto_1.buildCredentials)(opts.tls ?? false), (0, proto_1.buildChannelOptions)(opts.channelOptions));
    }
    /**
     * Build grpc call options for one RPC: a deadline (per-call `deadlineMs`,
     * else the client default). The deadline spans any automatic transport
     * retries. Abort-signal cancellation is wired separately (grpc-js exposes it
     * through the call handle, not these options) — see {@link unary}.
     */
    callOptions(opts) {
        const timeoutMs = opts?.deadlineMs ?? this.defaultCallTimeoutMs;
        const callOpts = {};
        if (timeoutMs != null)
            callOpts.deadline = Date.now() + timeoutMs;
        return callOpts;
    }
    /** Wait until the channel is ready (or reject after `timeoutMs`). */
    waitForReady(timeoutMs = 5000) {
        return new Promise((resolve, reject) => {
            const deadline = new Date(Date.now() + timeoutMs);
            this.client.waitForReady(deadline, (err) => (err ? reject(err) : resolve()));
        });
    }
    async acquire(params) {
        if (params.fencingToken !== undefined) {
            assertNonNegativeFencingToken(params.fencingToken, 'Acquire.fencingToken');
        }
        const req = wireAcquireRequest(params);
        const callOpts = this.callOptions(params);
        const res = acquirePathCount(req) <= ACQUIRE_UNARY_MAX_PATHS
            ? await unary(this.client, 'acquire', req, callOpts, params.signal)
            : await clientStreaming(this.client, 'acquireStream', chunkAcquireRequest(req), callOpts, params.signal);
        return decodeAcquireResponse(res);
    }
    async setNamespacePolicy(namespace, algorithm, options) {
        await unary(this.client, 'setNamespacePolicy', {
            namespace,
            algorithm: proto_1.LOCK_ALGORITHM_TO_WIRE[algorithm],
            ...idempotencyFields(options),
        }, this.callOptions(options), options?.signal);
    }
    async getNamespacePolicy(namespace) {
        const res = await unary(this.client, 'getNamespacePolicy', { namespace }, this.callOptions());
        return {
            algorithm: (0, proto_1.decodeWireEnum)(proto_1.LOCK_ALGORITHM_FROM_WIRE, res.algorithm, 'GetNamespacePolicyResponse.algorithm'),
            explicit: Boolean(res.explicit),
        };
    }
    async deleteNamespacePolicy(namespace, options) {
        await unary(this.client, 'deleteNamespacePolicy', {
            namespace,
            ...idempotencyFields(options),
        }, this.callOptions(options), options?.signal);
    }
    async release(ownerId, requests, optionsOrDelWaitKey = false) {
        const options = normalizeReleaseOptions(optionsOrDelWaitKey);
        await unary(this.client, 'release', {
            ownerId,
            requests: requests.map(wireRelease),
            delWaitKey: options.delWaitKey ?? false,
            ...idempotencyFields(options),
        }, this.callOptions(options), options?.signal);
    }
    async releaseAll(ownerId, optionsOrDelWaitKey = false) {
        const options = typeof optionsOrDelWaitKey === 'boolean'
            ? { delWaitKey: optionsOrDelWaitKey }
            : (optionsOrDelWaitKey ?? {});
        await unary(this.client, 'releaseAll', {
            ownerId,
            delWaitKey: options.delWaitKey ?? false,
            domains: options.domains ?? [],
            ...idempotencyFields(options),
        }, this.callOptions(options), options?.signal);
    }
    async renew(ownerId, ttlMs, options = {}) {
        const res = await unary(this.client, 'renew', {
            ownerId,
            ttlMs: (0, proto_1.toWirePositiveUint64)(ttlMs, 'Renew.ttlMs'),
            domains: options.domains ?? [],
            ...idempotencyFields(options),
        }, this.callOptions(options), options?.signal);
        return {
            status: (0, proto_1.decodeWireEnum)(proto_1.RENEW_STATUS_FROM_WIRE, res.status, 'RenewResponse.status'),
            path: res.path ?? '',
            reason: (0, proto_1.decodeWireEnum)(proto_1.REASON_FROM_WIRE, res.reason, 'RenewResponse.reason'),
            revokeRequested: Boolean(res.revokeRequested),
        };
    }
    async forceRelease(victimId, options) {
        await unary(this.client, 'forceRelease', { victimId, ...idempotencyFields(options) }, this.callOptions(options), options?.signal);
    }
    async assertFencing(ownerId, fencingToken, paths) {
        if (paths.length > 0) {
            assertPositiveFencingToken(fencingToken, 'AssertFencing.fencingToken');
        }
        const res = await unary(this.client, 'assertFencing', {
            ownerId,
            fencingToken: (0, proto_1.bigintToWireInt64)(fencingToken, 'AssertFencing.fencingToken'),
            paths,
        }, this.callOptions());
        return {
            status: (0, proto_1.decodeWireEnum)(proto_1.ASSERT_STATUS_FROM_WIRE, res.status, 'AssertFencingResponse.status'),
            path: res.path ?? '',
            reason: (0, proto_1.decodeWireEnum)(proto_1.REASON_FROM_WIRE, res.reason, 'AssertFencingResponse.reason'),
        };
    }
    async detectCycle(startOwnerId, maxDepth) {
        const res = await unary(this.client, 'detectCycle', { startOwnerId, maxDepth }, this.callOptions());
        return {
            kind: (0, proto_1.decodeWireEnum)(proto_1.CYCLE_KIND_FROM_WIRE, res.kind, 'DetectCycleResponse.kind'),
            chain: res.chain ?? [],
        };
    }
    async isBlocking(conflictPath, conflictOwner, reason) {
        const res = await unary(this.client, 'isBlocking', {
            conflictPath,
            conflictOwner,
            reason: proto_1.REASON_TO_WIRE[reason],
        }, this.callOptions());
        return Boolean(res.blocking);
    }
    async incrFencingToken(options) {
        const res = await unary(this.client, 'incrFencingToken', idempotencyFields(options), this.callOptions(options), options?.signal);
        return (0, proto_1.wireInt64ToBigInt)(res.token, 'IncrFencingTokenResponse.token');
    }
    async setWaitEdge(ownerId, conflictOwner, ttlMs, metadata, options) {
        if (metadata && (!metadata.conflictPath || !metadata.reason)) {
            throw new Error('SetWaitEdge metadata requires both conflictPath and reason');
        }
        await unary(this.client, 'setWaitEdge', {
            ownerId,
            conflictOwner,
            ttlMs: (0, proto_1.toWirePositiveUint64)(ttlMs, 'SetWaitEdge.ttlMs'),
            conflictPath: metadata?.conflictPath ?? '',
            reason: proto_1.REASON_TO_WIRE[metadata?.reason ?? 'unspecified'],
            ...idempotencyFields(options),
        }, this.callOptions(options), options?.signal);
    }
    async clearWaitEdge(ownerId, options) {
        await unary(this.client, 'clearWaitEdge', { ownerId, ...idempotencyFields(options) }, this.callOptions(options), options?.signal);
    }
    async isOwnerAlive(ownerId, options) {
        const res = await unary(this.client, 'isOwnerAlive', { ownerId, domains: options?.domains ?? [] }, this.callOptions(options), options?.signal);
        return Boolean(res.alive);
    }
    /**
     * Read-only snapshot of the lock state at one exact path: live write owner,
     * live read owners, semaphore owners, and fence value. Filtered by owner
     * liveness; never mutates daemon state.
     */
    async inspectPath(path) {
        const res = await unary(this.client, 'inspectPath', { path }, this.callOptions());
        return {
            writeOwner: res.writeOwner ? res.writeOwner : null,
            readOwners: res.readOwners ?? [],
            fence: res.hasFence ? (0, proto_1.wireInt64ToBigInt)(res.fence, 'InspectPathResponse.fence') : null,
            semaphoreOwners: res.semaphoreOwners ?? [],
        };
    }
    /**
     * Read-only listing of every lock recorded for one owner, plus whether its
     * liveness lease is still present. The owner-centric companion to
     * {@link inspectPath}.
     */
    async listOwnerLocks(ownerId, options) {
        const res = await unary(this.client, 'listOwnerLocks', { ownerId, domains: options?.domains ?? [] }, this.callOptions(options), options?.signal);
        const locks = (res.locks ?? []).map((l) => ({
            path: l.path,
            mode: (0, proto_1.decodeWireEnum)(proto_1.MODE_FROM_WIRE, l.mode, 'OwnedLock.mode'),
        }));
        return { alive: Boolean(res.alive), locks };
    }
    /**
     * Dump every live lock across the cluster, auto-paginating internally. Each
     * entry is one (owner, mode, path) holding with the fence for write locks.
     *
     * Best-effort observability: the daemon reads each owner in its own snapshot,
     * so the result is near-real-time, not a single global instant. To bound
     * memory, collection stops and throws once `maxEntries` entries are seen
     * (default {@link DUMP_DEFAULT_MAX_ENTRIES}); for very large clusters drive
     * {@link dumpLocksPages} directly and stream instead.
     */
    async dumpLocks(opts = {}) {
        const maxEntries = opts.maxEntries ?? DUMP_DEFAULT_MAX_ENTRIES;
        const out = [];
        for await (const page of this.dumpLocksPages(opts.ownerPage)) {
            for (const entry of page) {
                if (out.length >= maxEntries) {
                    throw new Error(`dumpLocks exceeded maxEntries (${maxEntries}); raise the cap or page with dumpLocksPages`);
                }
                out.push(entry);
            }
        }
        return out;
    }
    /**
     * Lower-level dump: an async generator yielding one decoded page of lock
     * entries per daemon round-trip. Lets callers stream an arbitrarily large
     * cluster without buffering it all. `ownerPage` sets how many owners the
     * daemon scans per page (0 / omitted uses the server default).
     */
    async *dumpLocksPages(ownerPage = 0) {
        let cursor = Buffer.alloc(0);
        for (;;) {
            const res = await unary(this.client, 'dumpLocks', { cursor, ownerPage }, this.callOptions());
            const page = (res.entries ?? []).map((e) => ({
                owner: e.owner,
                path: e.path,
                mode: (0, proto_1.decodeWireEnum)(proto_1.MODE_FROM_WIRE, e.mode, 'LockEntry.mode'),
                fence: e.hasFence ? (0, proto_1.wireInt64ToBigInt)(e.fence, 'LockEntry.fence') : null,
            }));
            if (page.length > 0)
                yield page;
            if (res.done)
                return;
            cursor = res.nextCursor;
        }
    }
    /**
     * Publish a cooperative REVOKE for `ownerId`: the daemon asks that owner to
     * release its locks (to break a detected deadlock cycle). The wait queue's
     * FIFO admission keeps the revoked victim queued behind the winner, so no
     * preemption reservation is needed.
     */
    async requestRevoke(ownerId) {
        await unary(this.client, 'requestRevoke', { ownerId }, this.callOptions());
    }
    /**
     * Open the per-owner event stream for `ownerId`. The returned subscription
     * only ever emits events for that owner (its `revoke`, `kill`, or `grant`). Returns
     * immediately; events arrive via the emitter.
     */
    subscribe(ownerId) {
        const stream = this.client.subscribe({ ownerId });
        return new PathlockdSubscription(stream);
    }
    async health() {
        const res = await unary(this.client, 'health', {}, this.callOptions());
        return { ok: Boolean(res.ok), detail: res.detail ?? '' };
    }
    close() {
        this.client.close();
    }
}
exports.PathlockdClient = PathlockdClient;
//# sourceMappingURL=client.js.map