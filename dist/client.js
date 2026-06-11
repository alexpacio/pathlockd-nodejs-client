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
function hasWriteRequest(params) {
    return params.requests.some((r) => (r.mode ?? 'write') === 'write');
}
function assertPositiveFencingToken(value, fieldName) {
    if (typeof value !== 'bigint' || value <= 0n) {
        throw new Error(`${fieldName} must be a positive bigint`);
    }
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
        if (hasWriteRequest(params)) {
            assertPositiveFencingToken(params.fencingToken, 'Acquire.fencingToken');
        }
        const res = await unary(this.client, 'acquire', {
            ownerId: params.ownerId,
            ttlMs: (0, proto_1.toWirePositiveUint64)(params.ttlMs, 'Acquire.ttlMs'),
            requests: params.requests.map((r) => ({
                path: r.path,
                mode: proto_1.MODE_TO_WIRE[r.mode ?? 'write'],
                state: proto_1.STATE_TO_WIRE[r.state ?? 'new'],
            })),
            fencingToken: (0, proto_1.bigintToWireInt64)(params.fencingToken, 'Acquire.fencingToken'),
            releaseRequests: (params.releaseRequests ?? []).map(wireRelease),
            emitRelease: params.emitRelease ?? false,
        });
        return {
            status: (0, proto_1.decodeWireEnum)(proto_1.ACQUIRE_STATUS_FROM_WIRE, res.status, 'AcquireResponse.status'),
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
        const res = await unary(this.client, 'renew', {
            ownerId,
            ttlMs: (0, proto_1.toWirePositiveUint64)(ttlMs, 'Renew.ttlMs'),
        });
        return {
            status: (0, proto_1.decodeWireEnum)(proto_1.RENEW_STATUS_FROM_WIRE, res.status, 'RenewResponse.status'),
            path: res.path ?? '',
            reason: res.reason ?? '',
        };
    }
    async forceRelease(victimId) {
        await unary(this.client, 'forceRelease', { victimId });
    }
    async assertFencing(ownerId, fencingToken, paths) {
        if (paths.length > 0) {
            assertPositiveFencingToken(fencingToken, 'AssertFencing.fencingToken');
        }
        const res = await unary(this.client, 'assertFencing', {
            ownerId,
            fencingToken: (0, proto_1.bigintToWireInt64)(fencingToken, 'AssertFencing.fencingToken'),
            paths,
        });
        return {
            status: (0, proto_1.decodeWireEnum)(proto_1.ASSERT_STATUS_FROM_WIRE, res.status, 'AssertFencingResponse.status'),
            path: res.path ?? '',
            reason: res.reason ?? '',
        };
    }
    async detectCycle(startOwnerId, maxDepth) {
        const res = await unary(this.client, 'detectCycle', { startOwnerId, maxDepth });
        return {
            kind: (0, proto_1.decodeWireEnum)(proto_1.CYCLE_KIND_FROM_WIRE, res.kind, 'DetectCycleResponse.kind'),
            chain: res.chain ?? [],
        };
    }
    async isBlocking(conflictPath, conflictOwner, reason) {
        const res = await unary(this.client, 'isBlocking', { conflictPath, conflictOwner, reason });
        return Boolean(res.blocking);
    }
    async incrFencingToken() {
        const res = await unary(this.client, 'incrFencingToken', {});
        return (0, proto_1.wireInt64ToBigInt)(res.token, 'IncrFencingTokenResponse.token');
    }
    async setWaitEdge(ownerId, conflictOwner, ttlMs, metadata) {
        if (metadata && (!metadata.conflictPath || !metadata.reason)) {
            throw new Error('SetWaitEdge metadata requires both conflictPath and reason');
        }
        await unary(this.client, 'setWaitEdge', {
            ownerId,
            conflictOwner,
            ttlMs: (0, proto_1.toWirePositiveUint64)(ttlMs, 'SetWaitEdge.ttlMs'),
            conflictPath: metadata?.conflictPath ?? '',
            reason: metadata?.reason ?? '',
        });
    }
    async clearWaitEdge(ownerId) {
        await unary(this.client, 'clearWaitEdge', { ownerId });
    }
    /**
     * Plant an anti-starvation claim reserving `path` for `claimantOwnerId`.
     * Claim-if-absent: a live claim by another claimant is reported as `held`
     * (never overwritten); re-planting one's own claim re-arms its TTL. Claims
     * are TTL-governed only — the claimant needs no lease, so a pure waiter can
     * reserve the path it is queued for, and a crashed claimant's reservation
     * expires on its own. The claimant's own acquire consumes the claim
     * atomically on grant.
     */
    async setClaim(path, claimantOwnerId, ttlMs = 0) {
        const res = await unary(this.client, 'setClaim', {
            path,
            claimantOwnerId,
            ttlMs: (0, proto_1.toWireUint64)(ttlMs, 'SetClaim.ttlMs'),
        });
        return {
            status: (0, proto_1.decodeWireEnum)(proto_1.SET_CLAIM_STATUS_FROM_WIRE, res.status, 'SetClaimResponse.status'),
            claimOwner: res.claimOwner ? res.claimOwner : null,
        };
    }
    /** Clear `claimantOwnerId`'s own claim on `path`; a foreign claim is untouched. */
    async clearClaim(path, claimantOwnerId) {
        await unary(this.client, 'clearClaim', { path, claimantOwnerId });
    }
    async isOwnerAlive(ownerId) {
        const res = await unary(this.client, 'isOwnerAlive', { ownerId });
        return Boolean(res.alive);
    }
    /**
     * Read-only snapshot of the lock state at one exact path: live write owner,
     * live read owners, fence value and any preemption claim. Filtered by owner
     * liveness; never mutates daemon state.
     */
    async inspectPath(path) {
        const res = await unary(this.client, 'inspectPath', { path });
        return {
            writeOwner: res.writeOwner ? res.writeOwner : null,
            readOwners: res.readOwners ?? [],
            fence: res.hasFence ? (0, proto_1.wireInt64ToBigInt)(res.fence, 'InspectPathResponse.fence') : null,
            claimOwner: res.claimOwner ? res.claimOwner : null,
        };
    }
    /**
     * Read-only listing of every lock recorded for one owner, plus whether its
     * liveness lease is still present. The owner-centric companion to
     * {@link inspectPath}.
     */
    async listOwnerLocks(ownerId) {
        const res = await unary(this.client, 'listOwnerLocks', { ownerId });
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
            const res = await unary(this.client, 'dumpLocks', { cursor, ownerPage });
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
     * Publish a cooperative REVOKE for `ownerId`. When `claim` is supplied, the
     * daemon also reserves `claim.path` for `claim.claimantOwnerId` (for
     * `claim.ttlMs`, or a short default) before publishing, so the revoked victim
     * cannot re-acquire the path before the claimant does. Omitting `claim`
     * yields the legacy pure-notification behavior.
     */
    async requestRevoke(ownerId, claim) {
        const req = { ownerId };
        if (claim) {
            req.claimPath = claim.path;
            req.claimantOwnerId = claim.claimantOwnerId;
            req.claimTtlMs = String(claim.ttlMs ?? 0);
        }
        await unary(this.client, 'requestRevoke', req);
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
//# sourceMappingURL=client.js.map