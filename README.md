# pathlockd-nodejs-client

Typed **Node.js / TypeScript** gRPC client for
[`pathlockd`](https://github.com/alexpacio/pathlockd) — fast, scalable,
opinionated path-based distributed locking primitives backed by an embedded
Multi-Raft + RocksDB engine.

It wraps the daemon's gRPC API in a small, fully-typed, promise-based surface:
string-union enums (`'write' | 'read'`), camelCase fields, a typed event emitter
for the per-owner lifecycle stream, and the `.proto` + generated `.d.ts` bundled
in the package — no codegen in your project.

> This is a thin client: it forwards primitives. Lock *orchestration* (renewal
> cadence, contention waiting, deadlock policy) belongs in your code.

## Install

```bash
npm install pathlockd-nodejs-client
```

The published package ships prebuilt JS + type declarations in `dist/`, so
installing from a Git ref needs no build step. Linux and macOS are supported on
**x86_64** and **arm64**.

## Quick start

```ts
import { PathlockdClient } from 'pathlockd-nodejs-client';

const client = new PathlockdClient({ endpoint: 'localhost:50051' });
await client.waitForReady(5000);

// 1) acquire a write lock on the subtree "google_drive:/a/b"
const res = await client.acquire({
  ownerId: 'owner-1',
  ttlMs: 10_000,
  requests: [{ path: 'google_drive:/a/b', mode: 'write', state: 'new' }],
});

if (res.status === 'ok') {
  // ...do work, renewing periodically so the lease never lapses...
  await client.renew('owner-1', 10_000);
} else if (res.status === 'queued') {
  console.log(`queued behind ${res.owner} on ${res.path} (${res.reason})`);
}

// 2) before an external side effect, prove you still own the path at its token
const fencingToken = res.fencingToken;
if (res.status !== 'ok' || fencingToken === null) throw new Error('lock not acquired');
const check = await client.assertFencing('owner-1', fencingToken, ['google_drive:/a/b']);
if (check.status === 'fail') throw new Error(`lost the lock: ${check.reason}`);

// 3) release everything when done
await client.releaseAll('owner-1', true);
client.close();
```

## Per-owner events

A subscription is bound to a single owner and only ever surfaces that owner's
lifecycle events: a cooperative `revoke`, a forced `kill`, or the `grant`
of a queued acquire.

```ts
const sub = client.subscribe('owner-1');
sub.on('event', (e) => {
  // e.type is 'grant' | 'killed' | 'revoke'; e.ownerId === 'owner-1'
  if (e.type === 'revoke') {
    // Cooperative preemption: finish the in-flight unit of work ASAP, then
    // release (e.g. releaseAll(ownerId, true)).
  } else if (e.type === 'killed') {
    // Forced preemption: your locks are ALREADY gone and your fencing token is
    // now stale. Stop touching the backing store immediately and abort — do NOT
    // keep working, and there is nothing to release (a late write would be
    // rejected by the new holder's AssertFencing anyway).
  } else if (e.type === 'grant') {
    // A previously queued acquire now holds its requested locks.
  }
});
sub.on('error', (err) => console.error(err)); // attach this — EventEmitter throws otherwise
// later:
sub.close();
```

> A subscription never carries information about other owners. Contended
> acquires are granted in place by the daemon; wait for the queued owner's
> `grant` event instead of retrying the acquire.

## Connection management & reliability

**Share one client.** A `PathlockdClient` owns a single gRPC channel that
multiplexes every call — all unary RPCs *and* any number of `subscribe(ownerId)`
streams — over one HTTP/2 connection. Construct it once per process and reuse it;
constructing a client per lock owner (or per request) churns TCP/HTTP-2
connections and pays connection-setup latency on your hot path for no benefit.

```ts
// Process-wide, created once.
const client = new PathlockdClient({ endpoint, defaultCallTimeoutMs: 15_000 });

// Per owner: only the subscription stream is owner-scoped. Open it on the
// shared client; do NOT create a new client and do NOT call client.close()
// when an owner finishes (that tears down the channel for everyone) — just
// sub.close().
const sub = client.subscribe(ownerId);
// ...
sub.close();
```

**Built-in channel defaults.** Every channel is created with reliability defaults
(`DEFAULT_CHANNEL_OPTIONS`), shallow-merged *under* any `channelOptions` you pass
(your keys win):

- **Keepalive** (30s ping / 10s timeout) so a half-open connection on a
  long-lived subscription is detected promptly instead of hanging until the OS
  TCP timeout.
- **Automatic retry** of calls that fail with `UNAVAILABLE` (a Raft leader
  election/failover, a load-shed, a transient blip), bounded and backing off.
  Every mutating RPC carries an `idempotencyKey` and every read is idempotent,
  so a retry can never double-apply.
- **64 MiB receive limit** so large `dumpLocks` pages aren't rejected.

**Deadlines.** Without a deadline a call waits forever for the daemon. Set
`defaultCallTimeoutMs` on the client (applied to every call), or pass a per-call
`deadlineMs` — e.g. on `acquire`, bound it by your own acquisition budget so a
stuck round-trip can't outlive the operation. Pass a `signal` (AbortSignal) on
any call to cancel the in-flight RPC the instant your operation is aborted,
rather than waiting for the deadline:

```ts
const ac = new AbortController();
await client.acquire({
  ownerId, ttlMs: 10_000, requests,
  deadlineMs: 5_000,   // overrides defaultCallTimeoutMs for this call
  signal: ac.signal,   // ac.abort() cancels the call
});
```

The deadline spans any automatic retries (it is not reset per attempt).

## API

`new PathlockdClient({ endpoint, tls?, channelOptions?, defaultCallTimeoutMs? })`

| Method | Returns |
|---|---|
| `acquire(params)` | `AcquireResult` — `status: 'ok' \| 'conflict' \| 'lost' \| 'queued'` |
| `setNamespacePolicy(namespace, algorithm, options?)` | `void` |
| `getNamespacePolicy(namespace)` | `NamespacePolicyResult` |
| `deleteNamespacePolicy(namespace, options?)` | `void` |
| `release(ownerId, requests, delWaitKeyOrOptions?)` | `void` |
| `releaseAll(ownerId, delWaitKeyOrOptions?)` | `void` |
| `renew(ownerId, ttlMs, options?)` | `RenewResult` — `status: 'ok' \| 'lost'` |
| `forceRelease(victimId, options?)` | `void` |
| `assertFencing(ownerId, fencingToken, paths)` | `AssertResult` — `status: 'ok' \| 'fail'` |
| `detectCycle(startOwnerId, maxDepth)` | `CycleResult` — `kind: 'none' \| 'cycle' \| 'truncated'` |
| `isBlocking(path, owner, reason)` | `boolean` |
| `incrFencingToken(options?)` | `bigint` (monotonic token; exact beyond 2^53) |
| `setWaitEdge(ownerId, conflictOwner, ttlMs, metadata?, options?)` | `void` |
| `clearWaitEdge(ownerId, options?)` | `void` |
| `isOwnerAlive(ownerId)` | `boolean` |
| `requestRevoke(ownerId)` | `void` |
| `inspectPath(path)` | `PathLockInfo` — write owner, read owners, semaphore owners, fence |
| `listOwnerLocks(ownerId)` | `OwnerLocksResult` — `{ alive, locks }` |
| `dumpLocks({ ownerPage?, maxEntries? })` | `LockEntry[]` — every live lock, auto-paginated |
| `dumpLocksPages(ownerPage?)` | `AsyncGenerator<LockEntry[]>` — stream the dump one page at a time |
| `subscribe(ownerId)` | `PathlockdSubscription` (typed EventEmitter) |
| `health()` | `{ ok, detail }` |
| `waitForReady(timeoutMs?)` / `close()` | — |

All request/result shapes are exported types (`AcquireParams`, `AcquireResult`,
`LockRequest`, `LockMode`, `LockState`, `RenewResult`, `AssertResult`,
`CycleResult`, `LockEvent`, `LockAlgorithm`, `ReasonCode`, …).

### Lock algorithms and semaphores

Namespace policies select one of `recursive_rw`, `point_rw`,
`recursive_write`, `point_write`, or `semaphore`. A path namespace is also
an explicit routing root:

```ts
await client.setNamespacePolicy('jobs:/workers', 'semaphore');

const permit = await client.acquire({
  ownerId: 'worker-7',
  ttlMs: 10_000,
  requests: [{ path: 'jobs:/workers/render', permits: 8 }],
});
```

Semaphore capacity is per path, not per namespace. The first acquire establishes
the path's capacity; later acquires for that path must supply the same
`permits` value. Semaphore locks are point-scoped, write-only, and do not use
fencing tokens. Changing a namespace's effective algorithm clears its held and
queued locks, and affected owners receive `killed` events.

Mutating RPCs that support daemon-side apply-once semantics accept
`idempotencyKey` either in their params object (`acquire`) or an optional
options object.

For `renew`, `options.domains` targets the renew fan-out at exactly the routing
namespaces where the owner holds locks — strongly recommended, since an empty
list makes the daemon probe **every** lock group on every heartbeat. You don't
have to compute namespaces yourself: a successful `acquire` echoes the routing
namespace it resolved to in `AcquireResult.namespace`. Record it per held path
and replay the distinct set as `domains`. Because the daemon resolved it, the
mapping is always correct (explicit namespaces included), and it can never
mis-target a group the owner doesn't hold:

```ts
const heldNamespaces = new Set<string>();

const res = await client.acquire({ ownerId: 'owner-1', ttlMs: 10_000, requests });
if (res.status === 'ok') heldNamespaces.add(res.namespace);

await client.renew('owner-1', 10_000, {
  domains: [...heldNamespaces],
  idempotencyKey: 'owner-1:renew:42',
});

await client.releaseAll('owner-1', {
  delWaitKey: true,
  idempotencyKey: 'owner-1:release-all:42',
});
```

For deadlock detection, pass the `path` and `reason` from a conflict response as
`metadata`:

```ts
await client.setWaitEdge(ownerId, conflict.owner, ttlMs, {
  conflictPath: conflict.path,
  reason: conflict.reason,
});
```

The daemon uses that metadata to discard stale live-owner wait edges before they
can be reported as deadlock cycles. Acquire fencing is optional: omit it or pass
`0n` to let the daemon mint a token, then read the exact `bigint` from
`AcquireResult.fencingToken`. Non-empty fencing assertions require a positive
`bigint`.

### Inspection

Three read-only calls expose live lock state for operators and tooling. They
filter by owner liveness (so they reflect what would actually block) and never
mutate daemon state:

```ts
// Path-centric: who holds this exact path?
const info = await client.inspectPath('google_drive:/a/b');
// info.writeOwner, info.readOwners[], info.semaphoreOwners[], info.fence

// Owner-centric: what does this owner hold?
const { alive, locks } = await client.listOwnerLocks('owner-1');

// Cluster-wide: every live lock (auto-paginated; bounded by maxEntries).
const all = await client.dumpLocks({ maxEntries: 50_000 });
for (const e of all) console.log(e.owner, e.mode, e.path, e.fence);

// Or stream a very large cluster a page at a time:
for await (const page of client.dumpLocksPages()) {
  for (const e of page) { /* ... */ }
}
```

## Development

```bash
npm install
npm run build   # tsc -> dist/
```

Keep `proto/pathlockd.proto` in sync with the daemon's contract; the client
loads it at runtime, so a stale copy silently drops new fields.

## License

[AGPL-3.0-or-later](LICENSE).
