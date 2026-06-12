# pathlockd-nodejs-client

Typed **Node.js / TypeScript** gRPC client for
[`pathlockd`](https://github.com/alexpacio/pathlockd) — fast, scalable,
opinionated path-based distributed locking primitives over TiKV.

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

// 1) allocate a monotonic fencing token for this owner
const fencingToken = await client.incrFencingToken();

// 2) acquire a write lock on the subtree "google_drive:/a/b"
const res = await client.acquire({
  ownerId: 'owner-1',
  ttlMs: 10_000,
  fencingToken,
  requests: [{ path: 'google_drive:/a/b', mode: 'write', state: 'new' }],
});

if (res.status === 'ok') {
  // ...do work, renewing periodically so the lease never lapses...
  await client.renew('owner-1', 10_000);
} else if (res.status === 'conflict') {
  console.log(`blocked by ${res.owner} on ${res.path} (${res.reason})`);
}

// 3) before an external side effect, prove you still own the path at your token
const check = await client.assertFencing('owner-1', fencingToken, ['google_drive:/a/b']);
if (check.status === 'fail') throw new Error(`lost the lock: ${check.reason}`);

// 4) release everything when done
await client.releaseAll('owner-1', true);
client.close();
```

## Per-owner events

A subscription is bound to a single owner and only ever surfaces that owner's
lifecycle events — its cooperative `revoke`, a forced `kill`, or its own
`released`. Use it to react when something asks your lock to yield:

```ts
const sub = client.subscribe('owner-1');
sub.on('event', (e) => {
  // e.type is 'released' | 'killed' | 'revoke'; e.ownerId === 'owner-1'
  if (e.type === 'revoke') {
    // Cooperative preemption: finish the in-flight unit of work ASAP, then
    // release (e.g. releaseAll(ownerId, true)).
  } else if (e.type === 'killed') {
    // Forced preemption: your locks are ALREADY gone and your fencing token is
    // now stale. Stop touching the backing store immediately and abort — do NOT
    // keep working, and there is nothing to release (a late write would be
    // rejected by the new holder's AssertFencing anyway).
  }
});
sub.on('error', (err) => console.error(err)); // attach this — EventEmitter throws otherwise
// later:
sub.close();
```

> A subscription never carries information about other owners. To learn that a
> lock you're *waiting on* has freed up, re-check with `isBlocking(...)` — that's
> how you drive contention progress.

## API

`new PathlockdClient({ endpoint, tls?, channelOptions? })`

| Method | Returns |
|---|---|
| `acquire(params)` | `AcquireResult` — `status: 'ok' \| 'conflict' \| 'lost'` |
| `release(ownerId, requests, delWaitKeyOrOptions?)` | `void` |
| `releaseAll(ownerId, delWaitKeyOrOptions?)` | `void` |
| `renew(ownerId, ttlMs, options?)` | `RenewResult` — `status: 'ok' \| 'lost'` |
| `forceRelease(victimId, options?)` | `void` |
| `assertFencing(ownerId, fencingToken, paths)` | `AssertResult` — `status: 'ok' \| 'fail'` |
| `detectCycle(startOwnerId, maxDepth)` | `CycleResult` — `kind: 'none' \| 'cycle' \| 'truncated'` |
| `isBlocking(path, owner, reason)` | `boolean` |
| `incrFencingToken(options?)` | `bigint` (PD-TSO token; exact beyond 2^53) |
| `setWaitEdge(ownerId, conflictOwner, ttlMs, metadata?, options?)` | `void` |
| `clearWaitEdge(ownerId, options?)` | `void` |
| `setClaim(path, claimantOwnerId, ttlMsOrOptions?)` | `SetClaimResult` — `status: 'ok' \| 'held'` |
| `clearClaim(path, claimantOwnerId, options?)` | `void` |
| `isOwnerAlive(ownerId)` | `boolean` |
| `requestRevoke(ownerId, claim?)` | `void` |
| `inspectPath(path)` | `PathLockInfo` — write owner, read owners, fence, claim |
| `listOwnerLocks(ownerId)` | `OwnerLocksResult` — `{ alive, locks }` |
| `dumpLocks({ ownerPage?, maxEntries? })` | `LockEntry[]` — every live lock, auto-paginated |
| `dumpLocksPages(ownerPage?)` | `AsyncGenerator<LockEntry[]>` — stream the dump one page at a time |
| `subscribe(ownerId)` | `PathlockdSubscription` (typed EventEmitter) |
| `health()` | `{ ok, detail }` |
| `waitForReady(timeoutMs?)` / `close()` | — |

All request/result shapes are exported types (`AcquireParams`, `AcquireResult`,
`LockRequest`, `LockMode`, `LockState`, `RenewResult`, `AssertResult`,
`CycleResult`, `LockEvent`, …).

Mutating RPCs that support daemon-side apply-once semantics accept
`idempotencyKey` either in their params object (`acquire`) or an optional
options object. For `renew`, `options.domains` can also target the renew fan-out
at the routing domains where the owner holds locks:

```ts
await client.renew('owner-1', 10_000, {
  domains: ['google_drive'],
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
can be reported as deadlock cycles. Write acquires and non-empty fencing asserts
also require a positive safe-integer fencing token; int64 responses are decoded
exactly and rejected if they exceed JavaScript's safe integer range.

### Inspection

Three read-only calls expose live lock state for operators and tooling. They
filter by owner liveness (so they reflect what would actually block) and never
mutate daemon state:

```ts
// Path-centric: who holds this exact path?
const info = await client.inspectPath('google_drive:/a/b');
// info.writeOwner, info.readOwners[], info.fence (bigint|null), info.claimOwner

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
