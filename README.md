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
installing from a Git ref needs no build step. Linux **x86_64** only.

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
  if (e.type === 'revoke' || e.type === 'killed') {
    // stop using the lock and release it
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
| `release(ownerId, requests, delWaitKey?)` | `void` |
| `releaseAll(ownerId, delWaitKey?)` | `void` |
| `renew(ownerId, ttlMs)` | `RenewResult` — `status: 'ok' \| 'lost'` |
| `forceRelease(victimId)` | `void` |
| `assertFencing(ownerId, fencingToken, paths)` | `AssertResult` — `status: 'ok' \| 'fail'` |
| `detectCycle(startOwnerId, maxDepth)` | `CycleResult` — `kind: 'none' \| 'cycle' \| 'truncated'` |
| `isBlocking(path, owner, reason)` | `boolean` |
| `incrFencingToken()` | `number` |
| `setWaitEdge(ownerId, conflictOwner, ttlMs, metadata?)` | `void` |
| `clearWaitEdge(ownerId)` | `void` |
| `isOwnerAlive(ownerId)` | `boolean` |
| `requestRevoke(ownerId)` | `void` |
| `subscribe(ownerId)` | `PathlockdSubscription` (typed EventEmitter) |
| `health()` | `{ ok, detail }` |
| `waitForReady(timeoutMs?)` / `close()` | — |

All request/result shapes are exported types (`AcquireParams`, `AcquireResult`,
`LockRequest`, `LockMode`, `LockState`, `RenewResult`, `AssertResult`,
`CycleResult`, `LockEvent`, …).

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

`PathlockdDebugClient` (test-only; requires `PATHLOCKD_ENABLE_DEBUG=1` on the
daemon) exposes `flush`, `expireOwner`, `deleteLockKey`, `setWriteOwner`,
`getWriteOwner`, `setFence`, `getFence`, `setFencingCounter`,
`getFencingCounter`, `ownedPaths` for fault-injection tests.

## Development

```bash
npm install
npm run build   # tsc -> dist/
```

Keep `proto/pathlockd.proto` in sync with the daemon's contract; the client
loads it at runtime, so a stale copy silently drops new fields.

## License

[AGPL-3.0-or-later](LICENSE).
