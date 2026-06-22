"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_CHANNEL_OPTIONS = exports.EVENT_TYPE_FROM_WIRE = exports.CYCLE_KIND_FROM_WIRE = exports.ASSERT_STATUS_FROM_WIRE = exports.RENEW_STATUS_FROM_WIRE = exports.REASON_FROM_WIRE = exports.REASON_TO_WIRE = exports.ACQUIRE_STATUS_FROM_WIRE = exports.LOCK_ALGORITHM_FROM_WIRE = exports.LOCK_ALGORITHM_TO_WIRE = exports.STATE_TO_WIRE = exports.MODE_FROM_WIRE = exports.MODE_TO_WIRE = exports.PROTO_PATH = void 0;
exports.loadPathlockdProto = loadPathlockdProto;
exports.decodeWireEnum = decodeWireEnum;
exports.toWireUint64 = toWireUint64;
exports.toWirePositiveUint64 = toWirePositiveUint64;
exports.toWireUint32 = toWireUint32;
exports.wireInt64ToBigInt = wireInt64ToBigInt;
exports.bigintToWireInt64 = bigintToWireInt64;
exports.buildCredentials = buildCredentials;
exports.buildChannelOptions = buildChannelOptions;
const path = __importStar(require("path"));
const grpc = __importStar(require("@grpc/grpc-js"));
const protoLoader = __importStar(require("@grpc/proto-loader"));
/** Bundled proto, resolved relative to the compiled output (dist/ -> ../proto). */
exports.PROTO_PATH = path.join(__dirname, '..', 'proto', 'pathlockd.proto');
let cached;
/** Load (once) and return the `pathlockd.v1` package namespace. */
function loadPathlockdProto() {
    if (cached)
        return cached;
    const def = protoLoader.loadSync(exports.PROTO_PATH, {
        keepCase: false, // camelCase fields: owner_id -> ownerId
        longs: String, // keep int64 exact; client validates before exposing as number
        enums: String, // enum values as their proto names
        defaults: true,
        oneofs: true,
    });
    const pkg = grpc.loadPackageDefinition(def);
    // proto-loader output is untyped; bridge it to PathlockdPackage at this boundary.
    cached = pkg.pathlockd.v1;
    return cached;
}
// --- enum <-> wire mappings (enums arrive/depart as their proto string names) ---
//
// Outbound maps are keyed by the public union (exhaustive). Inbound maps are
// keyed by `string`: the wire value is untrusted, so callers fall back to a
// default for any value not listed here.
exports.MODE_TO_WIRE = {
    write: 'MODE_WRITE',
    read: 'MODE_READ',
};
exports.MODE_FROM_WIRE = {
    MODE_WRITE: 'write',
    MODE_READ: 'read',
};
exports.STATE_TO_WIRE = {
    new: 'LOCK_STATE_NEW',
    held: 'LOCK_STATE_HELD',
};
exports.LOCK_ALGORITHM_TO_WIRE = {
    recursive_rw: 'LOCK_ALGORITHM_RECURSIVE_RW',
    point_rw: 'LOCK_ALGORITHM_POINT_RW',
    recursive_write: 'LOCK_ALGORITHM_RECURSIVE_WRITE',
    point_write: 'LOCK_ALGORITHM_POINT_WRITE',
    semaphore: 'LOCK_ALGORITHM_SEMAPHORE',
};
exports.LOCK_ALGORITHM_FROM_WIRE = {
    LOCK_ALGORITHM_RECURSIVE_RW: 'recursive_rw',
    LOCK_ALGORITHM_POINT_RW: 'point_rw',
    LOCK_ALGORITHM_RECURSIVE_WRITE: 'recursive_write',
    LOCK_ALGORITHM_POINT_WRITE: 'point_write',
    LOCK_ALGORITHM_SEMAPHORE: 'semaphore',
};
exports.ACQUIRE_STATUS_FROM_WIRE = {
    ACQUIRE_STATUS_OK: 'ok',
    ACQUIRE_STATUS_CONFLICT: 'conflict',
    ACQUIRE_STATUS_LOST: 'lost',
    ACQUIRE_STATUS_QUEUED: 'queued',
};
exports.REASON_TO_WIRE = {
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
exports.REASON_FROM_WIRE = {
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
exports.RENEW_STATUS_FROM_WIRE = {
    RENEW_STATUS_OK: 'ok',
    RENEW_STATUS_LOST: 'lost',
};
exports.ASSERT_STATUS_FROM_WIRE = {
    ASSERT_STATUS_OK: 'ok',
    ASSERT_STATUS_FAIL: 'fail',
};
exports.CYCLE_KIND_FROM_WIRE = {
    CYCLE_KIND_NONE: 'none',
    CYCLE_KIND_FOUND: 'cycle',
    CYCLE_KIND_TRUNCATED: 'truncated',
};
exports.EVENT_TYPE_FROM_WIRE = {
    EVENT_TYPE_KILLED: 'killed',
    EVENT_TYPE_REVOKE: 'revoke',
    EVENT_TYPE_GRANT: 'grant',
};
function decodeWireEnum(values, value, fieldName) {
    if (typeof value !== 'string') {
        throw new Error(`Unknown ${fieldName} enum value: ${String(value)}`);
    }
    const decoded = values[value];
    if (decoded === undefined) {
        throw new Error(`Unknown ${fieldName} enum value: ${JSON.stringify(value)}`);
    }
    return decoded;
}
function toWireUint64(value, fieldName) {
    if (!Number.isSafeInteger(value) || value < 0) {
        throw new Error(`${fieldName} must be a non-negative safe integer`);
    }
    return String(value);
}
function toWirePositiveUint64(value, fieldName) {
    if (!Number.isSafeInteger(value) || value <= 0) {
        throw new Error(`${fieldName} must be a positive safe integer`);
    }
    return String(value);
}
const UINT32_MAX = 4294967295;
function toWireUint32(value, fieldName) {
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
function wireInt64ToBigInt(value, fieldName) {
    try {
        if (typeof value === 'bigint')
            return value;
        if (typeof value === 'number' && Number.isInteger(value))
            return BigInt(value);
        if (typeof value === 'string' && /^-?\d+$/.test(value))
            return BigInt(value);
    }
    catch {
        // fall through to the thrown error below
    }
    throw new Error(`${fieldName} is not a valid int64: ${String(value)}`);
}
/** Encode a `bigint` as a wire int64 string, validating the full int64 range. */
function bigintToWireInt64(value, fieldName) {
    if (typeof value !== 'bigint' || value < INT64_MIN || value > INT64_MAX) {
        throw new Error(`${fieldName} must be an int64`);
    }
    return value.toString();
}
function buildCredentials(tls) {
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
exports.DEFAULT_CHANNEL_OPTIONS = Object.freeze({
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
function buildChannelOptions(overrides) {
    return { ...exports.DEFAULT_CHANNEL_OPTIONS, ...(overrides ?? {}) };
}
//# sourceMappingURL=proto.js.map