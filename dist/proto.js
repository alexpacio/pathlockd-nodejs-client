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
exports.SET_CLAIM_STATUS_FROM_WIRE = exports.EVENT_TYPE_FROM_WIRE = exports.CYCLE_KIND_FROM_WIRE = exports.ASSERT_STATUS_FROM_WIRE = exports.RENEW_STATUS_FROM_WIRE = exports.ACQUIRE_STATUS_FROM_WIRE = exports.STATE_TO_WIRE = exports.MODE_FROM_WIRE = exports.MODE_TO_WIRE = exports.PROTO_PATH = void 0;
exports.loadPathlockdProto = loadPathlockdProto;
exports.decodeWireEnum = decodeWireEnum;
exports.toWireUint64 = toWireUint64;
exports.toWirePositiveUint64 = toWirePositiveUint64;
exports.wireInt64ToBigInt = wireInt64ToBigInt;
exports.bigintToWireInt64 = bigintToWireInt64;
exports.buildCredentials = buildCredentials;
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
exports.ACQUIRE_STATUS_FROM_WIRE = {
    ACQUIRE_STATUS_OK: 'ok',
    ACQUIRE_STATUS_CONFLICT: 'conflict',
    ACQUIRE_STATUS_LOST: 'lost',
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
    EVENT_TYPE_RELEASED: 'released',
    EVENT_TYPE_KILLED: 'killed',
    EVENT_TYPE_REVOKE: 'revoke',
};
exports.SET_CLAIM_STATUS_FROM_WIRE = {
    SET_CLAIM_STATUS_OK: 'ok',
    SET_CLAIM_STATUS_HELD: 'held',
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
const INT64_MAX = 9223372036854775807n;
const INT64_MIN = -9223372036854775808n;
/**
 * Decode a wire int64 (kept as a `string` by the proto loader, see `longs: String`)
 * into a `bigint`. Fence values and fencing tokens are PD TSO timestamps that
 * routinely exceed `Number.MAX_SAFE_INTEGER`, so they must not pass through `Number`.
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
//# sourceMappingURL=proto.js.map