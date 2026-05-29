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
exports.EVENT_TYPE_FROM_WIRE = exports.CYCLE_KIND_FROM_WIRE = exports.ASSERT_STATUS_FROM_WIRE = exports.RENEW_STATUS_FROM_WIRE = exports.ACQUIRE_STATUS_FROM_WIRE = exports.STATE_TO_WIRE = exports.MODE_TO_WIRE = exports.PROTO_PATH = void 0;
exports.loadPathlockdProto = loadPathlockdProto;
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
        longs: Number, // int64 (fencing token) as JS number — safe well past 2^53 here
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
function buildCredentials(tls) {
    return tls ? grpc.credentials.createSsl() : grpc.credentials.createInsecure();
}
//# sourceMappingURL=proto.js.map