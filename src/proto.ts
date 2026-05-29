import * as path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

/** Bundled proto, resolved relative to the compiled output (dist/ -> ../proto). */
export const PROTO_PATH = path.join(__dirname, '..', 'proto', 'pathlockd.proto');

let cached: any;

/** Load (once) and return the `pathlockd.v1` package namespace. */
export function loadPathlockdProto(): any {
  if (cached) return cached;
  const def = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false, // camelCase fields: owner_id -> ownerId
    longs: Number, // int64 (fencing token) as JS number — safe well past 2^53 here
    enums: String, // enum values as their proto names
    defaults: true,
    oneofs: true,
  });
  const pkg = grpc.loadPackageDefinition(def) as any;
  cached = pkg.pathlockd.v1;
  return cached;
}

// --- enum <-> wire mappings (enums arrive/depart as their proto string names) ---

export const MODE_TO_WIRE: Record<string, string> = {
  write: 'MODE_WRITE',
  read: 'MODE_READ',
};

export const STATE_TO_WIRE: Record<string, string> = {
  new: 'LOCK_STATE_NEW',
  held: 'LOCK_STATE_HELD',
};

export const ACQUIRE_STATUS_FROM_WIRE: Record<string, string> = {
  ACQUIRE_STATUS_OK: 'ok',
  ACQUIRE_STATUS_CONFLICT: 'conflict',
  ACQUIRE_STATUS_LOST: 'lost',
};

export const RENEW_STATUS_FROM_WIRE: Record<string, string> = {
  RENEW_STATUS_OK: 'ok',
  RENEW_STATUS_LOST: 'lost',
};

export const ASSERT_STATUS_FROM_WIRE: Record<string, string> = {
  ASSERT_STATUS_OK: 'ok',
  ASSERT_STATUS_FAIL: 'fail',
};

export const CYCLE_KIND_FROM_WIRE: Record<string, string> = {
  CYCLE_KIND_NONE: 'none',
  CYCLE_KIND_FOUND: 'cycle',
  CYCLE_KIND_TRUNCATED: 'truncated',
};

export const EVENT_TYPE_FROM_WIRE: Record<string, string> = {
  EVENT_TYPE_RELEASED: 'released',
  EVENT_TYPE_KILLED: 'killed',
  EVENT_TYPE_REVOKE: 'revoke',
};

export function buildCredentials(tls: boolean): grpc.ChannelCredentials {
  return tls ? grpc.credentials.createSsl() : grpc.credentials.createInsecure();
}
