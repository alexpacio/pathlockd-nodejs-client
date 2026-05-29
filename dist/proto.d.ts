import * as grpc from '@grpc/grpc-js';
/** Bundled proto, resolved relative to the compiled output (dist/ -> ../proto). */
export declare const PROTO_PATH: string;
/** Load (once) and return the `pathlockd.v1` package namespace. */
export declare function loadPathlockdProto(): any;
export declare const MODE_TO_WIRE: Record<string, string>;
export declare const STATE_TO_WIRE: Record<string, string>;
export declare const ACQUIRE_STATUS_FROM_WIRE: Record<string, string>;
export declare const RENEW_STATUS_FROM_WIRE: Record<string, string>;
export declare const ASSERT_STATUS_FROM_WIRE: Record<string, string>;
export declare const CYCLE_KIND_FROM_WIRE: Record<string, string>;
export declare const EVENT_TYPE_FROM_WIRE: Record<string, string>;
export declare function buildCredentials(tls: boolean): grpc.ChannelCredentials;
//# sourceMappingURL=proto.d.ts.map