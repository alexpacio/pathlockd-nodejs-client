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
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.PROTO_PATH = exports.PathlockdSubscription = exports.PathlockdClient = void 0;
__exportStar(require("./types"), exports);
var client_1 = require("./client");
Object.defineProperty(exports, "PathlockdClient", { enumerable: true, get: function () { return client_1.PathlockdClient; } });
Object.defineProperty(exports, "PathlockdSubscription", { enumerable: true, get: function () { return client_1.PathlockdSubscription; } });
var proto_1 = require("./proto");
Object.defineProperty(exports, "PROTO_PATH", { enumerable: true, get: function () { return proto_1.PROTO_PATH; } });
//# sourceMappingURL=index.js.map