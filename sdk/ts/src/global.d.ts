/**
 * Minimal ambient declaration for the dynamic `require('ws')` /
 * `require('electron')` fallbacks (transport.ts/websocket.ts, electron.ts).
 *
 * We deliberately do NOT depend on @types/node so this package stays
 * isomorphic and builds cleanly for browsers too. The declaration only exists
 * for TypeScript; at runtime these calls are guarded by typeof/try-catch.
 */
declare function require(id: string): any;