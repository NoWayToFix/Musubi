import { defineConfig } from 'tsup';

/**
 * Build three formats:
 *  - ESM (dist/index.mjs)            — modern bundlers / Node ESM / Deno / Bun
 *  - CJS (dist/index.cjs)            — Node 18/20 (with `ws`), Electron main/preload
 *  - IIFE (dist/index.global.js)     — browser <script>, globalName `Hub`
 *
 * `ws` is intentionally NOT statically imported anywhere in src/ — the
 * WebSocketTransport loads it dynamically at runtime (globalThis.WebSocket
 * first, then require/import of 'ws'), so all three bundles stay browser-clean.
 */
export default defineConfig({
  entry: {
    index: 'src/index.ts',
    electron: 'src/electron.ts'
  },
  format: ['esm', 'cjs', 'iife'],
  globalName: 'Hub',
  dts: true,
  sourcemap: true,
  clean: true,
  target: 'es2020',
  treeshake: true,
  splitting: false,
  outExtension({ format }) {
    if (format === 'iife') return { js: '.global.js' };
    return { js: format === 'esm' ? '.mjs' : '.cjs' };
  }
});