#!/usr/bin/env node
// Node/Deno/Bun/Electron agent built on the TypeScript SDK Agent runtime.
//
//   cd sdk/ts && npm install && npm run build
//   node examples/agents/ts/agent.mjs --url http://127.0.0.1:8080 --id node-1
//
// The same SDK module works in the browser (CORS permitting): import Agent
// from '@musubi/hub-sdk' and call run() — platform.runtime becomes "browser".
import os from 'node:os';
import process from 'node:process';
import { Agent, AgentHandlerError, detectPlatform } from '../../../sdk/ts/dist/index.mjs';

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : fallback;
}

function csv(value) {
  return value.split(',').map((s) => s.trim()).filter(Boolean);
}

const id = arg('id', 'node-agent-1');
const agent = new Agent({
  url: arg('url', 'http://127.0.0.1:8080'),
  token: arg('token', undefined),
  nodeId: id,
  name: arg('name', id),
  capabilities: csv(arg('capabilities', 'echo,sys.info,sleep,fail')),
  tags: csv(arg('tags', 'agent,node')),
  pullWaitMs: Number(arg('pull-wait', '5000')),
  concurrency: Number(arg('concurrency', '2')),
  heartbeatMs: Number(arg('heartbeat', '10000')),
  onError: (err) => console.error('agent:', err)
});

agent
  .handle('sys.info', async () => ({
    node_id: agent.nodeID,
    host: os.hostname(),
    platform: os.platform(),
    arch: os.arch(),
    release: os.release(),
    pid: process.pid,
    node_version: process.version,
    cpu_count: os.cpus().length
  }))
  .handle('sleep', async (params) => {
    const ms = Number(params.ms ?? 0);
    await new Promise((resolve) => setTimeout(resolve, ms));
    return { slept_ms: ms };
  })
  .handle('fail', async (params) => {
    throw new AgentHandlerError(-32603, params.message ?? 'requested failure');
  });

const stop = () => agent.stop();
process.on('SIGINT', stop);
process.on('SIGTERM', stop);

console.log('ts agent starting:', id, 'platform:', detectPlatform());
agent
  .run()
  .then(() => console.log('agent stopped'))
  .catch((err) => {
    console.error('agent failed:', err);
    process.exitCode = 1;
  });
