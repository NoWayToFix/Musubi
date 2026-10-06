'use strict';

/**
 * Renderer process: uses window.hub (exposed by preload.js) to talk to the hub.
 *
 *   window.hub.invoke(method, params)  -> Promise (JSON-RPC via main process)
 *   window.hub.onEvent(cb)             -> hub events (pushed by main process)
 *   window.hub.onStatus(cb)            -> connection status changes
 *
 * Calls used: engine.info, node.list, event.subscribe, command.send
 * (echo/fail/sleep), task.create — matching the PROTOCOL.md method list.
 */

const $ = (id) => document.getElementById(id);

const els = {
  connStatus: $('conn-status'),
  nodesTable: $('nodes-table').querySelector('tbody'),
  nodeSelect: $('node-select'),
  methodInput: $('method-input'),
  paramsInput: $('params-input'),
  sendCommand: $('send-command'),
  cmdResult: $('cmd-result'),
  refreshNodes: $('refresh-nodes'),
  createTask: $('create-task'),
  taskResult: $('task-result'),
  taskProgress: $('task-progress'),
  taskProgressFill: $('task-progress-fill'),
  taskProgressText: $('task-progress-text'),
  log: $('log'),
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function logLine(text) {
  const line = document.createElement('div');
  const now = new Date().toISOString().substring(11, 23);
  line.textContent = `[${now}] ${text}`;
  els.log.prepend(line);
  // Keep the log bounded.
  while (els.log.children.length > 200) els.log.lastChild.remove();
}

function setStatus(connected, extra) {
  els.connStatus.textContent = connected ? 'connected' : 'offline';
  els.connStatus.className = 'status ' + (connected ? 'online' : 'offline');
  if (extra) logLine(extra);
}

function nodeIdOf(ev) {
  return ev && (ev.node_id ?? ev.nodeId ?? '');
}

// ---------------------------------------------------------------------------
// node table + command form
// ---------------------------------------------------------------------------

function renderNodes(nodes) {
  els.nodesTable.innerHTML = '';
  els.nodeSelect.innerHTML = '';
  if (!nodes || nodes.length === 0) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="4">no nodes registered</td>';
    els.nodesTable.appendChild(tr);
    return;
  }
  for (const n of nodes) {
    const tr = document.createElement('tr');
    const caps = (n.capabilities || []).join(', ');
    tr.innerHTML = `<td>${n.id}</td><td>${n.name}</td><td>${n.status}</td><td>${caps}</td>`;
    els.nodesTable.appendChild(tr);

    const opt = document.createElement('option');
    opt.value = n.id;
    opt.textContent = `${n.name} (${n.id})`;
    els.nodeSelect.appendChild(opt);
  }
}

async function refreshNodes() {
  try {
    const res = await window.hub.invoke('node.list', { limit: 100 });
    const nodes = res.nodes || [];
    renderNodes(nodes);
    logLine(`node.list: ${nodes.length} node(s)`);
  } catch (err) {
    logLine(`node.list failed: ${err.message ?? err}`);
  }
}

// ---------------------------------------------------------------------------
// commands
// ---------------------------------------------------------------------------

async function sendCommand() {
  const nodeId = els.nodeSelect.value;
  const method = els.methodInput.value.trim() || 'echo';
  let params = {};
  try {
    params = els.paramsInput.value.trim() ? JSON.parse(els.paramsInput.value) : {};
  } catch {
    logLine('params JSON is invalid — sending {}');
  }

  els.cmdResult.textContent = 'sending…';
  try {
    const res = await window.hub.invoke('command.send', {
      node_id: nodeId,
      method,
      params,
      timeout: 30000,
    });
    const cmd = res.command || res;
    els.cmdResult.textContent = JSON.stringify(cmd, null, 2);
    logLine(`command.send ${method} -> ${cmd.id} (${cmd.status})`);
  } catch (err) {
    const msg = err && err.code ? `hub error ${err.code}: ${err.message}` : String(err.message || err);
    els.cmdResult.textContent = msg;
    logLine(`command.send failed: ${msg}`);
  }
}

// ---------------------------------------------------------------------------
// tasks
// ---------------------------------------------------------------------------

async function createTask() {
  // Build one command per known node.
  const options = [...els.nodeSelect.options].map((o) => o.value);
  if (options.length === 0) {
    logLine('task.create: no nodes available');
    return;
  }
  const commands = options.map((nodeId) => ({
    node_id: nodeId,
    method: 'echo',
    params: { text: 'task batch' },
  }));
  try {
    const res = await window.hub.invoke('task.create', {
      name: 'electron-demo',
      commands,
      parallel: true,
    });
    const task = res.task || res;
    els.taskResult.textContent = JSON.stringify(task, null, 2);
    els.taskProgress.style.display = 'block';
    setTaskProgress((task.progress ?? 0) * 100, task.status);
    logLine(`task.create -> ${task.id} (${task.status})`);
  } catch (err) {
    els.taskResult.textContent = String(err.message || err);
    logLine(`task.create failed: ${err.message ?? err}`);
  }
}

function setTaskProgress(percent, status) {
  els.taskProgressFill.style.width = `${Math.max(0, Math.min(100, percent))}%`;
  els.taskProgressText.textContent = `${Math.round(percent)}% — ${status || ''}`;
}

// ---------------------------------------------------------------------------
// events
// ---------------------------------------------------------------------------

function handleEvent(ev) {
  // Ignore unknown event types (PROTOCOL.md §4.5).
  const type = ev.type || 'unknown';
  const nodeId = ev.node_id || ev.nodeId || '';
  const taskId = ev.task_id || ev.taskId || '';
  const commandId = ev.command_id || ev.commandId || '';

  logLine(`event ${type} node=${nodeId} task=${taskId} cmd=${commandId}`);

  // Progress bar follows task.progress events.
  if (type === 'task.progress' && ev.data && typeof ev.data.progress === 'number') {
    setTaskProgress(ev.data.progress * 100, 'running');
  }
  if (type === 'task.completed' || type === 'task.failed' || type === 'task.cancelled') {
    setTaskProgress(type === 'task.completed' ? 100 : 0, type.replace('task.', ''));
  }
}

// ---------------------------------------------------------------------------
// boot
// ---------------------------------------------------------------------------

window.hub.onStatus((status) => {
  setStatus(!!status.connected, status.error ? `connection error: ${status.error}` : null);
});
window.hub.onEvent(handleEvent);

(async function boot() {
  try {
    const info = await window.hub.invoke('engine.info', {
      client_info: { name: 'electron-renderer', version: '1.0.0' },
    });
    logLine(`engine.info: protocol ${info.protocol_version ?? info.protocolVersion}`);
  } catch (err) {
    logLine(`engine.info failed: ${err.message ?? err}`);
  }

  await refreshNodes();

  try {
    await window.hub.invoke('event.subscribe', {});
    logLine('event.subscribe: ok (all events)');
  } catch (err) {
    logLine(`event.subscribe failed: ${err.message ?? err} (events still stream via on("*"))`);
  }
})();

els.refreshNodes.addEventListener('click', refreshNodes);
els.sendCommand.addEventListener('click', sendCommand);
els.createTask.addEventListener('click', createTask);