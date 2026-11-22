'use strict';

// Musubi 集控中心。
// 零构建，同源，只用 fetch、WebSocket 和 SSE。
// 约定：所有动态文本走 esc()；不写内联样式，CSP 只允许外链资源。

const $ = (id) => document.getElementById(id);

const TABS = ['overview', 'platforms', 'nodes', 'seats', 'commands', 'tasks', 'events', 'rpc'];

const STATUS_LABELS = {
  online: '在线',
  busy: '忙碌',
  offline: '离线',
  unknown: '未知',
  standing: '常席',
  vacant: '虚席',
  running: '运行中',
  pending: '等待',
  accepted: '已受理',
  succeeded: '成功',
  failed: '失败',
  cancelled: '已取消',
  timeout: '超时',
  partial: '部分完成',
};

const CODE_AUTH_INVALID = -32002;
const AUTH_CODES = [-32001, CODE_AUTH_INVALID, -32003, -32004];

// 四张清单各自记录自己的同步状态，任何一张失败都不该拖垮其他页。
const LIST_KEYS = ['nodes', 'seats', 'commands', 'tasks'];
const LIST_LABELS = { nodes: '节点', seats: '席位', commands: '指令', tasks: '任务' };

const state = {
  access: '',
  connected: false,
  connecting: false,
  epoch: 0,
  authKind: '',
  info: null,
  tab: 'overview',
  ws: null,
  sse: null,
  streamRetry: null,
  streamAttempts: 0,
  nodes: [],
  seats: [],
  commands: [],
  tasks: [],
  lists: {
    nodes: { ok: false, error: '' },
    seats: { ok: false, error: '' },
    commands: { ok: false, error: '' },
    tasks: { ok: false, error: '' },
  },
  commandsTotal: 0,
  tasksTotal: 0,
  events: [],
  eventCount: 0,
  paused: false,
  lastSync: null,
  refreshTimer: null,
  refreshInFlight: null,
  refreshSeq: 0,
  refreshApplied: 0,
  seq: 0,
  inlineBind: null,
  pendingDelete: null,
  inlineFocus: null,
  focusInline: false,
  rowError: null,
  rowErrorTimer: null,
  pendingPlatform: '',
  pulse: [],
  pulseIndex: 0,
  pulseTimer: null,
  pulseBars: null,
  lastPulseEvent: 0,
};

// ---------------------------------------------------------------------------
// 基础工具

function esc(v) {
  return String(v === null || v === undefined ? '' : v).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

function pad(n) {
  return String(n).padStart(2, '0');
}

function asDate(ts) {
  if (!ts) return null;
  const d = new Date(ts);
  return isNaN(d.getTime()) ? null : d;
}

function fmtTime(ts) {
  if (!ts) return '—';
  const d = asDate(ts);
  if (!d) return String(ts);
  const now = new Date();
  const clock = pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  if (d.toDateString() === now.toDateString()) return clock;
  return pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes());
}

function fmtFull(ts) {
  const d = asDate(ts);
  if (!d) return String(ts || '—');
  return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) +
    ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
}

function isoTime(ts) {
  const d = asDate(ts);
  return d ? d.toISOString() : '';
}

function fmtClock(d) {
  return pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
}

function pretty(v) {
  try { return JSON.stringify(v, null, 2); } catch (e) { return String(v); }
}

function short(v, n) {
  const s = typeof v === 'string' ? v : pretty(v);
  const max = n || 100;
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function listText(arr, max) {
  if (!Array.isArray(arr) || !arr.length) return '—';
  const n = max || 4;
  const head = arr.slice(0, n).join(', ');
  return arr.length > n ? head + ' +' + (arr.length - n) : head;
}

function emptyRow(cols, text) {
  return '<tr><td colspan="' + cols + '" class="empty">' + esc(text) + '</td></tr>';
}

function emptyState(text, strong) {
  return '<div class="empty-state">' + (strong ? '<strong>' + esc(strong) + '</strong> ' : '') + esc(text) + '</div>';
}

function statusClass(status) {
  const s = String(status || 'unknown');
  return ['online', 'busy', 'offline', 'unknown', 'standing', 'vacant', 'running', 'pending',
    'accepted', 'succeeded', 'failed', 'cancelled', 'timeout', 'partial'].indexOf(s) >= 0
    ? 's-' + s : 's-unknown';
}

function statusMark(status) {
  return '<span class="status-mark ' + statusClass(status) + '" aria-hidden="true"></span>';
}

function statusPair(status) {
  const s = String(status || 'unknown');
  const label = STATUS_LABELS[s];
  const title = label ? label + ' · ' + s : s;
  const alarm = (s === 'failed' || s === 'timeout' || s === 'partial') ? ' s-' + s : '';
  return '<span class="status-pair" title="' + esc(title) + '">' +
    statusMark(s) + '<span class="status-word' + alarm + '">' + esc(label || s) + '</span></span>';
}

function isOnline(n) {
  return n.status === 'online' || n.status === 'busy';
}

function isAuthError(e) {
  return !!e && AUTH_CODES.indexOf(e.code) >= 0;
}

function parseJSON(raw, emptyValue) {
  const text = String(raw || '').trim();
  if (!text) return { ok: true, value: emptyValue };
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

function setResultEl(el, text, kind) {
  if (!el) return;
  el.textContent = text || '';
  el.classList.remove('ok', 'err');
  el.classList.add('result');
  if (kind) el.classList.add(kind);
}

function setResult(id, text, kind) {
  setResultEl($(id), text, kind);
}

function setButtonBusy(btn, busy) {
  if (!btn) return;
  btn.disabled = !!busy;
  btn.setAttribute('aria-busy', busy ? 'true' : 'false');
}

function setInvalid(el, invalid) {
  if (!el) return;
  if (invalid) el.setAttribute('aria-invalid', 'true');
  else el.removeAttribute('aria-invalid');
}

// ---------------------------------------------------------------------------
// JSON-RPC

async function rpc(method, params) {
  const body = { jsonrpc: '2.0', id: ++state.seq, method: method, params: params || {} };
  const headers = { 'Content-Type': 'application/json' };
  if (state.access) headers.Authorization = 'Bearer ' + state.access;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 15000);
  let res;
  try {
    res = await fetch('/v1/rpc', {
      method: 'POST',
      headers: headers,
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
  } catch (e) {
    if (e && e.name === 'AbortError') throw new Error('请求超时（15 秒）');
    throw e;
  } finally {
    clearTimeout(timer);
  }
  if (res.status === 204) return null;
  let data;
  try {
    data = await res.json();
  } catch (e) {
    throw new Error('HTTP ' + res.status + '：响应不是 JSON');
  }
  if (data.error) {
    const err = new Error(data.error.message || 'rpc error');
    err.code = data.error.code;
    err.data = data.error.data;
    throw err;
  }
  return data.result;
}

// ---------------------------------------------------------------------------
// 连接与鉴权

function setConnection(kind, label) {
  const square = $('status');
  square.className = 'status-square ' + (kind === 'on' ? 'on' : kind === 'wait' ? 'wait' : 'off');
  $('status-text').textContent = label;
}

function setTransport(label) {
  $('transport-label').textContent = label;
  $('events-transport').textContent = label;
}

function setDatelineDetail(text, isError) {
  const el = $('dateline-detail');
  el.textContent = text;
  el.classList.toggle('err', !!isError);
  el.title = text;
}

function setPanelOpen(open, opts) {
  const panel = $('connect-panel');
  const wasOpen = !panel.hidden;
  panel.hidden = !open;
  $('connect-toggle').setAttribute('aria-expanded', open ? 'true' : 'false');
  $('connect-toggle').textContent = open ? '收起' : '连接';
  if (open && !wasOpen) {
    $('disconnect').hidden = !state.connected;
    if (opts && opts.focus) {
      setTimeout(() => {
        const target = state.connected && !$('disconnect').hidden ? $('disconnect') : $('token');
        if (target && !target.hidden) target.focus();
      }, 0);
    }
  }
}

function resetInfoPanel() {
  const pre = $('info-json');
  pre.hidden = true;
  pre.textContent = '连接后显示';
  $('info-toggle').setAttribute('aria-expanded', 'false');
  $('info-toggle').textContent = '展开 engine.info';
}

// 重新连接失败时，页头要回到仍然有效的那次连接，而不是停在“连接中”。
function restoreConnectedChrome() {
  $('engine-meta').textContent = state.info
    ? state.info.server.name + ' ' + state.info.server.version + ' · protocol ' + state.info.protocol_version
    : '已连接';
  setConnection('on', '已连接');
  setDatelineDetail(state.lastSync ? '最近同步 ' + fmtClock(state.lastSync) : '已连接');
}

async function login(secret, username) {
  if (secret) {
    try {
      const r = await rpc('auth.login', { token: secret });
      return { kind: 'token', access: r.access_token };
    } catch (e) {
      if (!username) throw e;
      const r = await rpc('auth.login', { username: username, password: secret });
      return { kind: 'basic', access: r.access_token };
    }
  }
  const r = await rpc('auth.login', {});
  return { kind: 'anonymous', access: r.access_token };
}

async function connect(opts) {
  const auto = !!(opts && opts.auto);
  if (state.connecting) return;
  state.connecting = true;

  const secret = $('token').value.trim();
  const username = $('username').value.trim();
  const prev = { access: state.access, connected: state.connected, kind: state.authKind };
  const focusWasInPanel = $('connect-panel').contains(document.activeElement);

  setConnection('wait', '连接中');
  $('engine-meta').textContent = '连接中…';
  setDatelineDetail('等待 Core 响应');
  setResult('connect-result', '正在连接…', '');
  setButtonBusy($('connect'), true);

  let auth;
  try {
    auth = await login(secret, username);
  } catch (e1) {
    // 保存的凭证可能在切换模式后失效；再试一次匿名登录，确认 Core 是否已不再要求凭证。
    try {
      auth = await login('', '');
    } catch (e2) {
      state.connecting = false;
      setButtonBusy($('connect'), false);
      state.access = prev.access;
      state.connected = prev.connected;
      state.authKind = prev.kind;
      if (prev.connected) {
        restoreConnectedChrome();
        setResult('connect-result', '新凭证无效，仍保持原连接。', 'err');
        return;
      }
      const badSecret = !!secret && e1 && e1.code === CODE_AUTH_INVALID;
      const needsAuth = isAuthError(e1) || isAuthError(e2);
      setConnection('off', badSecret ? '凭证无效' : needsAuth ? '需要凭证' : '连接失败');
      $('engine-meta').textContent = needsAuth ? 'Musubi Core' : '无法连接 Core';
      $('auth-mode').textContent = 'offline';
      setDatelineDetail(badSecret ? '检查凭证后重试' : needsAuth ? '填写凭证后重试' : '连接失败：' + e2.message, !needsAuth && !badSecret);
      setResult('connect-result', badSecret
        ? '凭证无效。检查 Token 或用户名/密码后重试。'
        : needsAuth
          ? '此 Core 需要凭证。填写 Token；basic 模式再填用户名。'
          : '连接失败：' + e2.message, badSecret || !needsAuth ? 'err' : '');
      setPanelOpen(true, { focus: !auto });
      return;
    }
  }

  state.access = auth.access;
  state.authKind = auth.kind;

  let info;
  try {
    info = await rpc('engine.info', { client_info: { protocol_versions: ['1.0'] } });
  } catch (e) {
    state.connecting = false;
    setButtonBusy($('connect'), false);
    state.access = prev.access;
    state.connected = prev.connected;
    state.authKind = prev.kind;
    if (prev.connected) {
      restoreConnectedChrome();
    } else {
      setConnection('off', '连接失败');
      $('engine-meta').textContent = '无法连接 Core';
      $('auth-mode').textContent = 'offline';
      setDatelineDetail('engine.info 失败：' + e.message, true);
      setPanelOpen(true, { focus: !auto });
    }
    setResult('connect-result', '无法读取 engine.info：' + e.message, 'err');
    return;
  }

  state.info = info;
  state.connected = true;
  state.connecting = false;
  state.epoch++;
  state.refreshInFlight = null;
  state.lastSync = null;
  state.eventCount = 0;
  state.events = [];
  state.nodes = [];
  state.seats = [];
  state.commands = [];
  state.tasks = [];
  resetListState();
  state.streamAttempts = 0;
  state.inlineBind = null;
  state.pendingDelete = null;
  state.inlineFocus = null;
  state.focusInline = false;
  state.rowError = null;
  state.pendingPlatform = '';
  if (state.refreshTimer) {
    clearTimeout(state.refreshTimer);
    state.refreshTimer = null;
  }
  resetPulse();

  if (secret) {
    sessionStorage.setItem('musubi.token', secret);
    sessionStorage.setItem('musubi.user', username);
  } else {
    sessionStorage.removeItem('musubi.token');
    sessionStorage.removeItem('musubi.user');
  }

  setButtonBusy($('connect'), false);
  $('disconnect').hidden = false;
  setConnection('on', '已连接');
  $('engine-meta').textContent = info.server.name + ' ' + info.server.version + ' · protocol ' + info.protocol_version;
  $('auth-mode').textContent = auth.kind;
  $('footer-version').textContent = info.server.name + ' ' + info.server.version;
  $('info-json').textContent = pretty(info);
  setDatelineDetail('正在同步…');
  setResult('connect-result', '已连接', 'ok');
  setPanelOpen(false);
  if (!auto && focusWasInPanel) $('connect-toggle').focus();

  renderAll();
  closeEvents();
  openEvents();
  await refreshAll({ force: true });
  updateTargetList();
}

function resetFleet() {
  state.epoch++;
  state.access = '';
  state.connected = false;
  state.authKind = '';
  state.info = null;
  state.nodes = [];
  state.seats = [];
  state.commands = [];
  state.tasks = [];
  state.events = [];
  state.eventCount = 0;
  state.lastSync = null;
  resetListState();
  state.streamAttempts = 0;
  state.inlineBind = null;
  state.pendingDelete = null;
  state.inlineFocus = null;
  state.focusInline = false;
  state.rowError = null;
  state.pendingPlatform = '';
  if (state.refreshTimer) {
    clearTimeout(state.refreshTimer);
    state.refreshTimer = null;
  }
  if (state.rowErrorTimer) {
    clearTimeout(state.rowErrorTimer);
    state.rowErrorTimer = null;
  }
  resetPulse();
}

function handleAuthLoss(e) {
  if (!state.connected) return false;
  resetFleet();
  closeEvents();
  sessionStorage.removeItem('musubi.token');
  sessionStorage.removeItem('musubi.user');
  setConnection('off', '会话失效');
  $('engine-meta').textContent = '需要重新连接';
  $('auth-mode').textContent = 'offline';
  $('disconnect').hidden = true;
  $('footer-version').textContent = '—';
  resetInfoPanel();
  $('rpc-output').textContent = '—';
  $('rpc-meta').textContent = '响应';
  setDatelineDetail(e && e.message ? e.message : '会话已失效', true);
  setResult('connect-result', '会话已失效，请重新连接。', 'err');
  setPanelOpen(true, { focus: false });
  renderAll();
  return true;
}

function disconnect() {
  if (state.connecting) return;
  if (state.access) rpc('auth.logout', {}).catch(() => {});
  resetFleet();
  closeEvents();
  sessionStorage.removeItem('musubi.token');
  sessionStorage.removeItem('musubi.user');
  $('token').value = '';
  $('disconnect').hidden = true;
  $('engine-meta').textContent = '未连接';
  $('auth-mode').textContent = 'offline';
  $('footer-version').textContent = '—';
  resetInfoPanel();
  $('rpc-output').textContent = '—';
  $('rpc-meta').textContent = '响应';
  setConnection('off', '已断开');
  setDatelineDetail('等待连接');
  setResult('connect-result', '已断开。', '');
  setPanelOpen(true, { focus: true });
  renderAll();
}

// ---------------------------------------------------------------------------
// 事件脉搏：30 个 2 秒桶，只由真实到达的事件抬高；8 秒无事件后归零并停表。

const PULSE_BUCKETS = 30;
const PULSE_TICK_MS = 2000;
const PULSE_IDLE_MS = 8000;

function pulseLevel(n) {
  if (n <= 0) return 0;
  if (n === 1) return 1;
  if (n <= 3) return 2;
  if (n <= 6) return 3;
  return 4;
}

function renderPulse() {
  if (!state.pulseBars) return;
  state.pulseBars.forEach((bar, i) => {
    const n = state.pulse[i] || 0;
    const level = pulseLevel(n);
    bar.className = 'pulse-bar' + (level ? ' p' + level : '') +
      (i === state.pulseIndex && n > 0 ? ' current' : '');
  });
}

function buildPulse() {
  const host = $('event-pulse');
  if (!host) return;
  host.innerHTML = '<span class="pulse-bar"></span>'.repeat(PULSE_BUCKETS);
  state.pulseBars = Array.from(host.children);
  resetPulse();
}

function pulseTick() {
  if (Date.now() - state.lastPulseEvent >= PULSE_IDLE_MS) {
    resetPulse();
    return;
  }
  state.pulseIndex = (state.pulseIndex + 1) % PULSE_BUCKETS;
  state.pulse[state.pulseIndex] = 0;
  renderPulse();
}

function pulseAdd() {
  state.lastPulseEvent = Date.now();
  state.pulse[state.pulseIndex] = (state.pulse[state.pulseIndex] || 0) + 1;
  renderPulse();
  if (!state.pulseTimer) state.pulseTimer = setInterval(pulseTick, PULSE_TICK_MS);
}

function resetPulse() {
  if (state.pulseTimer) {
    clearInterval(state.pulseTimer);
    state.pulseTimer = null;
  }
  state.pulse = new Array(PULSE_BUCKETS).fill(0);
  state.pulseIndex = 0;
  state.lastPulseEvent = 0;
  renderPulse();
}

// ---------------------------------------------------------------------------
// 事件流

function closeEvents() {
  if (state.streamRetry) {
    clearTimeout(state.streamRetry);
    state.streamRetry = null;
  }
  if (state.ws) {
    try { state.ws.onclose = null; state.ws.close(); } catch (e) { /* ignore */ }
    state.ws = null;
  }
  if (state.sse) {
    try { state.sse.close(); } catch (e) { /* ignore */ }
    state.sse = null;
  }
  setTransport('未连接');
}

function scheduleStreamRetry() {
  if (!state.connected || state.streamRetry) return;
  const delay = Math.min(30000, 3000 * Math.pow(2, state.streamAttempts));
  state.streamAttempts++;
  state.streamRetry = setTimeout(() => {
    state.streamRetry = null;
    openEvents();
  }, delay);
}

function openEvents() {
  closeEvents();
  if (!state.connected) return;
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  let ws;
  try {
    ws = new WebSocket(proto + '://' + location.host + '/v1/ws');
  } catch (e) {
    openSSE();
    return;
  }
  state.ws = ws;
  const connectTimer = setTimeout(() => {
    if (state.ws === ws && ws.readyState !== WebSocket.OPEN) {
      try { ws.close(); } catch (e) { /* ignore */ }
    }
  }, 4000);

  ws.onopen = () => {
    clearTimeout(connectTimer);
    state.streamAttempts = 0;
    setTransport('WebSocket');
    const params = {};
    if (state.access) params.auth_token = state.access;
    ws.send(JSON.stringify({ jsonrpc: '2.0', id: ++state.seq, method: 'event.subscribe', params: params }));
  };
  ws.onmessage = (m) => {
    try {
      const msg = JSON.parse(m.data);
      if (msg.method === 'event' && msg.params && msg.params.event) handleEvent(msg.params.event);
    } catch (e) { /* ignore malformed frames */ }
  };
  ws.onerror = () => {
    try { ws.close(); } catch (e) { /* ignore */ }
  };
  ws.onclose = () => {
    clearTimeout(connectTimer);
    if (state.ws === ws) state.ws = null;
    if (state.connected) {
      setTransport('重连中');
      openSSE();
    }
  };
}

function openSSE() {
  if (state.sse || !state.connected || state.ws) return;
  const url = '/v1/events' + (state.access ? '?token=' + encodeURIComponent(state.access) : '');
  try {
    const es = new EventSource(url);
    state.sse = es;
    setTransport('SSE');
    es.onopen = () => {
      state.streamAttempts = 0;
      setTransport('SSE');
    };
    es.onmessage = (m) => {
      try {
        const msg = JSON.parse(m.data);
        if (msg.method === 'event' && msg.params && msg.params.event) handleEvent(msg.params.event);
      } catch (e) { /* ignore */ }
    };
    es.onerror = () => {
      try { es.close(); } catch (e) { /* ignore */ }
      if (state.sse === es) state.sse = null;
      if (state.connected) {
        setTransport('重连中');
        scheduleStreamRetry();
      }
    };
  } catch (e) {
    setTransport('事件流不可用');
    scheduleStreamRetry();
  }
}

function handleEvent(ev) {
  state.events.unshift(ev);
  if (state.events.length > 300) state.events.pop();
  state.eventCount++;
  pulseAdd();
  renderOverview();
  if (!state.paused) renderEvents();
  scheduleRefresh();
}

function scheduleRefresh() {
  if (state.inlineBind || state.pendingDelete) return;
  if (state.refreshTimer) return;
  state.refreshTimer = setTimeout(() => {
    state.refreshTimer = null;
    if (state.inlineBind || state.pendingDelete) return;
    refreshAll({ force: true });
  }, 300);
}

// ---------------------------------------------------------------------------
// 数据加载

function resetListState() {
  state.lists = {};
  for (const key of LIST_KEYS) state.lists[key] = { ok: false, error: '' };
  state.commandsTotal = 0;
  state.tasksTotal = 0;
}

// 清单各自的占位文案：加载中与失败分开，失败时说明哪张表没更新。
function listPlaceholder(key, what) {
  const err = state.lists[key].error;
  if (err) return what + '同步失败：' + err + '。点「刷新」重试。';
  return '正在同步' + what + '…';
}

function renderSyncBanner() {
  const el = $('sync-banner');
  if (!el) return;
  const failed = LIST_KEYS.filter((key) => state.lists[key].error);
  if (!state.connected || !failed.length) {
    el.hidden = true;
    $('sync-banner-label').textContent = '';
    $('sync-banner-text').textContent = '';
    return;
  }
  const allFailed = failed.length === LIST_KEYS.length;
  $('sync-banner-label').textContent = allFailed ? '同步失败' : '部分数据未更新';
  const last = state.lastSync ? '上次同步 ' + fmtClock(state.lastSync) + '。' : '';
  $('sync-banner-text').textContent = last +
    failed.map((key) => LIST_LABELS[key] + '：' + state.lists[key].error).join('；');
  el.hidden = false;
}

function refreshAll(opts) {
  if (!state.connected) return Promise.resolve(false);
  // 普通刷新可以复用进行中的请求；动作后的刷新必须拿到新快照。
  if (state.refreshInFlight && !(opts && opts.force)) return state.refreshInFlight;

  const seq = ++state.refreshSeq;
  const epoch = state.epoch;
  // 表格重建会丢掉按钮焦点；记下正在操作的那一行，重建后放回去。
  const active = document.activeElement;
  const focusBtn = active && active.matches && active.matches('button[data-act]')
    ? { act: active.dataset.act, id: active.dataset.id }
    : null;
  const focusLink = active && active.matches && active.matches('[data-platform-link]')
    ? active.dataset.platformLink
    : null;

  const task = (async () => {
    const results = await Promise.allSettled([
      rpc('node.list', { limit: 1000 }),
      rpc('seat.list', { limit: 1000 }),
      rpc('command.list', { limit: 200 }),
      rpc('task.list', { limit: 200 }),
    ]);

    const errors = [];
    const fresh = [];
    const statuses = {};
    let okCount = 0;
    let authErr = null;
    results.forEach((r, i) => {
      const key = LIST_KEYS[i];
      if (r.status === 'fulfilled') {
        okCount++;
        fresh[i] = r.value;
        statuses[key] = { ok: true, error: '' };
      } else if (isAuthError(r.reason)) {
        authErr = r.reason;
        statuses[key] = { ok: state.lists[key].ok, error: r.reason.message };
      } else {
        statuses[key] = { ok: state.lists[key].ok, error: r.reason.message };
        errors.push(LIST_LABELS[key] + '：' + r.reason.message);
      }
    });

    // 会话已经切换或断开时，旧请求的结果不能写进新的状态；更旧的刷新也不能覆盖新快照。
    if (epoch !== state.epoch) return false;
    if (seq < state.refreshApplied) return false;
    state.refreshApplied = seq;
    for (const key of LIST_KEYS) {
      if (statuses[key]) state.lists[key] = statuses[key];
    }

    if (fresh[0]) state.nodes = fresh[0].nodes || [];
    if (fresh[1]) state.seats = fresh[1].seats || [];
    if (fresh[2]) {
      state.commands = fresh[2].commands || [];
      const total = Number(fresh[2].total);
      state.commandsTotal = Number.isFinite(total) ? total : state.commands.length;
    }
    if (fresh[3]) {
      state.tasks = fresh[3].tasks || [];
      const total = Number(fresh[3].total);
      state.tasksTotal = Number.isFinite(total) ? total : state.tasks.length;
    }

    if (authErr) {
      handleAuthLoss(authErr);
      return false;
    }

    if (okCount > 0) state.lastSync = new Date();
    renderAll();

    if (focusBtn || focusLink) {
      const now = document.activeElement;
      if (!now || now === document.body) {
        let match = null;
        if (focusBtn) {
          match = Array.from(document.querySelectorAll('button[data-act]')).find((b) =>
            b.dataset.act === focusBtn.act && b.dataset.id === focusBtn.id);
        } else {
          match = Array.from(document.querySelectorAll('[data-platform-link]')).find((a) =>
            a.dataset.platformLink === focusLink);
        }
        if (match) match.focus({ preventScroll: true });
      }
    }

    if (okCount === 0) {
      setConnection(state.connected ? 'wait' : 'off', state.connected ? '同步异常' : '未连接');
      setDatelineDetail('同步失败：' + (errors[0] || '无法连接 Core'), true);
      return false;
    }
    setConnection('on', '已连接');
    if (errors.length) {
      setDatelineDetail('部分数据未更新：' + errors[0], true);
    } else {
      setDatelineDetail('最近同步 ' + fmtClock(state.lastSync));
    }
    return errors.length === 0;
  })();

  const wrapped = task.finally(() => {
    if (state.refreshInFlight === wrapped) state.refreshInFlight = null;
  });
  state.refreshInFlight = wrapped;
  return wrapped;
}

// ---------------------------------------------------------------------------
// 平台与概览

function platformOf(n) {
  return n.platform || {};
}

function platformMeta(n) {
  const p = platformOf(n);
  const parts = [];
  const osarch = [p.os, p.arch].filter(Boolean).join('/');
  if (osarch) parts.push(osarch);
  if (p.agent_version) parts.push('agent ' + p.agent_version);
  return parts.join(' · ');
}

function groupPlatforms() {
  const map = new Map();
  for (const n of state.nodes) {
    const p = platformOf(n);
    const key = p.runtime || 'unknown';
    if (!map.has(key)) {
      map.set(key, {
        runtime: key,
        nodes: [],
        osarch: new Set(),
        versions: new Set(),
        caps: new Set(),
      });
    }
    const g = map.get(key);
    g.nodes.push(n);
    const osarch = [p.os, p.arch].filter(Boolean).join('/');
    if (osarch) g.osarch.add(osarch);
    if (p.agent_version) g.versions.add(p.agent_version);
    for (const c of n.capabilities || []) g.caps.add(c);
  }
  return Array.from(map.values()).sort((a, b) =>
    (b.nodes.length - a.nodes.length) || a.runtime.localeCompare(b.runtime));
}

function setCount(id, value, known) {
  const el = $(id);
  el.textContent = known ? String(value) : '—';
  const valueEl = el.closest('.ledger-value') || el;
  valueEl.classList.toggle('is-empty', !known);
}

function setTableNote(id, text) {
  const el = $(id);
  if (!el) return;
  el.textContent = text || '';
  el.hidden = !text;
}

function renderCounts() {
  const nodesOk = state.connected && state.lists.nodes.ok;
  const seatsOk = state.connected && state.lists.seats.ok;
  const tasksOk = state.connected && state.lists.tasks.ok;
  const groups = groupPlatforms();
  const online = state.nodes.filter(isOnline).length;
  setCount('count-platforms', groups.length, nodesOk);
  setCount('count-nodes', state.nodes.length, nodesOk);
  setCount('count-online', online, nodesOk);
  setCount('count-nodes-denom', state.nodes.length, nodesOk);
  setCount('count-seats', state.seats.length, seatsOk);
  setCount('count-standing', state.seats.filter((s) => s.kind === 'standing').length, seatsOk);
  setCount('count-tasks', state.tasks.filter((t) => t.status === 'running').length, tasksOk);
  setCount('count-events', state.eventCount, state.connected);
}

function renderOverviewPlatforms() {
  const host = $('overview-platforms');
  if (!state.connected) {
    host.innerHTML = emptyState('连接后显示平台舰队。');
    return;
  }
  if (!state.lists.nodes.ok) {
    host.innerHTML = emptyState(listPlaceholder('nodes', '节点'));
    return;
  }
  const groups = groupPlatforms();
  if (!groups.length) {
    host.innerHTML = emptyState('还没有节点接入。运行一个 SDK Agent 或 POST /v1/register。');
    return;
  }
  const rows = groups.map((g) => {
    const online = g.nodes.filter(isOnline).length;
    const osarch = Array.from(g.osarch).join(' · ');
    const versions = Array.from(g.versions).join(', ');
    const runtime = g.runtime === 'unknown' ? '未上报' : g.runtime;
    return '<tr>' +
      '<td data-label="平台"><span class="cell-value"><a class="row-action" href="#nodes" data-platform-link="' + esc(g.runtime) + '">' + esc(runtime) + '</a></span></td>' +
      '<td class="num" data-label="在线"><span class="cell-value">' + online + ' / ' + g.nodes.length + '</span></td>' +
      '<td data-label="OS / 架构"><span class="cell-value">' + esc(osarch || '—') + '</span></td>' +
      '<td data-label="Agent"><span class="cell-value">' + esc(versions || '—') + '</span></td>' +
      '</tr>';
  }).join('');
  host.innerHTML = '<table class="data-table compact">' +
    '<thead><tr><th>平台</th><th class="num">在线</th><th>OS / 架构</th><th>Agent</th></tr></thead>' +
    '<tbody>' + rows + '</tbody></table>';
}

function renderOverviewEvents() {
  const host = $('overview-events');
  if (!state.connected) {
    host.innerHTML = emptyState('连接后显示事件流。');
    return;
  }
  if (!state.events.length) {
    host.innerHTML = emptyState('等待事件…');
    return;
  }
  const items = state.events.slice(0, 6).map((ev) => {
    const refs = [ev.node_id, ev.seat_id, ev.task_id, ev.command_id].filter(Boolean).join(' / ') || '—';
    const iso = isoTime(ev.time);
    return '<li>' +
      '<time' + (iso ? ' datetime="' + esc(iso) + '"' : '') + ' title="' + esc(fmtFull(ev.time)) + '">' + esc(fmtTime(ev.time)) + '</time>' +
      '<code>' + esc(ev.type || 'event') + '</code>' +
      '<span class="event-refs">' + esc(refs) + '</span>' +
      '</li>';
  }).join('');
  host.innerHTML = '<ol class="event-log">' + items + '</ol>';
}

function renderOverview() {
  renderCounts();
  renderOverviewPlatforms();
  renderOverviewEvents();
}

// ---------------------------------------------------------------------------
// 平台页

function renderPlatforms() {
  const host = $('platform-cards');
  if (!state.connected) {
    host.innerHTML = emptyState('连接后显示平台舰队。');
  } else if (!state.lists.nodes.ok) {
    host.innerHTML = emptyState(listPlaceholder('nodes', '节点'));
  } else if (!state.nodes.length) {
    host.innerHTML = emptyState('还没有节点上报平台。启动一个 SDK Agent 即可接入。');
  } else {
    const groups = groupPlatforms();
    host.innerHTML = groups.map((g) => {
      const online = g.nodes.filter(isOnline).length;
      const osarch = Array.from(g.osarch).join(' · ');
      const versions = Array.from(g.versions).join(', ');
      const caps = Array.from(g.caps);
      const meta = [osarch, versions.length ? 'agent ' + versions : ''].filter(Boolean).join(' · ');
      const chips = g.nodes.map((n) => {
        const p = platformOf(n);
        const title = [n.id, n.status || 'unknown', [p.os, p.arch].filter(Boolean).join('/')]
          .filter(Boolean).join(' · ');
        return '<span class="node-chip" title="' + esc(title) + '">' +
          statusMark(n.status) + esc(n.id) + '</span>';
      }).join('');
      const meterBlocks = g.nodes.slice(0, 16).map((n) => statusMark(n.status)).join('');
      const meterMore = g.nodes.length > 16 ? '<span class="fleet-more">+' + (g.nodes.length - 16) + '</span>' : '';
      const meterLabel = (g.runtime === 'unknown' ? '未上报' : g.runtime) + '，' + online + ' / ' + g.nodes.length + ' 在线';
      return '<article class="platform-sheet">' +
        '<div class="ps-head"><h3 class="ps-name">' + esc(g.runtime === 'unknown' ? '未上报' : g.runtime) + '</h3>' +
        '<span class="ps-count">' + online + ' / ' + g.nodes.length + ' 在线</span></div>' +
        (meta ? '<p class="ps-meta">' + esc(meta) + '</p>' : '<p class="ps-meta">平台信息未上报</p>') +
        '<div class="fleet-meter" role="img" aria-label="' + esc(meterLabel) + '">' + meterBlocks + meterMore + '</div>' +
        '<div class="ps-nodes chip-list">' + chips + '</div>' +
        '<p class="ps-caps">' + esc(caps.length ? listText(caps, 10) : '未上报能力') + '</p>' +
        '</article>';
    }).join('');
  }

  const sel = $('nodes-filter-runtime');
  if (!sel) return;
  const groups = groupPlatforms();
  // 选项没变就不重建，免得定时同步时把打开的下拉框和焦点一起冲掉。
  const sig = groups.map((g) => g.runtime + ':' + g.nodes.length).join('|');
  if (sel.dataset.sig === sig) return;
  sel.dataset.sig = sig;
  const prev = sel.value;
  sel.innerHTML = '<option value="">全部平台</option>' + groups.map((g) =>
    '<option value="' + esc(g.runtime) + '">' + esc(g.runtime) + ' (' + g.nodes.length + ')</option>').join('');
  sel.value = groups.some((g) => g.runtime === prev) ? prev : '';
}

// ---------------------------------------------------------------------------
// 节点页

function matchNode(n, q) {
  if (!q) return true;
  const p = platformOf(n);
  const hay = [
    n.id, n.name, p.runtime, p.os, p.arch, p.agent_version,
    (n.tags || []).join(' '), (n.capabilities || []).join(' '), n.seat_id,
  ].join(' ').toLowerCase();
  return hay.indexOf(q) >= 0;
}

function renderNodes() {
  const sel = $('nodes-filter-runtime');
  const runtime = sel ? sel.value : '';
  const q = ($('nodes-search').value || '').trim().toLowerCase();
  const nodes = state.nodes.filter((n) =>
    (!runtime || (platformOf(n).runtime || 'unknown') === runtime) && matchNode(n, q));

  const summary = $('nodes-summary');
  if (!state.connected) {
    summary.textContent = '等待连接';
  } else if (!state.lists.nodes.ok) {
    summary.textContent = state.lists.nodes.error ? '同步失败' : '正在同步';
  } else if (runtime || q) {
    summary.textContent = '显示 ' + nodes.length + ' / ' + state.nodes.length + ' 个节点';
  } else {
    summary.textContent = state.nodes.length + ' 个节点';
  }

  if (!state.connected) {
    $('nodes-body').innerHTML = emptyRow(7, '连接后显示节点。');
    return;
  }
  if (!state.lists.nodes.ok) {
    $('nodes-body').innerHTML = emptyRow(7, listPlaceholder('nodes', '节点'));
    return;
  }
  if (!nodes.length) {
    $('nodes-body').innerHTML = emptyRow(7, state.nodes.length ? '没有匹配的节点。' : '尚无节点接入。运行一个 SDK Agent 或 POST /v1/register。');
    return;
  }

  $('nodes-body').innerHTML = nodes.map((n) => {
    const p = platformOf(n);
    const platform = p.runtime ? p.runtime : '未上报';
    const meta = platformMeta(n);
    const tags = n.tags || [];
    const caps = n.capabilities || [];
    return '<tr>' +
      '<td data-label="节点"><span class="cell-value"><span class="cell-strong">' + esc(n.id) + '</span>' +
        '<span class="cell-sub">' + esc(n.name || '未命名') + '</span></span></td>' +
      '<td data-label="平台"><span class="cell-value"><span class="cell-main">' + esc(platform) + '</span>' +
        '<span class="cell-sub">' + esc(meta || 'OS 未上报') + '</span></span></td>' +
      '<td data-label="状态"><span class="cell-value">' + statusPair(n.status) + '</span></td>' +
      '<td data-label="标签"><span class="cell-value"><span title="' + esc(tags.join(', ')) + '">' + esc(listText(tags, 4)) + '</span></span></td>' +
      '<td data-label="能力"><span class="cell-value"><span title="' + esc(caps.join(', ')) + '">' + esc(listText(caps, 3)) + '</span></span></td>' +
      '<td data-label="席位"><span class="cell-value">' + (n.seat_id ? '<span class="cell-strong">' + esc(n.seat_id) + '</span>' : '—') + '</span></td>' +
      '<td class="num nowrap" data-label="最后心跳"><span class="cell-value" title="' + esc(fmtFull(n.last_seen)) + '">' + esc(fmtTime(n.last_seen)) + '</span></td>' +
      '</tr>';
  }).join('');
}

// ---------------------------------------------------------------------------
// 席位页

function rowErrorHtml(table, id) {
  if (!state.rowError || state.rowError.table !== table || state.rowError.id !== id) return '';
  return '<span class="cell-error" role="status">' + esc(state.rowError.text) + '</span>';
}

function setRowError(table, id, text) {
  state.rowError = { table: table, id: id, text: text };
  if (state.rowErrorTimer) clearTimeout(state.rowErrorTimer);
  state.rowErrorTimer = setTimeout(() => {
    state.rowError = null;
    state.rowErrorTimer = null;
    renderSeats();
    renderCommands();
    renderTasks();
  }, 8000);
}

function clearRowError() {
  state.rowError = null;
  if (state.rowErrorTimer) {
    clearTimeout(state.rowErrorTimer);
    state.rowErrorTimer = null;
  }
}

function bindRowHtml(seat) {
  if (state.inlineBind !== seat.id) return '';
  const nodes = state.nodes;
  let body;
  if (!nodes.length) {
    body = '<p class="empty-state">没有可绑定的节点。</p>' +
      '<button class="btn btn-sm btn-secondary" type="button" data-act="inline-cancel" data-table="seats">取消</button>';
  } else {
    const options = nodes.map((n) =>
      '<option value="' + esc(n.id) + '">' + esc(n.id + ' · ' + (n.name || '未命名') + ' (' + (STATUS_LABELS[n.status] || n.status || '未知') + ')') + '</option>').join('');
    body = '<form class="inline-form" data-inline="seat-bind" data-id="' + esc(seat.id) + '">' +
      '<label>绑定到节点<select name="node_id"><option value="" selected>选择节点</option>' + options + '</select></label>' +
      '<button class="btn btn-sm btn-primary" type="submit">确认绑定</button>' +
      '<button class="btn btn-sm btn-secondary" type="button" data-act="inline-cancel" data-table="seats">取消</button>' +
      '<p class="inline-result result" role="status" aria-live="polite"></p>' +
      '</form>';
  }
  return '<tr class="inline-row"><td colspan="7">' + body + '</td></tr>';
}

function renderSeats() {
  const body = $('seats-body');
  if (!state.connected) {
    body.innerHTML = emptyRow(7, '连接后显示席位。');
    return;
  }
  if (!state.lists.seats.ok) {
    body.innerHTML = emptyRow(7, listPlaceholder('seats', '席位'));
    return;
  }
  if (!state.seats.length) {
    body.innerHTML = emptyRow(7, '还没有席位。展开“创建虚席”新增一个。');
    return;
  }

  body.innerHTML = state.seats.map((s) => {
    const id = s.id;
    let actions;
    if (state.pendingDelete === id) {
      actions = '<span class="confirm-line">' +
        '<span class="confirm-text">删除后不可恢复</span>' +
        '<button class="row-action danger" type="button" data-act="seat-delete-confirm" data-id="' + esc(id) + '">确认删除</button>' +
        '<button class="row-action" type="button" data-act="inline-cancel" data-table="seats">取消</button>' +
        '</span>';
    } else if (s.kind === 'vacant') {
      actions = '<span class="row-actions">' +
        '<button class="row-action" type="button" data-act="seat-bind" data-id="' + esc(id) + '">绑定</button>' +
        '<button class="row-action danger" type="button" data-act="seat-delete" data-id="' + esc(id) + '">删除</button>' +
        '</span>';
    } else {
      actions = '<span class="row-actions">' +
        '<button class="row-action" type="button" data-act="seat-unbind" data-id="' + esc(id) + '">退席</button>' +
        '</span>';
    }
    const expect = s.expect && Object.keys(s.expect).length ? short(pretty(s.expect), 60) : '—';
    return '<tr>' +
      '<td data-label="ID"><span class="cell-value"><span class="cell-strong">' + esc(id) + '</span></span></td>' +
      '<td data-label="名称"><span class="cell-value">' + esc(s.name || '—') + '</span></td>' +
      '<td data-label="类型"><span class="cell-value">' + statusPair(s.kind) + '</span></td>' +
      '<td data-label="策略"><span class="cell-value">' + esc(s.bind_policy || '—') + '</span></td>' +
      '<td data-label="占用节点"><span class="cell-value">' + (s.node_id ? '<span class="cell-strong">' + esc(s.node_id) + '</span>' : '—') + '</span></td>' +
      '<td data-label="期望"><span class="cell-value"><span class="tag-text" title="' + esc(pretty(s.expect || {})) + '">' + esc(expect) + '</span></span></td>' +
      '<td data-label="操作"><span class="cell-value">' + rowErrorHtml('seats', id) + actions + '</span></td>' +
      '</tr>' + bindRowHtml(s);
  }).join('');

  if (state.focusInline) {
    state.focusInline = false;
    const sel = document.querySelector('.inline-form select');
    if (sel) sel.focus();
  }
}

// ---------------------------------------------------------------------------
// 指令页

function renderCommands() {
  const body = $('commands-body');
  if (!state.connected) {
    setTableNote('commands-note', '');
    body.innerHTML = emptyRow(7, '连接后显示指令。');
    return;
  }
  if (!state.lists.commands.ok) {
    setTableNote('commands-note', '');
    body.innerHTML = emptyRow(7, listPlaceholder('commands', '指令'));
    return;
  }
  setTableNote('commands-note', state.commandsTotal > state.commands.length
    ? '显示最新 ' + state.commands.length + ' / ' + state.commandsTotal + ' 条。更早的指令没有载入。'
    : '');
  const filter = $('commands-filter-status').value;
  const commands = filter ? state.commands.filter((c) => c.status === filter) : state.commands;
  if (!commands.length) {
    body.innerHTML = emptyRow(7, state.commands.length ? '没有匹配的指令。' : '还没有指令。在上方表单里下发一条。');
    return;
  }

  body.innerHTML = commands.map((c) => {
    const terminal = ['succeeded', 'failed', 'cancelled', 'timeout'].indexOf(c.status) >= 0;
    let detail;
    if (c.error) {
      const errText = typeof c.error === 'string' ? c.error :
        [c.error.code, c.error.message].filter(Boolean).join(' ');
      detail = '<span class="status-word s-failed" title="' + esc(pretty(c.error)) + '">' + esc(short(errText || 'error', 90)) + '</span>';
    } else if (c.result === null || c.result === undefined) {
      detail = '—';
    } else {
      detail = '<span title="' + esc(pretty(c.result)) + '">' + esc(short(c.result, 90)) + '</span>';
    }
    const action = terminal ? '' :
      '<button class="row-action danger" type="button" data-act="cmd-cancel" data-id="' + esc(c.id) + '">取消</button>';
    const stamped = c.finished_at || c.started_at || c.created_at || c.updated_at;
    return '<tr>' +
      '<td data-label="指令"><span class="cell-value"><span class="cell-strong" title="' + esc(c.id) + '">' + esc(short(c.id, 14)) + '</span></span></td>' +
      '<td data-label="目标"><span class="cell-value"><span class="cell-strong">' + esc(c.node_id || '—') + '</span>' +
        (c.seat_id ? '<span class="cell-sub">' + esc(c.seat_id) + '</span>' : '') + '</span></td>' +
      '<td data-label="方法"><span class="cell-value">' + esc(c.method || '—') + '</span></td>' +
      '<td data-label="状态"><span class="cell-value">' + statusPair(c.status) + '</span></td>' +
      '<td data-label="结果 / 错误"><span class="cell-value">' + detail + rowErrorHtml('commands', c.id) + '</span></td>' +
      '<td class="num nowrap" data-label="时间"><span class="cell-value" title="' + esc(fmtFull(stamped)) + '">' + esc(fmtTime(stamped)) + '</span></td>' +
      '<td data-label="操作"><span class="cell-value"><span class="row-actions">' + action + '</span></span></td>' +
      '</tr>';
  }).join('');
}

// ---------------------------------------------------------------------------
// 任务页

function renderTasks() {
  const body = $('tasks-body');
  if (!state.connected) {
    setTableNote('tasks-note', '');
    body.innerHTML = emptyRow(7, '连接后显示任务。');
    return;
  }
  if (!state.lists.tasks.ok) {
    setTableNote('tasks-note', '');
    body.innerHTML = emptyRow(7, listPlaceholder('tasks', '任务'));
    return;
  }
  setTableNote('tasks-note', state.tasksTotal > state.tasks.length
    ? '显示最新 ' + state.tasks.length + ' / ' + state.tasksTotal + ' 条。更早的任务没有载入。'
    : '');
  if (!state.tasks.length) {
    body.innerHTML = emptyRow(7, '还没有任务。展开“创建任务”新建一个。');
    return;
  }

  body.innerHTML = state.tasks.map((t) => {
    const terminal = ['succeeded', 'failed', 'cancelled', 'partial'].indexOf(t.status) >= 0;
    const pct = Math.max(0, Math.min(100, Math.round((t.progress || 0) * 100)));
    const failed = t.status === 'failed' || t.status === 'timeout';
    const progress = '<span class="progress-cell">' +
      '<progress max="100" value="' + pct + '"' + (failed ? ' class="is-failed"' : '') +
      ' aria-label="任务进度 ' + pct + '%">' + pct + '%</progress>' +
      '<span class="progress-text">' + pct + '%</span></span>';
    const action = terminal ? '' :
      '<button class="row-action danger" type="button" data-act="task-cancel" data-id="' + esc(t.id) + '">取消</button>';
    return '<tr>' +
      '<td data-label="任务"><span class="cell-value"><span class="cell-strong" title="' + esc(t.id) + '">' + esc(short(t.id, 14)) + '</span></span></td>' +
      '<td data-label="名称"><span class="cell-value">' + esc(t.name || '—') + '</span></td>' +
      '<td data-label="状态"><span class="cell-value">' + statusPair(t.status) + '</span></td>' +
      '<td data-label="进度"><span class="cell-value">' + progress + '</span></td>' +
      '<td class="num" data-label="指令数"><span class="cell-value">' + (t.command_ids || []).length + '</span></td>' +
      '<td class="num nowrap" data-label="更新时间"><span class="cell-value" title="' + esc(fmtFull(t.updated_at)) + '">' + esc(fmtTime(t.updated_at)) + '</span></td>' +
      '<td data-label="操作"><span class="cell-value"><span class="row-actions">' + action + rowErrorHtml('tasks', t.id) + '</span></span></td>' +
      '</tr>';
  }).join('');
}

// ---------------------------------------------------------------------------
// 事件页

function renderEvents() {
  const q = ($('events-filter').value || '').trim().toLowerCase();
  const matched = state.events.filter((ev) => {
    if (!q) return true;
    return [ev.type, ev.node_id, ev.seat_id, ev.task_id, ev.command_id, JSON.stringify(ev.data || {})]
      .join(' ').toLowerCase().indexOf(q) >= 0;
  });
  const shown = matched.slice(0, 200);

  let summary = '未连接';
  if (state.connected) {
    const held = state.events.length;
    if (q) {
      summary = '显示 ' + shown.length + ' / ' + held;
    } else if (shown.length < held) {
      summary = '显示最新 ' + shown.length + ' / ' + held + ' 条';
    } else {
      summary = held + ' 条';
    }
    if (state.eventCount > held) summary += ' · 会话 ' + state.eventCount + ' 条';
    if (state.paused) summary = '已暂停 · ' + summary;
  }
  $('events-summary').textContent = summary;

  if (!state.connected) {
    $('events-body').innerHTML = emptyRow(4, '连接后显示事件流。');
    return;
  }
  if (!state.events.length) {
    $('events-body').innerHTML = emptyRow(4, '等待事件…');
    return;
  }
  if (!shown.length) {
    $('events-body').innerHTML = emptyRow(4, '没有匹配的事件。');
    return;
  }

  $('events-body').innerHTML = shown.map((ev) => {
    const refs = [ev.node_id, ev.seat_id, ev.task_id, ev.command_id].filter(Boolean).join(' / ') || '—';
    const fullData = typeof ev.data === 'string' ? ev.data : pretty(ev.data || {});
    const data = ev.data === null || ev.data === undefined ? '—' : short(fullData, 140);
    const iso = isoTime(ev.time);
    return '<tr>' +
      '<td class="num nowrap" data-label="时间"><span class="cell-value"' + (iso ? ' title="' + esc(fmtFull(ev.time)) + '"' : '') + '>' + esc(fmtTime(ev.time)) + '</span></td>' +
      '<td data-label="类型"><span class="cell-value"><span class="cell-strong">' + esc(ev.type || 'event') + '</span></span></td>' +
      '<td data-label="节点 / 席位 / 任务 / 指令"><span class="cell-value">' + esc(refs) + '</span></td>' +
      '<td data-label="data"><span class="cell-value"><span class="clamp-1" title="' + esc(short(fullData, 400)) + '">' + esc(data) + '</span></span></td>' +
      '</tr>';
  }).join('');
}

function renderAll() {
  renderSyncBanner();
  renderOverview();
  renderPlatforms();
  renderNodes();
  renderSeats();
  renderCommands();
  renderTasks();
  renderEvents();
  updateTargetList();
}

// ---------------------------------------------------------------------------
// 指令目标

function currentTargetType() {
  const checked = document.querySelector('input[name="cmd-target-type"]:checked');
  return checked ? checked.value : 'node';
}

function updateTargetList() {
  const type = currentTargetType();
  const input = $('cmd-target');
  const list = $('cmd-target-list');
  input.placeholder = type === 'seat' ? 's-...' : 'n-1';
  const items = type === 'seat'
    ? state.seats.map((s) => ({ value: s.id, label: [s.id, s.name, STATUS_LABELS[s.kind] || s.kind].filter(Boolean).join(' · ') }))
    : state.nodes.map((n) => ({ value: n.id, label: [n.id, n.name, platformOf(n).runtime].filter(Boolean).join(' · ') }));
  const sig = type + '|' + items.map((it) => it.value + ':' + it.label).join('|');
  if (list.dataset.sig === sig) return;
  list.dataset.sig = sig;
  list.innerHTML = items.map((it) =>
    '<option value="' + esc(it.value) + '" label="' + esc(it.label) + '"></option>').join('');
}

// ---------------------------------------------------------------------------
// 动作：下发、创建、RPC

async function sendCommand(ev) {
  ev.preventDefault();
  const type = currentTargetType();
  const targetEl = $('cmd-target');
  const methodEl = $('cmd-method');
  const paramsEl = $('cmd-params');
  const target = targetEl.value.trim();
  const method = methodEl.value.trim();
  setInvalid(targetEl, !target);
  setInvalid(methodEl, !method);
  if (!target || !method) {
    setResult('cmd-result', '目标和方法不能为空。', 'err');
    return;
  }
  const parsed = parseJSON(paramsEl.value, {});
  const objectOk = parsed.ok && parsed.value !== null &&
    typeof parsed.value === 'object' && !Array.isArray(parsed.value);
  setInvalid(paramsEl, !objectOk);
  if (!parsed.ok) {
    setResult('cmd-result', 'params 不是合法 JSON：' + parsed.error, 'err');
    return;
  }
  const params = parsed.value;
  if (!objectOk) {
    setResult('cmd-result', 'params 必须是一个 JSON 对象。', 'err');
    return;
  }
  const req = { method: method, params: params };
  req[type === 'seat' ? 'seat_id' : 'node_id'] = target;
  const timeout = parseInt($('cmd-timeout').value, 10);
  if (Number.isFinite(timeout) && timeout >= 0) req.timeout = timeout;

  const btn = ev.target.querySelector('button[type="submit"]');
  setButtonBusy(btn, true);
  setResult('cmd-result', '下发中…', '');
  try {
    const r = await rpc('command.send', req);
    setInvalid(targetEl, false);
    setInvalid(methodEl, false);
    setInvalid(paramsEl, false);
    const label = STATUS_LABELS[r.command.status] || r.command.status;
    setResult('cmd-result', '已下发 ' + r.command.id + ' · ' + label, 'ok');
    await refreshAll({ force: true });
  } catch (e) {
    if (handleAuthLoss(e)) return;
    setResult('cmd-result', '下发失败：' + e.message, 'err');
  } finally {
    setButtonBusy(btn, false);
  }
}

async function createSeat(ev) {
  ev.preventDefault();
  const req = { name: $('seat-name').value.trim(), bind_policy: $('seat-policy').value };
  const expectEl = $('seat-expect');
  const parsed = parseJSON(expectEl.value, null);
  const objectOk = parsed.ok && (parsed.value === null ||
    (typeof parsed.value === 'object' && !Array.isArray(parsed.value)));
  setInvalid(expectEl, !objectOk);
  if (!parsed.ok) {
    setResult('seat-result', 'expect 不是合法 JSON：' + parsed.error, 'err');
    return;
  }
  if (parsed.value !== null) {
    if (!objectOk) {
      setResult('seat-result', 'expect 必须是一个 JSON 对象。', 'err');
      return;
    }
    req.expect = parsed.value;
  }

  const btn = ev.target.querySelector('button[type="submit"]');
  setButtonBusy(btn, true);
  setResult('seat-result', '创建中…', '');
  try {
    const r = await rpc('seat.create', req);
    setResult('seat-result', '已创建席位 ' + r.seat.id, 'ok');
    setInvalid(expectEl, false);
    $('seat-name').value = '';
    $('seat-expect').value = '';
    await refreshAll({ force: true });
  } catch (e) {
    if (handleAuthLoss(e)) return;
    setResult('seat-result', '创建失败：' + e.message, 'err');
    const details = $('seat-create');
    if (details) details.open = true;
  } finally {
    setButtonBusy(btn, false);
  }
}

async function createTask(ev) {
  ev.preventDefault();
  const commandsEl = $('task-commands');
  const parsed = parseJSON(commandsEl.value, []);
  const arrayOk = parsed.ok && Array.isArray(parsed.value);
  setInvalid(commandsEl, !arrayOk);
  if (!parsed.ok) {
    setResult('task-result', 'commands 不是合法 JSON：' + parsed.error, 'err');
    return;
  }
  if (!Array.isArray(parsed.value)) {
    setResult('task-result', 'commands 必须是 JSON 数组。', 'err');
    return;
  }

  const btn = ev.target.querySelector('button[type="submit"]');
  setButtonBusy(btn, true);
  setResult('task-result', '创建中…', '');
  try {
    const r = await rpc('task.create', {
      name: $('task-name').value.trim(),
      parallel: $('task-parallel').checked,
      commands: parsed.value,
    });
    setResult('task-result', '已创建任务 ' + r.task.id, 'ok');
    setInvalid(commandsEl, false);
    $('task-commands').value = '';
    await refreshAll({ force: true });
  } catch (e) {
    if (handleAuthLoss(e)) return;
    setResult('task-result', '创建失败：' + e.message, 'err');
    const details = $('task-create');
    if (details) details.open = true;
  } finally {
    setButtonBusy(btn, false);
  }
}

async function invokeRpc() {
  const methodEl = $('rpc-method');
  const paramsEl = $('rpc-params');
  const method = methodEl.value.trim();
  setInvalid(methodEl, !method);
  if (!method) {
    $('rpc-output').textContent = '方法不能为空。';
    $('rpc-meta').textContent = '错误';
    return;
  }
  const parsed = parseJSON(paramsEl.value, {});
  setInvalid(paramsEl, !parsed.ok);
  if (!parsed.ok) {
    $('rpc-output').textContent = 'params 不是合法 JSON：' + parsed.error;
    $('rpc-meta').textContent = '错误';
    return;
  }

  setButtonBusy($('rpc-invoke'), true);
  $('rpc-output').textContent = '调用中…';
  $('rpc-meta').textContent = '调用中';
  const started = performance.now();
  try {
    const result = await rpc(method, parsed.value);
    setInvalid(methodEl, false);
    setInvalid(paramsEl, false);
    const ms = Math.round(performance.now() - started);
    $('rpc-output').textContent = pretty(result);
    $('rpc-meta').textContent = '成功 · ' + ms + ' ms';
    await refreshAll({ force: true });
  } catch (e) {
    if (handleAuthLoss(e)) return;
    const ms = Math.round(performance.now() - started);
    $('rpc-meta').textContent = '错误' + (e.code !== undefined ? ' ' + e.code : '') + ' · ' + ms + ' ms';
    $('rpc-output').textContent = '错误 ' + (e.code !== undefined ? e.code + ' ' : '') + e.message + '\n' + pretty(e.data || '');
  } finally {
    setButtonBusy($('rpc-invoke'), false);
  }
}

let copyTimer = null;
async function copyRpc() {
  const text = $('rpc-output').textContent;
  const label = (msg) => {
    const btn = $('rpc-copy');
    btn.textContent = msg;
    if (copyTimer) clearTimeout(copyTimer);
    copyTimer = setTimeout(() => { btn.textContent = '复制'; }, 1600);
  };
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      label('已复制');
    } else {
      throw new Error('clipboard unavailable');
    }
  } catch (e) {
    const range = document.createRange();
    range.selectNodeContents($('rpc-output'));
    const sel = window.getSelection();
    sel.removeAllRanges();
    sel.addRange(range);
    label('已选中，请手动复制');
  }
}

// ---------------------------------------------------------------------------
// 行内动作

function focusRowAction(act, id) {
  const match = Array.from(document.querySelectorAll('button[data-act]')).find((b) =>
    b.dataset.act === act && b.dataset.id === id);
  if (match) match.focus({ preventScroll: true });
  return !!match;
}

// 表格重建后把焦点放回这一行；行没了就退到该页的「刷新」。
function restoreRowFocus(table, id, act) {
  const now = document.activeElement;
  if (now && now !== document.body) return;
  if (focusRowAction(act, id)) return;
  if (act === 'seat-delete-confirm' && focusRowAction('seat-delete', id)) return;
  const tab = $('tab-' + table);
  const fallback = tab ? tab.querySelector('[data-refresh]') : null;
  if (fallback) fallback.focus({ preventScroll: true });
}

async function runRowAction(btn, table, id, fn) {
  const act = btn.dataset.act;
  setButtonBusy(btn, true);
  try {
    await fn();
    clearRowError();
    state.inlineBind = null;
    state.pendingDelete = null;
    state.inlineFocus = null;
    await refreshAll({ force: true });
    restoreRowFocus(table, id, act);
  } catch (e) {
    if (handleAuthLoss(e)) return;
    setRowError(table, id, '操作失败：' + e.message);
    state.pendingDelete = null;
    state.inlineBind = null;
    state.inlineFocus = null;
    renderSeats();
    renderCommands();
    renderTasks();
    restoreRowFocus(table, id, act);
  } finally {
    setButtonBusy(btn, false);
  }
}

function clearInteraction(opts) {
  const back = opts && opts.keepFocus === false ? null : state.inlineFocus;
  state.inlineFocus = null;
  state.inlineBind = null;
  state.pendingDelete = null;
  clearRowError();
  renderSeats();
  renderCommands();
  renderTasks();
  if (back) focusRowAction(back.act, back.id);
}

// ---------------------------------------------------------------------------
// 导航

function activateTab(name, opts) {
  if (TABS.indexOf(name) < 0) name = 'overview';
  // 离开席位页时收起未完成的行内操作，避免下次回来看到过期表单。
  if (name !== 'seats' && (state.inlineBind || state.pendingDelete)) {
    clearInteraction({ keepFocus: false });
  }
  state.tab = name;
  document.querySelectorAll('.index-link').forEach((a) => {
    const active = a.dataset.tab === name;
    a.classList.toggle('active', active);
    if (active) a.setAttribute('aria-current', 'page');
    else a.removeAttribute('aria-current');
  });
  document.querySelectorAll('.tab').forEach((t) => {
    t.classList.toggle('active', t.id === 'tab-' + name);
  });
  if (name === 'nodes' && state.pendingPlatform) {
    const sel = $('nodes-filter-runtime');
    if (sel) {
      sel.value = state.pendingPlatform;
      renderNodes();
    }
    state.pendingPlatform = '';
  }
  if (opts && opts.scroll) window.scrollTo(0, 0);
  if (opts && opts.focus) {
    const section = $('tab-' + name);
    const heading = section ? section.querySelector('h2') : null;
    if (heading) {
      heading.setAttribute('tabindex', '-1');
      heading.focus({ preventScroll: true });
    }
  }
}

function activateFromHash(opts) {
  const name = location.hash.replace(/^#/, '');
  if (name === 'main') {
    const main = $('main');
    if (main) main.focus({ preventScroll: true });
    return;
  }
  if (!name) {
    activateTab('overview', { scroll: true, focus: !!(opts && opts.focus) });
    return;
  }
  if (TABS.indexOf(name) < 0) return;
  activateTab(name, { scroll: true, focus: !!(opts && opts.focus) });
}

// ---------------------------------------------------------------------------
// 事件绑定

function wire() {
  // 连接
  $('connect-toggle').addEventListener('click', () => {
    if ($('connect-panel').hidden) setPanelOpen(true, { focus: true });
    else setPanelOpen(false);
  });
  $('connect-form').addEventListener('submit', (e) => {
    e.preventDefault();
    connect({ auto: false });
  });
  $('disconnect').addEventListener('click', disconnect);

  // 页面导航
  document.addEventListener('click', (e) => {
    const platformLink = e.target.closest('[data-platform-link]');
    if (platformLink) {
      state.pendingPlatform = platformLink.dataset.platformLink;
      if (location.hash === '#nodes') {
        e.preventDefault();
        activateTab('nodes', { scroll: true, focus: true });
      }
      return;
    }
    const anchor = e.target.closest('a[href^="#"]');
    if (!anchor) return;
    const name = anchor.getAttribute('href').slice(1);
    if (TABS.indexOf(name) < 0) return;
    if (location.hash === '#' + name) {
      e.preventDefault();
      activateTab(name, { scroll: true, focus: true });
    }
  });
  window.addEventListener('hashchange', () => activateFromHash({ focus: true }));

  // 刷新按钮
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-refresh]');
    if (!btn) return;
    setButtonBusy(btn, true);
    refreshAll().finally(() => setButtonBusy(btn, false));
  });

  // 定时同步：页面在前台、没有行内交互时才跑。
  setInterval(() => {
    if (!state.connected || document.hidden || state.refreshInFlight) return;
    if (state.inlineBind || state.pendingDelete) return;
    refreshAll();
  }, 15000);

  // 行内动作
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-act]');
    if (!btn) return;
    const act = btn.dataset.act;
    const id = btn.dataset.id;

    if (act === 'seat-bind') {
      if (state.refreshTimer) {
        clearTimeout(state.refreshTimer);
        state.refreshTimer = null;
      }
      state.inlineBind = id;
      state.pendingDelete = null;
      state.inlineFocus = { act: 'seat-bind', id: id };
      state.focusInline = true;
      clearRowError();
      renderSeats();
      return;
    }
    if (act === 'seat-delete') {
      if (state.refreshTimer) {
        clearTimeout(state.refreshTimer);
        state.refreshTimer = null;
      }
      state.pendingDelete = id;
      state.inlineBind = null;
      state.inlineFocus = { act: 'seat-delete', id: id };
      clearRowError();
      renderSeats();
      focusRowAction('seat-delete-confirm', id);
      return;
    }
    if (act === 'inline-cancel') {
      clearInteraction();
      return;
    }
    if (act === 'seat-delete-confirm') {
      runRowAction(btn, 'seats', id, () => rpc('seat.delete', { seat_id: id }));
      return;
    }
    if (act === 'seat-unbind') {
      runRowAction(btn, 'seats', id, () => rpc('seat.unbind', { seat_id: id }));
      return;
    }
    if (act === 'cmd-cancel') {
      runRowAction(btn, 'commands', id, () => rpc('command.cancel', { command_id: id }));
      return;
    }
    if (act === 'task-cancel') {
      runRowAction(btn, 'tasks', id, () => rpc('task.cancel', { task_id: id }));
    }
  });

  // 行内绑定表单
  document.addEventListener('submit', async (e) => {
    const form = e.target.closest('form[data-inline="seat-bind"]');
    if (!form) return;
    e.preventDefault();
    const id = form.dataset.id;
    const nodeSel = form.elements.node_id;
    const nodeId = nodeSel ? nodeSel.value : '';
    const result = form.querySelector('.inline-result');
    const btn = form.querySelector('button[type="submit"]');
    setInvalid(nodeSel, !nodeId);
    if (!nodeId) {
      setResultEl(result, '请选择一个节点。', 'err');
      return;
    }
    setButtonBusy(btn, true);
    setResultEl(result, '绑定中…', '');
    try {
      await rpc('seat.bind', { seat_id: id, node_id: nodeId });
      state.inlineBind = null;
      clearRowError();
      await refreshAll({ force: true });
    } catch (err) {
      if (handleAuthLoss(err)) return;
      setResultEl(result, '绑定失败：' + err.message, 'err');
      setButtonBusy(btn, false);
    }
  });
  document.addEventListener('change', (e) => {
    const sel = e.target.closest && e.target.closest('form[data-inline="seat-bind"] select[name="node_id"]');
    if (sel) setInvalid(sel, false);
  });
  document.addEventListener('input', (e) => {
    if (e.target.getAttribute && e.target.getAttribute('aria-invalid') === 'true') {
      setInvalid(e.target, false);
    }
  });

  // 表单
  $('command-form').addEventListener('submit', sendCommand);
  $('seat-form').addEventListener('submit', createSeat);
  $('task-form').addEventListener('submit', createTask);
  $('rpc-form').addEventListener('submit', (e) => {
    e.preventDefault();
    invokeRpc();
  });
  $('rpc-copy').addEventListener('click', copyRpc);

  // 过滤与暂停
  $('nodes-filter-runtime').addEventListener('change', renderNodes);
  $('nodes-search').addEventListener('input', renderNodes);
  $('commands-filter-status').addEventListener('change', renderCommands);
  $('events-filter').addEventListener('input', renderEvents);
  $('events-pause').addEventListener('change', (e) => {
    state.paused = e.target.checked;
    renderEvents();
  });
  $('events-clear').addEventListener('click', () => {
    state.events = [];
    resetPulse();
    renderOverview();
    renderEvents();
  });

  // 指令寻址方式
  document.querySelectorAll('input[name="cmd-target-type"]').forEach((radio) => {
    radio.addEventListener('change', () => {
      $('cmd-target').value = '';
      updateTargetList();
    });
  });

  // engine.info 展开
  $('info-toggle').addEventListener('click', () => {
    const pre = $('info-json');
    const open = pre.hidden;
    pre.hidden = !open;
    $('info-toggle').setAttribute('aria-expanded', open ? 'true' : 'false');
    $('info-toggle').textContent = open ? '收起 engine.info' : '展开 engine.info';
  });

  // Esc：先取消行内交互，再收起连接面板
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    if (state.inlineBind || state.pendingDelete) {
      clearInteraction();
      return;
    }
    const panel = $('connect-panel');
    if (!panel.hidden && panel.contains(document.activeElement)) {
      setPanelOpen(false);
      $('connect-toggle').focus();
    }
  });

  // 页面回到前台时，事件流可能已被浏览器断开，计数也可能已经过期。
  document.addEventListener('visibilitychange', () => {
    if (document.hidden || !state.connected) return;
    if (!state.ws && !state.sse && !state.streamRetry) openEvents();
    if (!state.inlineBind && !state.pendingDelete) refreshAll();
  });
}

// ---------------------------------------------------------------------------
// 启动

(function boot() {
  wire();
  buildPulse();
  activateFromHash();
  $('auth-mode').textContent = 'offline';
  $('dateline-detail').textContent = '等待连接';
  renderAll();
  // 入场动画只放一次；之后的重绘不再重播。
  setTimeout(() => {
    const ledger = document.querySelector('.ledger');
    if (ledger) ledger.classList.add('intro-done');
  }, 900);

  const savedToken = sessionStorage.getItem('musubi.token') || '';
  const savedUser = sessionStorage.getItem('musubi.user') || '';
  $('token').value = savedToken;
  $('username').value = savedUser;
  connect({ auto: true });
})();
