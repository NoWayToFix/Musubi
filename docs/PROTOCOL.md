# Control Hub 协议规范 (PROTOCOL)

> 本文件是**唯一契约锚点**：Go 引擎、TS/Go/Python SDK、Electron 示例、契约测试全部以本文为准。
> 协议版本：`1.0`（本规范描述的协议版本）。

---

## 1. 总览

Control Hub（集控系统）采用 **JSON-RPC 2.0** 作为最小公分母协议：

- 信封：标准 JSON-RPC 2.0（`request` / `response` / `notification` / `batch`）。
- 默认编码：`application/json`；可选协商 `msgpack` / `protobuf`（按需启用）。
- 主传输：WebSocket；回退：HTTP POST + SSE / HTTP 长轮询。
- 服务端主动推送一律使用 **notification**（`method = "event"`），不占用请求 id。
- 所有时序值均为 ISO-8601 UTC 字符串，如 `2026-10-05T12:00:00.000Z`。

### 1.1 设计原则（版本化、可复用、极致兼容）

1. **只增不删**：字段只新增、不删除；废弃字段保留并标记 `deprecated`。错误码只增不改。
2. **忽略未知**：请求中的未知字段服务端必须忽略；响应/事件中的未知字段客户端必须忽略，一律不得报错。
3. **新增方法不改变旧方法行为**：旧客户端收到新事件类型时忽略或走默认处理。
4. **传输可替换、编码可协商、认证可插拔、能力可发现**，且互不影响业务层。

---

## 2. 信封 (Envelope)

### 2.1 请求

```json
{"jsonrpc": "2.0", "id": 1, "method": "node.list", "params": {"limit": 100}}
```

- `id`：`number | string`，必须。批处理（batch）中唯一。
- `params`：对象，可省略。

### 2.2 响应

```json
{"jsonrpc": "2.0", "id": 1, "result": {"nodes": [], "total": 0}}
```

### 2.3 错误

```json
{"jsonrpc": "2.0", "id": 1, "error": {"code": -32010, "message": "node not found", "data": {"node_id": "n-1"}}}
```

- `error.data` 可选，必须是对象。

### 2.4 通知（服务端 → 客户端）

```json
{"jsonrpc": "2.0", "method": "event", "params": {"subscription_id": "sub-1", "event": {"id": "ev-9", "type": "node.status.changed", "time": "2026-10-05T12:00:00.000Z", "node_id": "n-1", "data": {"status": "online"}}}}
```

### 2.5 批处理

JSON-RPC 2.0 数组批处理可用：`[{"jsonrpc":"2.0",...}, {...}]`。响应为数组（按请求顺序对应，`id` 匹配）。服务端对批处理的支持属于可选能力（`features.batch = true`，引擎默认开启）。

---

## 3. 会话、认证与版本协商

### 3.1 握手流程（任一传输一致）

1. 客户端连接（WS 或 HTTP）。
2. 若服务端要求认证（`server_info.session.authenticated = false`），先 `auth.login` 取得 `access_token`，之后通过 `Authorization: Bearer <token>` 或 `params.auth_token` 携带。
3. 客户端调用 `engine.info`，参数携带 `client_info`；服务端返回 `server_info`，其中包含**协商结果**（协议版本、编码、传输、能力）。
4. 客户端按协商结果工作；后续每次请求都带 `Authorization` 头（HTTP 传输），WS 传输在握手后第一次消息即视为已鉴权（或每帧可选携带）。

### 3.2 client_info（`engine.info` 请求参数）

```json
{
  "name": "hub-ts", "version": "1.0.0",
  "protocol_versions": ["1.0"],
  "encodings": ["json"],
  "transports": ["ws", "http"],
  "features": {"batch": true, "subscribe": true},
  "platform": {"os": "linux", "runtime": "node", "arch": "x64"}
}
```

- 所有字段可选。省略 `protocol_versions` 时视为 `["1.0"]`。

### 3.3 server_info（`engine.info` 结果）

```json
{
  "protocol_version": "1.0",
  "server": {"name": "hubd", "version": "1.0.0"},
  "supported_versions": ["1.0"],
  "encodings": ["json"],
  "transports": ["ws", "http"],
  "features": {"batch": true, "subscribe": true, "sse": true, "poll": true},
  "capabilities": [{"id": "node.registry", "version": "1.0"}, {"id": "command.exec", "version": "1.0"}, {"id": "task.flow", "version": "1.0"}, {"id": "event.stream", "version": "1.0"}, {"id": "auth.token", "version": "1.0"}],
  "session": {"authenticated": true, "expires_at": "2026-10-05T13:00:00.000Z"},
  "server_time": "2026-10-05T12:00:00.000Z"
}
```

### 3.4 版本协商规则

- 双方版本集合取交集；取**共同支持的最高协议版本**。
- 无交集 → 服务端返回错误 `-32005 protocol.version.mismatch`（不建立业务会话）。
- 版本号语义：`MAJOR.MINOR`。MINOR 递增 = 向后兼容的增量（新方法/新可选字段/新事件类型）；MAJOR 递增 = 破坏性变更。引擎必须同时支持 `supported_versions` 中所有版本。
- 客户端可发送 `protocol_versions` 中的较低版本，用 `client_info.features` 请求按需能力。

### 3.5 认证模式（可插拔）

| 模式 | 说明 | 启用方式 |
|---|---|---|
| `none` | 匿名（默认） | 默认 |
| `token` | 静态 Bearer Token | `hubd -auth token -token <secret>` |
| `basic` | Basic Auth 账号口令 | `hubd -auth basic -user u -pass p` |
| `oauth2` | 预留接口（Auth 接口实现） | 自定义插件 |

`auth.login` 参数按模式不同：

- `token`：`{"token": "<secret>"}`
- `basic`：`{"username": "...", "password": "..."}`
- `none`：`{}`（直接返回匿名 token）

---

## 4. 数据模型

### 4.1 Node（被控节点）

```json
{
  "id": "n-1", "name": "worker-1",
  "status": "online",
  "capabilities": ["shell.exec", "file.read"],
  "tags": ["linux", "x64"],
  "metadata": {"host": "h1", "arch": "amd64"},
  "last_seen": "2026-10-05T12:00:00.000Z",
  "registered_at": "2026-10-05T11:00:00.000Z"
}
```

- `status` ∈ `online | busy | offline | unknown`。
- `capabilities`、`tags`、`metadata` 均由注册方上报，引擎不解释（向前兼容：未知能力字符串原样保留）。

### 4.2 Command（指令）

```json
{
  "id": "c-1", "node_id": "n-1", "method": "echo",
  "params": {"text": "hi"},
  "status": "succeeded",
  "result": {"text": "hi"},
  "error": null,
  "timeout": 30000,
  "created_at": "2026-10-05T12:00:00.000Z",
  "started_at": "2026-10-05T12:00:00.000Z",
  "finished_at": "2026-10-05T12:00:01.000Z",
  "idempotency_key": "ik-1",
  "task_id": null
}
```

- `status` ∈ `pending | accepted | running | succeeded | failed | cancelled | timeout`。
- `result` 与 `error` 互斥（`error` 为错误对象时 `result` 为 `null`）。
- 未启动的字段省略或为 `null` 均可；**客户端必须容忍任意字段缺失**。

### 4.3 Task（任务 = Command 集合）

```json
{
  "id": "t-1", "name": "deploy",
  "status": "running",
  "progress": 0.5,
  "command_ids": ["c-1", "c-2"],
  "error": null,
  "created_at": "2026-10-05T12:00:00.000Z",
  "updated_at": "2026-10-05T12:00:01.000Z"
}
```

- `status` ∈ `pending | running | succeeded | failed | cancelled | partial`（`partial` = 部分成功）。
- `progress` ∈ `[0,1]`，`succeeded` 时为 `1`。

### 4.4 Event（事件）

```json
{
  "id": "ev-1", "type": "task.progress",
  "time": "2026-10-05T12:00:00.000Z",
  "node_id": null, "task_id": "t-1", "command_id": null,
  "data": {"progress": 0.5}
}
```

### 4.5 事件类型清单（`event.type`）

| type | 关联 | data 内容 |
|---|---|---|
| `node.registered` | node_id | node 对象 |
| `node.updated` | node_id | node 对象（变更后） |
| `node.status.changed` | node_id | `{status}` |
| `node.unregistered` | node_id | `{}` |
| `command.accepted` | node_id, command_id | command |
| `command.started` | node_id, command_id | `{command_id}` |
| `command.result` | node_id, command_id | command（终态） |
| `command.failed` | node_id, command_id | command（终态，含 error） |
| `command.cancelled` | node_id, command_id | command（终态） |
| `task.created` | task_id | task |
| `task.progress` | task_id | `{progress}` |
| `task.updated` | task_id | task |
| `task.completed` | task_id | task |
| `task.failed` | task_id | task（含 error） |
| `task.cancelled` | task_id | task |

> 客户端兼容规则：**未知事件类型必须忽略**（除非显式订阅且收到，也一律忽略不抛错）。

### 4.6 Capability（能力）

```json
{"id": "node.registry", "version": "1.0"}
```

引擎内置能力：`node.registry`、`command.exec`、`task.flow`、`event.stream`、`auth.token`、`transport.ws`、`transport.http`、`encoding.json`。

---

## 5. 方法清单

> 约定：`params?` = 可选。所有请求返回 `result` 对象或错误。方法名对未登录客户端返回 `-32001`（若引擎要求认证）。

| 方法 | params | result 要点 |
|---|---|---|
| `engine.info` | `{client_info?}` | 见 3.3 server_info |
| `engine.ping` | `{}` | `{"pong": true, "time": ISO}` |
| `engine.capabilities` | `{}` | `{"capabilities": [...], "features": {...}}` |
| `auth.login` | 见 3.5 | `{"access_token", "token_type":"bearer", "expires_in", "refresh_token"?}` |
| `auth.refresh` | `{refresh_token}` | 同上（新 access_token） |
| `auth.logout` | `{}` | `{}`（吊销当前 token） |
| `node.list` | `{filter?:{status?,tags?}, limit?, offset?}` | `{"nodes":[...], "total": N}`（limit 默认 100，最大 1000） |
| `node.get` | `{node_id}` | `{"node": {...}}`；不存在 → `-32010` |
| `node.subscribe` | `{node_id?}` | `{"subscription_id": "sub-..."}`（订阅该节点全部事件） |
| `node.unsubscribe` | `{subscription_id}` | `{}` |
| `command.send` | `{node_id, method, params?, timeout?, idempotency_key?}` | `{"command": {...}}`；节点离线 → `-32011` |
| `command.batch` | `{commands:[{node_id,method,params?,timeout?}], idempotency_key?}` | `{"commands":[...], "task_id"?}`（同步逐个下发；失败者返回对应 error 于 command） |
| `command.cancel` | `{command_id}` | `{"command": {...}}`；终态不可取消 → `-32014` |
| `task.create` | `{name, commands:[{node_id,method,params?,timeout?}], parallel?}` | `{"task": {...}}`；立即开始执行 |
| `task.get` | `{task_id}` | `{"task": {...}}`；不存在 → `-32020` |
| `task.list` | `{filter?:{status?}, limit?, offset?}` | `{"tasks":[...], "total": N}` |
| `task.cancel` | `{task_id}` | `{"task": {...}}`；终态 → `-32021` |
| `task.subscribe` | `{task_id?}` | `{"subscription_id": "sub-..."}` |
| `event.subscribe` | `{event_types?:[str], node_id?, task_id?}` | `{"subscription_id": "sub-..."}`；`event_types` 与 node/task 过滤取交集；空 = 全量 |
| `event.unsubscribe` | `{subscription_id}` | `{}`；不存在 → `-32030` |

> 兼容性说明：`filter.tags` 为数组时做"包含任一"匹配；`filter.status` 精确匹配。引擎对未知过滤字段一律忽略。

---

## 6. 传输与端点

| 传输 | 端点 | 说明 |
|---|---|---|
| WebSocket（主） | `ws[s]://host/v1/ws` | JSON-RPC 消息帧；`msgpack` 协商后二进制帧承载 msgpack 信封 |
| HTTP POST（回退） | `http[s]://host/v1/rpc` | `Content-Type: application/json`（或 msgpack）；body 为单个信封或数组；响应同步返回 |
| SSE | `http[s]://host/v1/events?token=...` | `text/event-stream`；`data:` 为 JSON-RPC notification（`event` 方法） |
| 长轮询 | `http[s]://host/v1/poll?cursor=N&timeout=30&token=...` | 返回 `{"cursor": N, "events":[...]}`（事件对象数组）或空列表；`timeout` 最大 60s |
| 健康检查 | `GET /v1/health` | `{"ok": true, "version": "1.0.0", "time": ISO}` |

### 6.1 传输降级顺序（SDK 内置）

`ws` → `sse` → `poll`。SDK 按 `server_info.transports` 与自身支持取交集，按上表优先级选择。传输切换不改变上层业务逻辑（Transport 接口统一）。

### 6.2 网络兼容

- **心跳**：WS 用协议层 ping/pong；HTTP/SSE/长轮询用 `engine.ping`，SDK 默认每 30s。
- **断线重连**：指数退避（base 1s、factor 2、max 30s、±20% 抖动），无限重试（可配置 `max_retries`）。
- **离线队列**：连接恢复前，写操作（command.send / task.create / auth.login 之外的调用）进入内存队列（上限 1000，FIFO），恢复后按序发送；丢弃策略由 SDK 配置。
- **去重与幂等**：`idempotency_key` 由调用方提供；引擎在 TTL（默认 5 分钟）内对相同 key 返回首次结果。SDK 侧对事件按 `event.id` 去重（窗口 5000 条）。
- **语义**：默认"至多一次"（请求响应式）；启用 `idempotency_key` 后可达"至少一次"。
- **代理/TLS/自签名**：各 SDK 支持 `proxy` / `ca` / `insecure` 配置项；自签名证书通过 `ca` 提供 CA 或 `insecure_skip_verify`。

---

## 7. 错误码（只增不改）

### 7.1 JSON-RPC 标准错误（沿用规范）

| code | 含义 |
|---|---|
| `-32700` | Parse error |
| `-32600` | Invalid Request |
| `-32601` | Method not found |
| `-32602` | Invalid params |
| `-32603` | Internal error |

### 7.2 领域错误码（Hub 自定义，`-32000` 段，稳定不删除）

| code | 含义 | data 示例 |
|---|---|---|
| `-32001` | auth.required 未认证 | `{}` |
| `-32002` | auth.invalid 凭证无效 | `{}` |
| `-32003` | auth.expired token 过期 | `{"retry_after": 0}` |
| `-32004` | auth.forbidden 无权限 | `{}` |
| `-32005` | protocol.version.mismatch | `{"supported": ["1.0"], "client": ["0.9"]}` |
| `-32006` | encoding.not.supported | `{"supported": ["json"]}` |
| `-32007` | transport.not.supported | `{}` |
| `-32010` | node.not_found | `{"node_id": "..."}` |
| `-32011` | node.offline | `{"node_id": "..."}` |
| `-32012` | node.busy | `{"node_id": "..."}` |
| `-32013` | command.not_found | `{"command_id": "..."}` |
| `-32014` | command.cancel.rejected | `{"command_id": "...", "status": "succeeded"}` |
| `-32020` | task.not_found | `{"task_id": "..."}` |
| `-32021` | task.cancel.rejected | `{"task_id": "...", "status": "succeeded"}` |
| `-32030` | subscription.not_found | `{"subscription_id": "..."}` |
| `-32031` | event.type.not_supported | `{"event_type": "..."}` |
| `-32040` | feature.disabled | `{"feature": "..."}` |
| `-32041` | rate.limited | `{"retry_after": 30}` |

---

## 8. 兼容性规则（实现方必须遵守）

1. 服务端：请求解析用"已知字段白名单 + 忽略未知"，**永不因未知字段失败**。
2. 客户端：响应/事件解析对未知字段、缺失字段一律容忍。
3. 新增字段：`optional: true`；新增方法：独立注册；新增事件类型：版本 MINOR+1。
4. 废弃字段：schema 标记 `"deprecated": true` 并注明 `deprecatedMessage`，至少保留 2 个大版本。
5. `engine.info`/`engine.capabilities` 永不鉴权要求失败（匿名可查），用于发现。
6. 心跳失败、超时、断线一律不抛出业务异常，走重连/降级路径。