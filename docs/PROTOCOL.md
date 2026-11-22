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

> **标识符格式**：`c-`（指令）、`t-`（任务）、`s-`（席位）、`sub-`（订阅）前缀后的变量部分为**不可猜测的随机 token**（128-bit base64url，约 22 字符），**只有前缀是契约的一部分**。下文示例中的 `c-1` / `t-1` / `s-1` / `sub-1` 仅为示意，客户端不得假设其长度、格式或顺序。`ev-` 事件 id 为引擎内**递增序号**（事件 id 不对应任何可鉴权操作，事件流本就对所有订阅者全量广播，可预测无碍）。

### 4.1 Node（被控节点）

```json
{
  "id": "n-1", "name": "worker-1",
  "status": "online",
  "capabilities": ["shell.exec", "file.read"],
  "tags": ["linux", "x64"],
  "metadata": {"host": "h1", "arch": "amd64"},
  "platform": {
    "os": "linux", "arch": "amd64",
    "runtime": "go", "runtime_version": "go1.21.5",
    "agent_version": "1.1.0",
    "labels": {"env": "prod", "zone": "a"}
  },
  "last_seen": "2026-10-05T12:00:00.000Z",
  "registered_at": "2026-10-05T11:00:00.000Z"
}
```

- `status` ∈ `online | busy | offline | unknown`。
- `capabilities`、`tags`、`metadata` 均由注册方上报，引擎不解释（向前兼容：未知能力字符串原样保留）。
- `platform`（可选，多平台集控）：注册方自报的平台描述。字段均可选、引擎不解释：
  - `os` / `arch`：操作系统与架构（`linux` / `darwin` / `windows`，`amd64` / `arm64` / `x64` …）；
  - `runtime`：接入运行时（`go` / `node` / `deno` / `bun` / `electron` / `browser` / `python` / `demo`，未知值原样保留）；
  - `runtime_version` / `agent_version`：运行时版本与 SDK/Agent 版本；
  - `labels`：字符串键值对，可用于平台分组或运维筛选。
  官方 SDK 的 Agent 运行时自动探测该描述；`node.list` 支持按 `runtime` / `os` 精确过滤（§5）。
  重新注册时提供 `platform` 会**整体替换**旧描述（未提供的字段不继承）；不提供则保留原值。
- `seat_id`（可选，§4.7）：该节点占用的席位 id；未占席时省略或为 `null`。

### 4.2 Command（指令）

```json
{
  "id": "c-1", "node_id": "n-1", "seat_id": "s-1", "method": "echo",
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
- `seat_id`（可选）：指令经席位路由时记录所占席位；`node_id` 始终是最终执行节点。经席位下发的指令两个字段都会出现。
- **两种执行模式**：内置 demo 节点由引擎进程内模拟器执行（`echo`/`fail`/`sleep`/`shell`）；真实节点通过 `command.pull` 领取、`command.complete` 回报（见 §5.3），指令在领取前为 `pending`。

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
| `seat.created` | — | seat 对象（虚席） |
| `seat.filled` | node_id | seat 对象（补席后的常席） |
| `seat.released` | node_id | seat 对象（退席后的虚席） |
| `seat.deleted` | — | `{}` |

> 客户端兼容规则：**未知事件类型必须忽略**（除非显式订阅且收到，也一律忽略不抛错）。

### 4.6 Capability（能力）

```json
{"id": "node.registry", "version": "1.0"}
```

引擎内置能力：`node.registry`、`command.exec`、`task.flow`、`event.stream`、`auth.token`、`transport.ws`、`transport.http`、`encoding.json`、`seat.registry`、`seat.routing`、`command.dispatch`、`platform.registry`。

能力是**只增不删**的：客户端必须忽略未知能力；`platform.registry` 表示节点可携带 `platform` 描述（§4.2）且 `node.list` 支持 `runtime` / `platform` / `os` 过滤，同时 `features.platforms = true`。旧引擎不宣告该能力，旧客户端不受影响。

### 4.7 Seat（席位）

席位是引擎中"**可占用的位置**"的一等抽象，是本系统**可拓展性的代名词**：凡以"预留 → 占用 → 释放"生命周期接入引擎的资源，都通过席位统一管理。席位分为两种：

- **常席（standing）**：已被实体（当前为节点）占用的席位。席位与占用者绑定，占用者在线期间常席有效。
- **虚席（vacant）**：预留但尚未被占用的席位。可预先声明**期望画像**（`expect`）等待匹配实体补席，也可仅作容量预留。

```json
{
  "id": "s-1",
  "kind": "standing",
  "name": "edge-worker-slot",
  "bind_policy": "auto",
  "expect": {"tags": ["linux"], "capabilities": ["shell.exec"]},
  "node_id": "n-1",
  "created_at": "2026-10-12T12:00:00.000Z",
  "filled_at": "2026-10-12T12:01:00.000Z",
  "released_at": null
}
```

- `kind` ∈ `standing | vacant`。`node_id` 仅常席非空（虚席为 `null`）。
- `bind_policy` ∈ `auto | manual`（虚席专属语义）：
  - `auto`：节点注册（`POST /v1/register` 或引擎内部注册）且命中 `expect` 画像时**自动补席**（占席）；
  - `manual`：仅能通过 `seat.bind` 显式指派。
- `expect`（虚席专属，可省略）：期望画像，字段均可选，未知字段一律忽略。匹配规则：
  - `capabilities`：节点须具备**全部**列出能力（子集匹配）；
  - `tags`：节点标签**包含任一**即命中（与 `node.list` 的 `filter.tags` 语义一致）；
  - `metadata`：列出的每个键在节点 metadata 中存在且值相等。
- 生命周期与状态转换：
  - **补席（fill）**：虚席 → 常席。触发：`auto` 虚席被匹配节点注册自动占用，或 `seat.bind` 手动指派。多个虚席同时命中时按创建顺序取第一个。手动 `seat.bind` 同样校验 `expect`（不匹配 → `-32052`），保证画像对两种补席路径一致生效。
  - **退席（release）**：常席 → 虚席。触发：占用者注销/被判离线（看门狗）、或显式 `seat.unbind`。**壳位与画像保留**，可再次补席（"位置是持久的，占位者是流动的"）。
  - **消亡**：仅虚席可 `seat.delete`；常席须先退席。
- 一个节点至多占用一个席位：节点补席时若已占用其他席位，旧席位自动退席（释放为虚席）。
- **席位参与指令路由**：`command.send` / `command.batch` / `task.create` 的每个条目可用 `seat_id` 取代 `node_id`，引擎将其解析为当前占位节点（`node_id` 与 `seat_id` **互斥**，同时给出或同时缺省 → `-32602`）。空席没有执行者 → `-32053 seat.vacant`；席位不存在 → `-32050`。指令对象同时记录 `node_id`（实际执行者）与 `seat_id`（路由来源）。
- `node.get` / `node.list` 返回的节点对象带可选字段 `seat_id`（占用席位 id）；旧客户端按"忽略未知字段"规则自然兼容。
- 席位与现有 `node.registry` 正交：无席位时系统行为与现状完全一致；只有真实节点注册才触发**自动**补席（内置 demo 节点不参与自动补席，但可被 `seat.bind` 手动指派）。
- 席位资源记录创建者所有者（规则同 §5 资源所有权）：非所有者调用 `seat.bind` / `seat.unbind` / `seat.delete` → `-32004`（`data.resource_type = "seat"`）。

---

## 5. 方法清单

> 约定：`params?` = 可选。所有请求返回 `result` 对象或错误。方法名对未登录客户端返回 `-32001`（若引擎要求认证）。

| 方法 | params | result 要点 |
|---|---|---|
| `engine.info` | `{client_info?}` | 见 3.3 server_info |
| `engine.ping` | `{}` | `{"pong": true, "time": ISO}` |
| `engine.capabilities` | `{}` | `{"capabilities": [...], "features": {...}}` |
| `auth.login` | 见 3.5 | `{"access_token", "token_type":"bearer", "expires_in", "refresh_token"?}` |
| `auth.refresh` | `{refresh_token}` | 同上（**新 access_token 且新 refresh_token**；旧 token 对原子失效，旧 refresh_token 不可重放） |
| `auth.logout` | `{}` | `{}`（吊销当前 token） |
| `node.register` | `{node_id?, name?, capabilities?, tags?, metadata?, platform?}` | `{"node": {...}}`；注册或更新真实节点（同 id 重复注册 = 更新并恢复 online）；`node_id` 省略时引擎生成；demo id 冲突 → `-32602`；跨所有者接管 → `-32004` |
| `node.list` | `{filter?:{status?,tags?,runtime?,platform?,os?}, limit?, offset?}` | `{"nodes":[...], "total": N}`（limit 默认 100，最大 1000；`runtime` 与 `platform` 等价，按 `platform.runtime` 精确匹配；`os` 按 `platform.os` 精确匹配） |
| `node.get` | `{node_id}` | `{"node": {...}}`；不存在 → `-32010` |
| `node.heartbeat` | `{node_id}` | `{"node": {...}}`；刷新 `last_seen`，离线节点恢复 online；不存在 → `-32010` |
| `node.unregister` | `{node_id}` | `{"node": {...}}`；注销节点并终结其未完成指令 |
| `node.subscribe` | `{node_id?}` | `{"subscription_id": "sub-..."}`（订阅该节点全部事件） |
| `node.unsubscribe` | `{subscription_id}` | `{}` |
| `command.send` | `{node_id?, seat_id?, method, params?, timeout?, idempotency_key?}` | `{"command": {...}}`；节点离线 → `-32011`；`node_id`/`seat_id` 二选一 |
| `command.batch` | `{commands:[{node_id?, seat_id?, method, params?, timeout?}], idempotency_key?}` | `{"commands":[...], "task_id"?}`（同步逐个下发；真实节点会等待其 pull/complete；失败者返回对应 error 于 command） |
| `command.cancel` | `{command_id}` | `{"command": {...}}`；终态不可取消 → `-32014` |
| `command.get` | `{command_id}` | `{"command": {...}}`；不存在 → `-32013` |
| `command.list` | `{filter?:{status?,node_id?,task_id?}, limit?, offset?}` | `{"commands":[...], "total": N}`（最新的在前） |
| `command.pull` | `{node_id, limit?, wait?}` | `{"commands":[...]}`；真实节点领取待执行指令，`wait` 为长轮询毫秒（≤30000） |
| `command.complete` | `{node_id, command_id, status, result?, error?}` | `{"command": {...}}`；幂等：已终态原样返回 |
| `task.create` | `{name, commands:[{node_id?, seat_id?, method, params?, timeout?}], parallel?}` | `{"task": {...}}`；立即开始执行 |
| `task.get` | `{task_id}` | `{"task": {...}}`；不存在 → `-32020` |
| `task.list` | `{filter?:{status?}, limit?, offset?}` | `{"tasks":[...], "total": N}` |
| `task.cancel` | `{task_id}` | `{"task": {...}}`；终态 → `-32021` |
| `task.subscribe` | `{task_id?}` | `{"subscription_id": "sub-..."}` |
| `event.subscribe` | `{event_types?:[str], node_id?, task_id?}` | `{"subscription_id": "sub-..."}`；`event_types` 与 node/task 过滤取交集；空 = 全量；`event_types` 非数组或含非字符串 → `-32602` |
| `event.unsubscribe` | `{subscription_id}` | `{}`；不存在 → `-32030` |
| `seat.list` | `{filter?:{kind?,node_id?}, limit?, offset?}` | `{"seats":[...], "total": N}`（`kind` ∈ `standing\|vacant`，精确匹配） |
| `seat.create` | `{name?, bind_policy?, expect?}` | `{"seat": {...}}`（创建虚席；`bind_policy` 默认 `manual`；`expect` 仅保留已知字段 `capabilities`/`tags`/`metadata`） |
| `seat.get` | `{seat_id}` | `{"seat": {...}}`；不存在 → `-32050` |
| `seat.delete` | `{seat_id}` | `{}`；仅虚席可删，常席 → `-32051` |
| `seat.bind` | `{seat_id, node_id}` | `{"seat": {...}}`；席位已占用 → `-32051`；节点不存在 → `-32010`；节点不匹配 `expect` → `-32052`；节点已占其他席位则旧席位自动退席 |
| `seat.unbind` | `{seat_id}` | `{"seat": {...}}`（退席回虚席，壳位与画像保留；对虚席为幂等 no-op，原样返回） |

> 兼容性说明：`filter.tags` 为数组时做"包含任一"匹配；`filter.status` 精确匹配。引擎对未知过滤字段一律忽略。
>
> **超时/时长参数**：`command.send` / `command.batch` / `task.create` 的 `timeout` 与 `sleep` 的 `ms` 必须为**整数毫秒**，范围 `[0, 2147483647]`；`0` = 不超时（`sleep` 的 `ms=0` 为立即返回）；非整数（如 `0.5`）→ `-32602`；负值按 `0` 处理。
>
> **资源所有权**：订阅、指令、任务资源记录创建者所有者（token 模式为签发的 access_token，basic 模式为 `basic:<user>`）。非所有者调用 `event.unsubscribe` / `node.unsubscribe` / `command.cancel` / `task.cancel` → `-32004`（见 §7.2），且该检查先于存在性/终态检查（跨所有者探测无法区分资源是否存在）；匿名（none 模式、无 token）创建的资源不校验所有权。none 模式下 `auth.login {}` 签发的**匿名 token 只是会话标识，不构成所有者**。节点可被任意新会话重新注册（否则重启后的 Agent 无法认领自己的节点）。

### 5.3 真实节点下发（`command.pull` / `command.complete`）

内置 demo 节点在引擎进程内执行；**真实节点**（`node.register` 或 `POST /v1/register` 注册）通过拉取模式执行指令，因此任意语言、任意传输都能做 worker，引擎不需要向节点建立回调连接。两种注册入口语义一致：请求体/参数相同（`node_id` 在 HTTP 入口叫 `id`）、同 id 重复注册为更新、注册者成为所有者。

**状态机**（仅真实节点）：

```
pending --command.pull--> running --command.complete--> succeeded | failed | cancelled | timeout
   |                          |
   +-- timeout/cancel/node offline ---> 终态
```

- `command.send` 发往真实节点时立即返回 `status=pending` 的指令；节点用 `command.pull` 领取（可长轮询 `wait` 毫秒），领取瞬间转为 `running` 并发布 `command.started`。
- 节点用 `command.complete` 回报 `succeeded | failed | cancelled | timeout`，可附 `result` 或 `{code,message,data}` 错误对象。**幂等**：对已终态指令再次 complete 原样返回，不改变首次终态。
- `timeout`（默认 30000ms）是**从下发到终态的引擎侧上限**，包含排队等待；`0` 表示不设上限。超时由引擎终结为 `timeout`。
- `command.cancel` 对尚未领取的指令直接出队；已领取的指令标记 `cancelled`，节点随后回报的结果被忽略（首次终态优先）。
- 节点注销、被看门狗判离线时，其队列中（含已领取未回报）的指令统一终结为 `failed` + `-32011 node.offline`。
- **至多一次**：引擎从不在重启后重放指令；重启时未完成的指令标记为 `failed`（`data.reason = "engine.restarted"`）。
- 节点身份：`node.register` / `POST /v1/register` 记录注册者所有者；`command.pull` / `command.complete` / `node.heartbeat` / `node.unregister` 要求调用者与节点所有者一致（匿名注册的节点保持开放，与 none 模式一致），防止冒领其他节点的指令。
- `command.batch` 对真实节点保持**同步**语义：服务端等待节点领取并回报，直到终态或超时。
- `task.create` 的指令同样进入队列，任务进度在 `command.complete` 后推进；任务取消会出队/终结对应指令。

### 5.4 平台 Agent 生命周期（SDK 参考实现）

多平台集控的最小接入单元是"Agent"：一个在目标平台上运行、由 SDK 提供运行时、承载业务 handler 的进程/线程。官方三套 SDK（Go / TypeScript / Python）内置 Agent，其他语言可按本节自行实现：

```
node.register  →  node.heartbeat（周期）  →  command.pull（长轮询）
               →  本地执行 handler        →  command.complete
               →  node.unregister（退出）
```

- **注册**：上报 `node_id`（可省略）、`name`、`capabilities`、`tags`、`metadata` 和 `platform`。Agent 应把自身支持的方法列入 `capabilities`，便于控制台/调用方选型。
- **心跳**：默认 10s；引擎在 3 个心跳周期（15s）未收到心跳后判定 offline。心跳/拉取返回 `-32010 node.not_found` 或 `-32011 node.offline` 时，Agent 应重新注册。
- **执行**：未注册的指令方法回报 `failed` + `-32601`；handler 抛错回报 `failed`，错误对象为 `{code, message}`（默认 `-32603`）。
- **幂等/退出**：`command.complete` 幂等；正常退出前调用 `node.unregister`，引擎会终结该节点未完成指令（`-32011`）。异常退出则由看门狗回收。
- **并发**：Agent 可运行多个拉取循环，每个循环一次只领取一条（`limit=1`），避免一次领取超过本地执行能力。
- **传输**：WebSocket 单连接支持并发请求（长轮询不阻塞心跳/回执）；HTTP 长轮询天然独立。SDK 默认 `auto` 协商。

---

## 6. 传输与端点

| 传输 | 端点 | 说明 |
|---|---|---|
| WebSocket（主） | `ws[s]://host/v1/ws` | JSON-RPC 消息帧；`msgpack` 协商后二进制帧承载 msgpack 信封 |
| HTTP POST（回退） | `http[s]://host/v1/rpc` | `Content-Type: application/json`（或 msgpack）；body 为单个信封或数组；响应同步返回 |
| SSE | `http[s]://host/v1/events?token=...` | `text/event-stream`；`data:` 为 JSON-RPC notification（`event` 方法） |
| 长轮询 | `http[s]://host/v1/poll?cursor=N&timeout=30&token=...` | 返回 `{"cursor": N, "events":[...]}`（事件对象数组）或空列表；`timeout` 最大 60s |
| 健康检查 | `GET /v1/health` | `{"ok": true, "version": "1.0.0", "time": ISO}` |
| 节点注册 | `POST /v1/register` | 真实节点注册（body：`{id?, name?, capabilities?, tags?, metadata?, platform?}`）；等价于 JSON-RPC `node.register`（`id` ↔ `node_id`）；同 id 重复注册为**更新**；请求 id 与内置 **demo 节点冲突 → `-32602`**（HTTP 400 + JSON-RPC 错误信封，不允许遮蔽 demo 节点）；注册者成为节点的所有者 |
| Web 控制台 | `GET /console/` | 引擎内嵌的零构建管理控制台（静态资源，同源调用 RPC 与事件流；API 本身仍按 §3 鉴权） |

### 6.1 传输降级顺序（SDK 内置）

`ws` → `sse` → `poll`。SDK 按 `server_info.transports` 与自身支持取交集，按上表优先级选择。传输切换不改变上层业务逻辑（Transport 接口统一）。

### 6.2 网络兼容

- **心跳**：WS 用协议层 ping/pong；HTTP/SSE/长轮询用 `engine.ping`，SDK 默认每 30s。
- **断线重连**：指数退避（base 1s、factor 2、max 30s、±20% 抖动），无限重试（可配置 `max_retries`）。
- **离线队列**：三个官方 SDK 均内置且**默认开启**（可关闭）。连接恢复前，写操作（`engine.*` 与 `auth.*` 之外的调用；Python SDK 额外排除 `event.*`）在**传输层失败**时进入内存队列（上限 1000，FIFO），恢复后按序重放；服务端已受理（返回权威 JSON-RPC 错误）的条目直接丢弃（尽力而为）。Go SDK 以哨兵错误 `ErrQueued` 提示已入队（尚未执行），Python SDK 抛出携带 `data["queued"] = true` 的 `HubError`。
- **重连后的事件重放**：引擎事件游标仅存于内存（不持久化，历史上限 10000 条）。SDK 在重连（首次成功轮询/流恢复）后会把游标重置为 0 重读历史，以免引擎重启后游标永久失效导致饥饿；`event.id` 去重窗口（5000 条）会吸收大部分重复，但**超出窗口的旧事件可能被重复投递**（历史超 5000 条时），消费者必须保持幂等。
- **去重与幂等**：`idempotency_key` 由调用方提供；引擎在 TTL（默认 5 分钟）内对相同 key 返回首次结果。SDK 侧对事件按 `event.id` 去重（窗口 5000 条）。
- **语义**：请求响应式调用默认"至多一次"；启用 `idempotency_key` 后可达"至少一次"；**离线队列重放的写操作为"至少一次"**（重放中再遇传输层失败会重新入队，服务端可能已执行，建议配合 `idempotency_key` 去重）。
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
| `-32004` | auth.forbidden 无权限 | `{"resource_type": "subscription" \| "command" \| "task" \| "seat" \| "node"}` |
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
| `-32050` | seat.not_found | `{"seat_id": "..."}` |
| `-32051` | seat.occupied | `{"seat_id": "..."}` |
| `-32052` | seat.expect.mismatch | `{"seat_id": "...", "node_id": "..."}` |
| `-32053` | seat.vacant 席位无占位者 | `{"seat_id": "..."}` |

> `-32004 auth.forbidden` 的触发场景：**非资源所有者**调用 `event.unsubscribe` / `node.unsubscribe` / `command.cancel` / `task.cancel`，`data.resource_type` 标明资源种类。所有者身份 = 签发该 access_token 的会话（token 模式）或 basic 用户（`basic:<user>`）；匿名（none 模式、无 token）创建的资源不校验。同一 access_token 的多次调用视为同一所有者。

---

## 8. 兼容性规则（实现方必须遵守）

1. 服务端：请求解析用"已知字段白名单 + 忽略未知"，**永不因未知字段失败**。
2. 客户端：响应/事件解析对未知字段、缺失字段一律容忍。
3. 新增字段：`optional: true`；新增方法：独立注册；新增事件类型：版本 MINOR+1。
4. 废弃字段：schema 标记 `"deprecated": true` 并注明 `deprecatedMessage`，至少保留 2 个大版本。
5. `engine.info`/`engine.capabilities` 永不鉴权要求失败（匿名可查），用于发现。
6. 心跳失败、超时、断线一律不抛出业务异常，走重连/降级路径。

---

## 9. 持久化与会话恢复（可选，`-data-dir`）

默认纯内存。指定 `-data-dir` 后，引擎把需要跨重启的实体快照到 `<dir>/hub-state.json`（单文件、原子写入、0600 权限，1 秒防抖 + 退出时 flush）。

**持久化内容**：真实节点注册表、席位（含期望画像）、任务、指令历史（最近 2000 条）、未过期的 access/refresh token。

**恢复语义（明确且不重放）**：

| 实体 | 恢复后 |
|---|---|
| 真实节点 | 状态 `offline`，等待重新注册/心跳；`seat_id` 清空；`platform`/`capabilities`/`tags`/`metadata` 原样保留 |
| 常席 | 退为虚席（壳位与 `expect` 保留），符合"位置持久、占位者流动" |
| 未完成指令/任务 | 标记 `failed`，error `data.reason = "engine.restarted"`；**绝不重放** |
| 已终态指令/任务 | 原样恢复（含结果/错误） |
| token | 在 TTL 内继续有效，所有者校验保持可用 |

事件游标与事件历史仍只在内存中（见 §6.2）。持久化文件包含 token，应视同凭证保护。

---

## 10. 安全默认值（shell 执行器）

- `shell` 仅在内置 demo 执行器上可用，且必须**同时**满足：`-allow-shell` 开启 + `-shell-allow <bin>[,...]` 非空白名单。
- 只接受 **argv 数组**（`{"cmd":["echo","hi"]}`）；字符串形式（会交给 `sh -c`）一律拒绝，因为白名单无法安全审计脚本内容。
- `argv[0]` 的 basename 必须命中白名单；未命中 → `-32040 feature.disabled`（`data.reason = "command_not_allowed"`）。
- 每次获准执行都会写入审计日志（节点、argv、所有者）；未启用时 demo 节点也不广告 `shell.exec` 能力。
- 真实节点的执行器由节点自己负责，引擎只下发方法与 params（见 §5.3）。