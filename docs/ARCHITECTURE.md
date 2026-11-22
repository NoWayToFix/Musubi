# Musubi 架构：Core + SDK Agents + 控制台

Musubi 是一个**多平台集控中心**。它由三层组成，边界固定、只通过 JSON-RPC 2.0 契约通信：

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Web 控制台（集控中心控制台）                    │
│  /console/  平台舰队 · 节点 · 席位 · 指令 · 任务 · 事件流 · RPC        │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ 同源 HTTP + WebSocket / SSE
┌───────────────────────────────▼──────────────────────────────────────┐
│                         Musubi Core（engine）                        │
│  节点注册表 · 席位路由 · 指令队列 · 任务编排 · 事件总线 · 认证 · 持久化 │
│  传输：WS / HTTP POST / SSE / 长轮询                                   │
└───────┬───────────────────────────────┬──────────────────────────────┘
        │  node.register / heartbeat    │  command.pull / complete
        │  （节点主动发起，core 不回调） │
┌───────▼──────────┐   ┌────────────────▼───┐   ┌─────────────────────┐
│ Go SDK Agent     │   │ TS SDK Agent       │   │ Python SDK Agent    │
│ Linux/macOS/Win  │   │ Node/Deno/Bun/     │   │ Linux/macOS/Win     │
│ 嵌入式/服务端     │   │ Electron/浏览器     │   │ 服务端/桌面          │
└──────────────────┘   └────────────────────┘   └─────────────────────┘
        ▲                        ▲                        ▲
   任意其他语言：按契约四步接入（node.register → heartbeat → pull → complete）
```

## 1. Core（`engine/`）

Core 是唯一的状态与调度中心，单二进制、仅标准库、默认纯内存（`-data-dir` 可选持久化）。

| 子系统 | 职责 |
|---|---|
| `internal/hub/nodes.go` | 节点注册表、平台描述、心跳/看门狗、所有权 |
| `internal/hub/commands.go` | 指令生命周期、幂等键、席位路由、demo 执行器 |
| `internal/hub/dispatch.go` | 真实节点队列、长轮询、超时/取消/下线回收 |
| `internal/hub/tasks.go` | 任务编排与进度 |
| `internal/hub/seats.go` | 常席/虚席、自动补席、席位寻址 |
| `internal/hub/events.go` | 事件总线、订阅过滤、事件历史 |
| `internal/hub/store.go` | 原子快照与重启恢复（不重放） |
| `internal/httpapi/` | WS/HTTP/SSE/长轮询、注册端点、控制台挂载 |
| `internal/console/` | 内嵌控制台静态资源（go:embed） |

**核心约束**

- 契约只增不删：未知字段/事件/能力值一律忽略（`docs/PROTOCOL.md` §8）。
- Core **从不主动连接节点**：所有节点流量都是节点发起的请求，因此 NAT/防火墙/单向网络均可接入。
- 指令语义是**至多一次**：重启不重放；未完成指令标记失败，由上层决定是否重发。
- 席位是稳定寻址层：节点上下线、换人补席不影响调用方地址。

## 2. SDK 层（`sdk/`）是每个平台的接入依据

三套官方 SDK 都包含两部分：

1. **Client**：协议全量封装（传输协商、重连、离线队列、事件订阅、token 轮换）；
2. **Agent**：真实节点的参考运行时（注册 → 心跳 → 拉取 → 执行 → 回执 → 注销）。

| SDK | 适用平台 | Agent 入口 |
|---|---|---|
| Go | Linux / macOS / Windows / 嵌入式 / 服务端 | `sdk.NewAgent` + `agent.Handle` + `agent.Run(ctx)` |
| TypeScript | Node ≥18、Deno、Bun、Electron、浏览器 | `new Agent(opts)` + `agent.handle` + `agent.run()` |
| Python | Linux / macOS / Windows，Python 3.8+ | `Agent(...)` + `agent.handle` + `agent.start()` |

Agent 自动探测 `platform`（os/arch/runtime/runtime_version/agent_version），业务只需注册 handler。
示例见 [`examples/agents/`](../examples/agents/)；不依赖 SDK 的最小协议实现见
[`examples/worker/`](../examples/worker/)。

**为什么以 SDK 为接入依据**

- 同一套语义在三端一致：所有者、幂等、超时、重连、离线队列；
- 平台差异被收敛在 SDK 的 platform 探测与传输层，业务 handler 与平台无关；
- 非官方语言只需实现 §5.4 的四步生命周期，即可获得同样的控制台体验。

## 3. 控制台（`/console/`）

控制台是集控中心的操作界面：展示整个舰队的状态与调度，不做单点调试。

| 页签 | 内容 |
|---|---|
| 概览 | 平台数、节点在线率、席位、运行中任务、事件量、平台分布 |
| 平台 | 按 `platform.runtime` 分组的平台舰队：在线数、OS / 架构、agent 版本、能力并集、节点列表 |
| 节点 | 平台列 + 平台过滤 + 状态/标签/能力/席位/心跳 |
| 席位 | 创建虚席、绑定/退席/删除；席位即可作为指令寻址目标 |
| 指令 | 按 `node_id` 或 `seat_id` 下发，实时状态与结果 |
| 任务 | 串行/并行任务编排与进度 |
| 事件流 | WebSocket 推送（SSE 回退），按类型、节点、席位、任务、指令过滤 |
| RPC | 原始 JSON-RPC 调用 |

控制台为静态资源（`go:embed`，零构建），不改变 API 鉴权；操作走与 SDK 相同的
`/v1/rpc`，事件先连 `/v1/ws`，失败时回退到 `/v1/events` 的 SSE。

## 4. 一次指令的完整路径

```
调用方（SDK/控制台/curl）
  │ command.send {seat_id | node_id, method, params, timeout}
  ▼
Core：解析席位 → 记录 command(pending) → 发布 command.accepted
  │
  ├─ demo 节点：进程内模拟执行 → succeeded/failed
  └─ 真实节点：进入节点队列
        │ command.pull {node_id, wait}
        ▼
     SDK Agent：领取（running）→ handler(params) → command.complete
        │
        ▼
     Core：写入终态 → 发布 command.result/failed → 推进任务进度
```

## 5. 部署形态

- **单机**：`hubd` 一个二进制 + 控制台，Agent 跑在各终端；
- **中心/边缘**：Core 部署在可达位置，边缘节点主动 pull，无需入站端口；
- **多平台混合**：同一 Core 下同时挂 Go/Python/Node/浏览器 Agent，用 `platform` 与席位分组；
- **持久化**：`-data-dir` 保存节点/席位/任务/指令/token；重启后节点 offline、席位退为虚席、未完成指令失败不重放。

## 6. 扩展点

| 需求 | 扩展方式 |
|---|---|
| 新平台 | 用现成 SDK 写 Agent（推荐），或按 §5.4 实现四步协议 |
| 新传输 | Core 侧 Transport 接口；SDK 侧 Transport 适配器 |
| 新认证 | Auth 接口插件（OAuth2/mTLS 预留） |
| 新编码 | Encoder/Decoder 接口（MessagePack/Protobuf 预留） |
| 新业务能力 | 节点 handler + `capabilities` 自报；Core 不感知业务方法 |
| 新调度策略 | 席位 `expect`/`bind_policy` + 上层编排；Core 只做寻址与队列 |
