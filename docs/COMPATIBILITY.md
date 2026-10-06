# 兼容性矩阵 (COMPATIBILITY)

> 目标：任何语言、任何运行时、任何传输、任何平台，只要能发 JSON 就能接入 Control Hub。
> 图例：✅ 开箱即用（默认/已实现） · 🟡 可选（按需启用/需安装扩展依赖） · ⭕ 未实现（预留，按契约可实现）

---

## 1. 语言 / 运行时支持矩阵

| 语言 | SDK 状态 | 最小版本 | JSON-RPC over WS | over HTTP | 事件订阅 | 断线重连/退避 | 离线队列 | 说明 |
|---|---|---|---|---|---|---|---|---|
| Go | ✅ `sdk/go` | 1.20+，无 CGO | ✅ 内置极简 WS 客户端 | ✅ | ✅ 轮询+WS | ✅ | ✅ | 单文件依赖，仅标准库 |
| TypeScript | ✅ `sdk/ts` | Node ≥ 18 / 现代浏览器 | ✅ 自动适配（Node 原生 WebSocket / 浏览器 WebSocket / 可选 `ws` 包） | ✅ fetch | ✅ SSE+WS | ✅ | ✅ | 同构：ESM/CJS/UMD 三构建 |
| JavaScript | ✅ 同 TS SDK | Node ≥ 18 | ✅ | ✅ | ✅ | ✅ | ✅ | 消费 `dist/` 产物 |
| Python | ✅ `sdk/python` | 3.8+ | 🟡 安装 `websocket-client` 后可用 | ✅ urllib（0 依赖） | ✅ 长轮询+SSE | ✅ | ✅ | 默认零第三方依赖 |
| Rust / C# / Java / C/C++ | ⭕ | — | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | 按 `docs/PROTOCOL.md` + `schema/` 自行实现（契约已稳定） |
| 任何语言 | ✅ | — | 🟡 | ✅ | 🟡 | 🟡 | 🟡 | 最小接入：HTTP POST `/v1/rpc` + 轮询 `/v1/poll`，仅需 JSON |

## 2. 运行时矩阵（TS SDK 同构性）

| 运行时 | WebSocket | HTTP/SSE | 自动检测 | 备注 |
|---|---|---|---|---|
| 浏览器（Chrome/Edge/Firefox/Safari） | ✅ 原生 | ✅ fetch + EventSource | ✅ | UMD 或 ESM |
| Node.js ≥ 18 | ✅ 原生全局 WebSocket（Node ≥ 21 稳定；18/20 用 `ws` 包回退） | ✅ fetch | ✅ | CJS/ESM |
| Deno | ✅ 原生 | ✅ fetch | ✅ | ESM |
| Bun | ✅ 原生 | ✅ fetch | ✅ | ESM |
| Electron 主进程 | ✅（同 Node） | ✅ | ✅ `process.type === 'browser'` | 见 §4 |
| Electron 渲染进程 | ✅ 浏览器 WebSocket | ✅ | ✅ | preload + contextBridge |
| Electron preload | ✅ 透传主进程（IPC） | — | ✅ | 不直接建连，走 IPC |

> TS SDK 最低支持：**Node ≥ 18 / Electron ≥ 22（内置 Node 16.17+ 可运行，推荐 ≥ 28）**。浏览器要求支持 ES2020。
> 自动适配优先级：`process.type`（Electron）→ `globalThis.WebSocket` 存在性 → 注入适配器。

## 3. 传输矩阵

| 传输 | 引擎 | Go SDK | TS SDK | Python SDK | 说明 |
|---|---|---|---|---|---|
| WebSocket `/v1/ws` | ✅ 标准库自实现（无第三方依赖） | ✅ | ✅ | 🟡 websocket-client | 主传输，含 ping/pong、二进制帧（msgpack） |
| HTTP POST `/v1/rpc` | ✅ | ✅ | ✅ | ✅ | 回退传输，同步请求响应 |
| SSE `/v1/events` | ✅ | ✅ | ✅ | ✅ | 服务端推送；Node 用 fetch 流，浏览器用 EventSource |
| 长轮询 `/v1/poll` | ✅ | ✅ | ✅ | ✅ | 最弱网络兜底，cursor 增量 |
| MQTT / NATS / stdio / gRPC-Web | ⭕ 预留 | ⭕ | ⭕ | ⭕ | Transport 接口实现即可接入，业务层不变 |

降级顺序（SDK 内置）：`ws → sse → poll`，由 `server_info.transports` 与本地支持求交集决定。

## 4. Electron 接入矩阵

| 场景 | 接入方式 | 支持 |
|---|---|---|
| 主进程 | `HubClient` Node 传输（Node 原生/`ws`） | ✅ |
| 渲染进程 | `HubClient` 浏览器传输（原生 WebSocket） | ✅ |
| 推荐架构 | 主进程持连接，preload `contextBridge.exposeInMainWorld('hub', ...)`，渲染进程 `window.hub.invoke()`（`ipcRenderer.invoke` → `ipcMain.handle` → SDK） | ✅ |
| 旧版兼容 | Electron ≥ 22（Node 16.17）可用 `ws` 包；SDK 内部自动回退 | ✅ |

## 5. 编码矩阵

| 编码 | 引擎 | Go SDK | TS SDK | Python SDK | 协商方式 |
|---|---|---|---|---|---|
| JSON | ✅ 默认 | ✅ 默认 | ✅ 默认 | ✅ 默认 | 最小公分母，始终可用 |
| MessagePack | 🟡 插件（`internal/encoding` 接口） | 🟡 | 🟡 | 🟡 | `client_info.encodings` 协商；WS 二进制帧 / HTTP `application/msgpack` |
| Protobuf | 🟡 插件 | 🟡 | 🟡 | 🟡 | 同上 |

编码层为可插拔接口（`Encoder` / `Decoder`），默认仅编译 JSON，不增加二进制体积。

## 6. 平台 / 部署矩阵（Go 引擎）

| 平台 | GOOS/GOARCH | 状态 |
|---|---|---|
| Linux x86_64 / arm64 / armv7 | `linux/amd64,arm64,arm` | ✅ CGO_ENABLED=0 交叉编译 |
| macOS x86_64 / arm64 | `darwin/amd64,arm64` | ✅ |
| Windows x86_64 / arm64 | `windows/amd64,arm64` | ✅ |
| 嵌入式（只读 FS、无 root） | 任意 | ✅ 默认不写文件；`-store sqlite` 才需要写权限 |

| 部署形态 | 支持 | 说明 |
|---|---|---|
| 单二进制 | ✅ | `hubd`，目标 < 20MB（默认无 SQLite 约 6–8MB） |
| Docker | ✅ | 提供 `Dockerfile`（多阶段，`scratch`/`distroless`） |
| 系统服务 | ✅ | systemd unit 示例见 `docs/`；前台/后台模式 |
| 无 root / 只读文件系统 | ✅ | 默认纯内存运行，不落盘 |
| 反向代理 / TLS | ✅ | 可置于 nginx/网关后；SDK 支持代理与自签名 CA |

## 7. 协议版本支持矩阵

| 协议版本 | 引擎 | Go SDK | TS SDK | Python SDK | 说明 |
|---|---|---|---|---|---|
| `1.0` | ✅ | ✅ | ✅ | ✅ | 当前基线 |
| 未来 `1.x` | ✅ 策略保证 | ✅ 策略保证 | ✅ 策略保证 | ✅ 策略保证 | 只增字段/方法/事件，旧客户端忽略未知 |
| 未来 `2.x` | 🟡 双版本共存 | 🟡 | 🟡 | 🟡 | `supported_versions` 列表，按 `engine.info` 协商 |

协商规则：取双方版本交集最高值；无交集 → `-32005`。SDK 固定声明 `protocol_versions: ["1.0"]`，对未知字段一律忽略 → **向前兼容**；引擎忽略未知请求字段 → **向后兼容**。

## 8. 认证 / 安全矩阵

| 模式 | 引擎 | 客户端支持 | 说明 |
|---|---|---|---|
| none | ✅ 默认 | ✅ | 匿名 |
| token | ✅ `-auth token` | ✅ `token` 配置 | Bearer |
| basic | ✅ `-auth basic` | ✅ | 用户名/口令 |
| mTLS | 🟡 反向代理层 | 🟡 `ca`/证书配置 | 走网关 |
| OAuth2 | 🟡 Auth 接口插件 | 🟡 | 预留 |

## 9. 一致性测试矩阵（tests/）

| 测试 | 覆盖 | 语言 |
|---|---|---|
| 契约用例（JSON 驱动） | 全部方法、错误码、事件、协商 | 语言无关（`tests/contract/cases/*.json`） |
| 多语言一致性 | 同一用例集跑 Go/TS/Python SDK，比对结果 | Go + Node + Python runner |
| 兼容性矩阵冒烟 | 传输降级、未知字段忽略、旧事件忽略、断线重连 | 引擎 + TS SDK |

> 说明：矩阵中 🟡/⭕ 项不影响默认"极致兼容"承诺——所有未实现项都可通过契约（JSON + WS/HTTP）接入，或通过已定义的可插拔接口（Transport / Encoder / Auth）启用。