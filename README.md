# Musubi — 多平台集控中心

Musubi 把**一个 Core、三套 SDK、一个控制台**拼成多平台集控中心：

- **Core**（`engine/`）：Go 单二进制、仅标准库，负责节点注册、席位路由、指令队列、任务编排、事件与持久化；
- **SDK**（`sdk/`）：Go / TypeScript / Python 三套接入依据，内置 **Agent 运行时**，每个平台用自己的一份 SDK 注册为受控节点；
- **控制台**（`/console/`）：集控中心的操作界面，按平台展示整个舰队，并直接下发指令/任务。

> 核心思想：**任何语言、任何运行时、任何传输、任何平台，只要能发 JSON 就能接入。**
> 契约唯一锚点：[docs/PROTOCOL.md](docs/PROTOCOL.md) ｜ 架构：[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) ｜ 支持矩阵：[docs/COMPATIBILITY.md](docs/COMPATIBILITY.md)

## 目录结构

```
engine/                Musubi Core（Go 单二进制，仅标准库）
  cmd/hubd/            入口（-addr / -auth / -token / -demo / -data-dir / -shell-allow）
  internal/jsonrpc/    JSON-RPC 2.0 信封与路由分发
  internal/hub/        核心：节点/指令/任务/事件/认证/席位/下发表/持久化/平台描述
  internal/ws/         极简标准库 WebSocket 服务端（RFC6455 最小子集）
  internal/httpapi/    HTTP：RPC / SSE / 长轮询 / 健康检查 / 节点注册（WS 请求并发处理）
  internal/console/    内嵌集控中心控制台（go:embed，零构建）
sdk/go/                Go SDK + Agent 运行时（仅标准库，无 CGO，go 1.20+）
sdk/ts/                TypeScript 同构 SDK + Agent（浏览器/Node/Deno/Bun/Electron）
sdk/python/            Python SDK + Agent（3.8+，默认零依赖）
schema/                JSON Schema 契约（信封/模型/平台/方法/能力/错误码）
examples/agents/       三平台 SDK Agent 示例（Go / TypeScript / Python）
examples/electron/     Electron 主进程 + preload + 渲染进程接入示例
examples/tui/          终端 TUI demo（Go 单文件、零依赖、事件驱动）+ 无头脚本入口
examples/worker/       不依赖 SDK 的原生 worker（协议四步：注册/心跳/pull/complete）
tests/                 契约测试 + 多语言一致性起爆 + 兼容性矩阵起爆
docs/                  PROTOCOL 规范 + ARCHITECTURE + 兼容性矩阵
```

## 快速开始

### 1. 编译并启动 Go 引擎

```bash
export PATH="$PATH:/usr/local/go/bin"
cd engine && CGO_ENABLED=0 go build -o hubd ./cmd/hubd   # 单二进制，< 10MB
./hubd -addr :8080 -demo 3                              # 3 个模拟节点（echo/fail/sleep）
./hubd -addr :8080 -demo 3 -data-dir ./state            # 持久化节点/席位/任务/指令
./hubd -addr :8080 -demo 3 -allow-shell -shell-allow echo,date   # 白名单式 argv shell
./hubd -addr :8080 -auth token -token secret-token      # token 认证
```

交叉编译（Linux/macOS/Windows × x86_64/arm64/arm）：

```bash
cd engine
GOOS=linux  GOARCH=arm64 CGO_ENABLED=0 go build -o hubd-linux-arm64  ./cmd/hubd
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o hubd.exe ./cmd/hubd
```

### 2. 任意语言最小接入（HTTP + JSON，仅 curl）

```bash
curl -s -X POST http://127.0.0.1:8080/v1/rpc -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"engine.info","params":{"client_info":{"protocol_versions":["1.0"]}}}'
# → {"jsonrpc":"2.0","id":1,"result":{"protocol_version":"1.0", ...}}
```

### 3. 三个 SDK 五秒上手

```go
// sdk/go，见 sdk/go/README.md
c, _ := sdk.New(sdk.Options{URL: "http://127.0.0.1:8080"})
info, err := c.Info(ctx, sdk.ClientInfo{ProtocolVersions: []string{"1.0"}})
cmd, err := c.CommandSend(ctx, sdk.CommandSpec{NodeID: "n-1", Method: "echo", Params: map[string]any{"text": "hi"}})
```

```ts
// sdk/ts，浏览器 / Node / Deno / Bun / Electron 同构
const hub = new HubClient({ url: "http://127.0.0.1:8080" });
await hub.open();
const nodes = await hub.nodeList();
const cmd = await hub.commandSend({ node_id: "n-1", method: "echo", params: { text: "hi" } });
hub.on("node.updated", (ev) => console.log(ev.data));  // 按事件类型分发
hub.on("*", (ev) => console.log(ev.type, ev));         // 全量（含未知类型）；无 "event" 这个 type
```

```python
# sdk/python，零依赖
c = HubClient("http://127.0.0.1:8080")
info = c.info({"protocol_versions": ["1.0"]})
cmd = c.command_send("n-1", "echo", {"text": "hi"})
```

SDK 同时内置 **Agent 运行时**，让每个平台用一份 SDK 就成为受控节点（自动上报 `platform`）：

```go
// Go：Linux / macOS / Windows / 嵌入式
a := sdk.NewAgent(sdk.AgentOptions{URL: "http://127.0.0.1:8080", NodeID: "go-1"})
a.Handle("sys.info", func(ctx context.Context, p map[string]any) (any, error) { return sysInfo(), nil })
a.Run(ctx)   // 注册 → 心跳 → pull → 执行 → complete → 注销
```

```ts
// TS：Node / Deno / Bun / Electron / 浏览器
const agent = new Agent({ url: 'http://127.0.0.1:8080', nodeId: 'node-1' });
agent.handle('sys.info', async () => sysInfo());
await agent.run();
```

```python
# Python
agent = Agent(url="http://127.0.0.1:8080", node_id="py-1")
agent.handle("sys.info", lambda p: sys_info())
agent.start()
```

完整可运行示例：[examples/agents/](examples/agents/)。

### 4. Electron 接入

```bash
cd sdk/ts && npm install && npm run build      # 构建 TS SDK（ESM/CJS/UMD）
cd ../../examples/electron
npm install electron && npm start              # 需先启动引擎（步骤 1）
```

架构：**主进程**持有连接（Node WebSocket）→ `ipcMain.handle('hub:invoke')` → **preload** `contextBridge.exposeInMainWorld('hub')` → **渲染进程** `window.hub.invoke()` 调用集控接口、`onEvent` 接收推送。详见 [examples/electron/README.md](examples/electron/README.md)。

### 5. 终端 TUI demo（能跑通的带界面示例）

零依赖、Go 单文件实现，把引擎的状态与事件流实时渲染在终端里：

```bash
cd examples/tui && GOFLAGS=-mod=mod CGO_ENABLED=0 go build -o hubtui .
./hubtui --url http://127.0.0.1:8080        # 首选 WS 传输，事件流驱动界面
./hubtui --transport ws|http|poll           # 显式指定传输（默认 auto → ws）
./hubtui --render-once --no-color           # 输出单帧（无 TTY 场景/CI）
```

命令语言：`send n-1 echo {"text":"hi"}` / `batch n-2:echo,n-2:fail` / `task demo` /
`cancel` / `taskcancel` / `nodes` / `tasks` / `refresh` / `dump` / `wait <ms>` / `help` / `quit`。

**无头脚本入口**（管道 stdin 或 `--script`），供 CI 验证：

```bash
./hubtui --script 'nodes;send n-1 echo {"text":"hello tui"};wait 600;dump;quit'
# COMMANDS (n) 与 EVENTS (latest 5) 面板即时反映 send 的异步终态
```

界面模型完全由 `event.subscribe` 事件流驱动（辅以 2s 轮询兜底）：节点的注册/上下线、
指令 accepted→started→result、任务进度都来自引擎推送，界面不做本地推断。

### 6. 多平台 Agent（SDK 是接入依据）

每个平台选一份 SDK，注册 handler 即成为节点；`platform` 由 Agent 自动探测并上报。

```bash
# Go（Linux / macOS / Windows / 嵌入式）
cd examples/agents/go && go run . -url http://127.0.0.1:8080 -id go-1

# TypeScript（Node / Deno / Bun / Electron；浏览器同样可用）
cd sdk/ts && npm install && npm run build
node examples/agents/ts/agent.mjs --url http://127.0.0.1:8080 --id node-1

# Python
PYTHONPATH=sdk/python python3 examples/agents/python/agent.py --url http://127.0.0.1:8080 --id py-1
```

三者都会出现在控制台「平台」页签，按 `go` / `node` / `python` 分组。下发示例：

```bash
curl -s -X POST http://127.0.0.1:8080/v1/rpc -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"command.send","params":{"node_id":"py-1","method":"sys.info"}}'
```

也可以把指令发给席位，由当前占位节点执行：

```bash
# seat.create → seat.bind go-1 → command.send {"seat_id":"s-...",...}
```

**任意其他语言**不依赖 SDK 也能接入：`examples/worker` 用纯 Go 标准库演示
"注册 → 心跳 → `command.pull` 长轮询 → 本地执行 → `command.complete`"四步。
各 SDK 的 Agent 内部做的就是这四步（PROTOCOL.md §5.4）。

### 7. 集控中心控制台

引擎自带零构建控制台，打开 <http://127.0.0.1:8080/console/> 即可：

- **平台**：按运行时分组的平台舰队（在线数、OS / 架构、agent 版本、能力、节点列表）
- **概览 / 节点**：舰队规模、在线率、平台分布；节点表含平台列与平台过滤
- **席位 / 指令 / 任务**：创建/绑定/退席；按 `node_id` 或 `seat_id` 下发；任务编排
- **事件流 / RPC**：WebSocket 实时推送（SSE 回退）、原始 JSON-RPC 调用；token/basic 在右上角「连接」里填凭证

控制台是静态资源，不改变 API 鉴权；`/` 会自动跳转到 `/console/`。

### 8. 测试

```bash
bash tests/run_all.sh           # 契约测试(none+token 两轮) + schema + 三语言一致性起爆 + 矩阵起爆
```

## 架构：Core / SDK Agent / 控制台

```
Web 控制台  ──▶  Musubi Core（engine：节点/席位/指令/任务/事件/持久化）
                     ▲   node.register / heartbeat / command.pull / complete
                     │   （节点主动发起，Core 从不回调）
        ┌────────────┼────────────┬──────────────────┐
     Go SDK Agent  TS SDK Agent  Python SDK Agent   任意语言（按契约四步）
```

- **Core** 是唯一状态与调度中心，单二进制、仅标准库；默认纯内存，`-data-dir` 可选持久化。
- **SDK** 是各平台的接入依据：Client 封装协议，Agent 提供节点运行时；平台差异只存在于
  `platform` 探测与传输层。
- **控制台** 提供舰队视图与指令操作，界面资源由引擎内嵌。

详见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。平台支持：

| 平台 | 接入方式 | 状态 |
|---|---|---|
| Linux / macOS / Windows（服务端、桌面、嵌入式） | Go SDK Agent | ✅ |
| Linux / macOS / Windows（服务端、桌面） | Python SDK Agent | ✅ |
| Node.js / Deno / Bun / Electron | TypeScript SDK Agent | ✅ |
| 浏览器 | TypeScript SDK Agent（CORS 放行后） | ✅ |
| 任意其他语言/运行时 | 契约四步：`node.register` → `heartbeat` → `pull` → `complete` | ✅ |

## 极致兼容性一览

| 维度 | 承诺 |
|---|---|
| 语言 | 任何能发 JSON / 能开 WS 或 HTTP 的语言（JSON-RPC 2.0 为最小公分母） |
| 运行时 | 浏览器、Node ≥18、Deno、Bun、Electron ≥22、Python ≥3.8、Go ≥1.20 |
| 传输 | WebSocket 主 + HTTP 回退 + SSE + 长轮询，SDK 自动降级 `ws → sse → poll` |
| 编码 | JSON 默认，MessagePack/Protobuf 可协商（可插拔编码接口） |
| 版本 | 协商取最高共同版本；只增不删；未知字段/未知事件一律忽略（向前+向后兼容） |
| 网络 | 心跳、断线重连（指数退避）、离线队列、幂等键去重、代理/自签名/TLS |
| 平台 | Linux / macOS / Windows × x86_64 / arm64 / arm；CGO_ENABLED=0 静态交叉编译 |
| 部署 | 单二进制（< 10MB）、Docker、无 root、只读文件系统（默认纯内存） |
| 认证 | none / token / basic 可插拔，OAuth2 / mTLS 预留接口 |
| 节点执行 | demo 节点进程内模拟；真实节点 `command.pull` / `command.complete` 拉取式下发，任何语言可实现 worker |
| 多平台 | 节点注册带可选 `platform`（os/arch/runtime/version/labels）；三套 SDK 内置 Agent 自动探测 |
| 席位路由 | `command.send` / `batch` / `task.create` 可用 `seat_id` 寻址，引擎解析到占位节点；空席 `-32053` |
| 状态 | 默认纯内存；`-data-dir` 原子快照（1s 防抖），重启恢复且**不重放**未完成指令 |
| Shell | 默认关闭；`-allow-shell` + `-shell-allow` 双重 opt-in，仅 argv、白名单、审计日志 |

## 测试覆盖

- `tests/contract/cases/*.json` — 语言无关契约用例（9 组：engine/auth/nodes/commands/tasks/events/batch/seats/platform）
- `tests/run_contract.py` — 契约运行器（JSON 路径断言、`mode` 门控、batch 单项错误、事件驱动终态）
- `tests/compat/detonate_{py,ts,go}` — 三语言**核心场景等价覆盖**的一致性起爆（各语言实现细节略有差异）
- `tests/compat/matrix_detonate.sh` — 兼容性铁律起爆（未知字段忽略、版本不匹配、SSE、批处理、断线重连）
- `tests/schema_validate.py` — 引擎响应/事件对 `schema/*.json` 的结构校验
- `examples/tui --script` — TUI demo 无头脚本回归（COMMANDS/EVENTS 面板断言）

完整结果（`bash tests/run_all.sh`）：契约 **none 66/66 + token 72/72**（none 模式跳过 6 个 token 专属用例）、
Schema 全过、Python **28/28**、TypeScript **15/15**、Go **26/26**、矩阵起爆全过。

## 席位（Seat）— 可拓展性的代名词

引擎的一切扩展点以「席位」接入（PROTOCOL.md §4.7）：**常席**（standing）已被实体占用，**虚席**（vacant）
预留待补。虚席可声明期望画像（`expect`），`auto` 策略下匹配节点注册即自动补席，`manual` 策略走
`seat.bind`；退席后壳位与画像保留，位置持久、占位者流动。方法：`seat.list/create/get/delete/bind/unbind`；
事件：`seat.created/filled/released/deleted`；能力：`seat.registry`、`seat.routing`。

**席位参与路由**：指令和任务条目可以直接用 `seat_id` 替代 `node_id`，引擎把它解析为当前占位节点
（指令对象同时记录执行节点与路由席位）；空席没有执行者，返回 `-32053 seat.vacant`。这让"位置"成为
稳定的调用目标：节点上下线、换人补席时，调用方无需改地址。

## License

Apache-2.0（见 LICENSE）