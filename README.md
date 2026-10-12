# Musubi Control Hub — 极致兼容的跨语言集控接口

轻量、可复用、跨语言、**极致兼容**的集控系统：**Go 单二进制引擎** + 语言无关的 **JSON-RPC 2.0 契约** + 三套 SDK（Go / TypeScript / Python，全部协议方法均可调用；离线队列与 token 轮换三端行为一致）+ **Electron 接入示例**。

> 核心思想：**任何语言、任何运行时、任何传输、任何平台，只要能发 JSON 就能接入。**
> 契约唯一锚点：[docs/PROTOCOL.md](docs/PROTOCOL.md) ｜ 支持矩阵：[docs/COMPATIBILITY.md](docs/COMPATIBILITY.md)

## 目录结构

```
engine/                Go 集控引擎（仅标准库，单二进制）
  cmd/hubd/            入口（-addr / -auth / -token / -demo / -verbose）
  internal/jsonrpc/    JSON-RPC 2.0 信封与路由分发
  internal/hub/        引擎核心：节点/指令/任务/事件总线/认证/版本协商
  internal/ws/         极简标准库 WebSocket 服务端（RFC6455 最小子集）
  internal/httpapi/    HTTP：RPC / SSE / 长轮询 / 健康检查 / 节点注册
sdk/go/                Go 客户端 SDK（仅标准库，无 CGO，go 1.20+）
sdk/ts/                TypeScript 同构 SDK（浏览器/Node/Deno/Bun/Electron；ESM/CJS/UMD）
sdk/python/            Python SDK（3.8+，默认零依赖）
schema/                JSON Schema 契约（信封/模型/方法/能力/错误码）
examples/electron/     Electron 主进程 + preload + 渲染进程接入示例
examples/tui/          终端 TUI demo（Go 单文件、零依赖、事件驱动）+ 无头脚本入口
tests/                 契约测试 + 多语言一致性起爆 + 兼容性矩阵起爆
docs/                  PROTOCOL 规范 + 兼容性矩阵
```

## 快速开始

### 1. 编译并启动 Go 引擎

```bash
export PATH="$PATH:/usr/local/go/bin"
cd engine && CGO_ENABLED=0 go build -o hubd ./cmd/hubd   # 单二进制，< 10MB
./hubd -addr :8080 -demo 3                              # 3 个模拟节点（echo/fail/sleep）
./hubd -addr :8080 -demo 3 -allow-shell                 # 额外启用 shell（会以本进程用户执行主机命令）
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
// sdk/go —— 见 sdk/go/README.md
c, _ := sdk.New(sdk.Options{URL: "http://127.0.0.1:8080"})
info, err := c.Info(ctx, sdk.ClientInfo{ProtocolVersions: []string{"1.0"}})
cmd, err := c.CommandSend(ctx, sdk.CommandSpec{NodeID: "n-1", Method: "echo", Params: map[string]any{"text": "hi"}})
```

```ts
// sdk/ts —— 浏览器 / Node / Deno / Bun / Electron 同构
const hub = new HubClient({ url: "http://127.0.0.1:8080" });
await hub.open();
const nodes = await hub.nodeList();
const cmd = await hub.commandSend({ node_id: "n-1", method: "echo", params: { text: "hi" } });
hub.on("node.updated", (ev) => console.log(ev.data));  // 按事件类型分发
hub.on("*", (ev) => console.log(ev.type, ev));         // 全量（含未知类型）；无 "event" 这个 type
```

```python
# sdk/python —— 零依赖
c = HubClient("http://127.0.0.1:8080")
info = c.info({"protocol_versions": ["1.0"]})
cmd = c.command_send("n-1", "echo", {"text": "hi"})
```

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
指令 accepted→started→result、任务进度都直接来自引擎推送，而非本地猜测。

### 6. 测试

```bash
bash tests/run_all.sh           # 契约测试(none+token 两轮) + schema + 三语言一致性起爆 + 矩阵起爆
```

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

## 测试覆盖

- `tests/contract/cases/*.json` — 语言无关契约用例（8 组：engine/auth/nodes/commands/tasks/events/batch/seats）
- `tests/run_contract.py` — 契约运行器（JSON 路径断言、`mode` 门控、batch 单项错误、事件驱动终态）
- `tests/compat/detonate_{py,ts,go}` — 三语言**核心场景等价覆盖**的一致性起爆（各语言实现细节略有差异）
- `tests/compat/matrix_detonate.sh` — 兼容性铁律起爆（未知字段忽略、版本不匹配、SSE、批处理、断线重连）
- `tests/schema_validate.py` — 引擎响应/事件对 `schema/*.json` 的结构校验
- `examples/tui --script` — TUI demo 无头脚本回归（COMMANDS/EVENTS 面板断言）

完整结果（`bash tests/run_all.sh`）：契约 **none 56/56 + token 62/62**（none 模式跳过 6 个 token 专属用例）、
Schema 全过、Python **28/28**、TypeScript **15/15**、Go **26/26**、矩阵起爆全过。

## 席位（Seat）— 可拓展性的代名词

引擎的一切扩展点以「席位」接入（PROTOCOL.md §4.7）：**常席**（standing）已被实体占用，**虚席**（vacant）
预留待补。虚席可声明期望画像（`expect`），`auto` 策略下匹配节点注册即自动补席，`manual` 策略走
`seat.bind`；退席后壳位与画像保留，位置持久、占位者流动。方法：`seat.list/create/get/delete/bind/unbind`；
事件：`seat.created/filled/released/deleted`；能力：`seat.registry`。

## License

Apache-2.0（见 LICENSE）