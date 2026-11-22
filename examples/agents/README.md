# examples/agents — 每个平台用一份 SDK 接入

Musubi 的定位是**多平台集控中心**：`engine` 是 core，三套 SDK 是各平台的接入依据，
Agent 是 SDK 里内置的参考节点运行时。每个平台只需选自己的 SDK、注册 handler、启动 Agent：

```
SDK Agent                     Musubi Core                     Web 控制台
─────────                     ───────────                     ──────────
node.register  ─────────────▶ 节点注册（platform 自报）
node.heartbeat ─────────────▶ 心跳保活                       平台 / 节点 / 席位
command.pull   ◀────────────▶ 指令队列（长轮询）              指令 / 任务 / 事件
command.complete ───────────▶ 终态回执
node.unregister ────────────▶ 退出（回收未完成指令）
```

引擎不回调节点：所有流量都是节点发起的标准 JSON-RPC，因此任意语言、任意网络方向都能接入。

## 三种官方 SDK 的 Agent

| 平台 | SDK | 示例 | 运行 |
|---|---|---|---|
| Linux / macOS / Windows / 嵌入式 | Go | [`go/main.go`](go/main.go) | `cd examples/agents/go && go run . -id go-1` |
| Node / Deno / Bun / Electron | TypeScript | [`ts/agent.mjs`](ts/agent.mjs) | `cd sdk/ts && npm run build`，然后 `node examples/agents/ts/agent.mjs --id node-1` |
| Linux / macOS / Windows | Python 3.8+ | [`python/agent.py`](python/agent.py) | `PYTHONPATH=sdk/python python3 examples/agents/python/agent.py --id py-1` |
| 浏览器 | TypeScript（同构） | 任意网页 `import { Agent } from '@musubi/hub-sdk'` | 需引擎允许 CORS 源（`-allow-origin`） |

每个示例都自带 `echo`、`sys.info`、`sleep`、`fail` 四个方法（`echo` / `platform.info`
由 Agent 内置），用于验证下发、成功、失败、超时。

## 快速验证

```bash
# 1. 启动集控中心
cd engine && CGO_ENABLED=0 go build -o hubd ./cmd/hubd && ./hubd -demo 1

# 2. 启动任意平台 Agent
cd examples/agents/go && go run . -id go-1
PYTHONPATH=sdk/python python3 examples/agents/python/agent.py --id py-1
node examples/agents/ts/agent.mjs --id node-1

# 3. 下发
curl -s -X POST http://127.0.0.1:8080/v1/rpc -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"command.send","params":{"node_id":"py-1","method":"sys.info"}}'
```

打开 <http://127.0.0.1:8080/console/>：

- **平台**页签会按 `platform.runtime` 生成平台卡片（go / node / python / demo），
  显示在线数、OS/arch、agent 版本、能力并集；
- **节点**页签新增平台列与平台过滤；
- **概览**显示平台总数与各平台在线比。

## 自定义一个平台

Agent 只要求实现四件事：注册、心跳、拉取、回执。以 Python 为例：

```python
from hub import Agent

agent = Agent(url="http://127.0.0.1:8080", node_id="kiosk-7",
              capabilities=["screen.show"], tags=["kiosk"])

@agent.handle("screen.show")
def show(params):
    return {"shown": params.get("text", "")}

agent.start(); agent.wait()
```

Go 用 `sdk.NewAgent` + `agent.Handle` + `agent.Run(ctx)`；TypeScript 用
`new Agent(options).handle(...).run()`。平台描述可显式传入，否则由 SDK 自动探测
（`os`、`arch`、`runtime`、`runtime_version`、`agent_version`）。

## 非官方语言

任何能发 JSON 的运行时都可以做节点：`POST /v1/register`（或 `node.register`）
→ `node.heartbeat` → `command.pull` → `command.complete`。参考
[`examples/worker`](../worker/)（纯 Go 标准库、不依赖 SDK）与
[docs/PROTOCOL.md](../../docs/PROTOCOL.md) §5.3。
