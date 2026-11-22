# examples/worker — 真实节点（拉取式下发）

一个仅用 Go 标准库的 worker，演示引擎与真实节点之间的完整链路：

```
POST /v1/register  →  node.heartbeat（每 3s）
                   →  command.pull {wait:5000}  领取指令（长轮询）
                   →  本地执行 echo / fail / sleep / uptime
                   →  command.complete          回报终态
                   →  node.unregister           Ctrl-C 退出
```

引擎不向节点建立回调连接，节点也不需要 SDK：只要能发 JSON-RPC，任何语言都能做 worker。

## 运行

```bash
# 终端 1：启动引擎（默认 demo 节点之外再加真实节点）
cd engine && go build -o /tmp/hubd ./cmd/hubd && /tmp/hubd -addr :8080 -demo 1

# 终端 2：启动 worker
cd examples/worker
go run . -url http://127.0.0.1:8080 -id w-1

# 终端 3：下发指令
curl -s -X POST http://127.0.0.1:8080/v1/rpc -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"command.send","params":{"node_id":"w-1","method":"echo","params":{"text":"hi"}}}'
```

打开 <http://127.0.0.1:8080/console/> 可以看到 `w-1` 的指令从 `pending → running → succeeded`。

## 参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `-url` | `http://127.0.0.1:8080` | 引擎地址 |
| `-token` | | token/basic 模式的 Bearer token；同时决定节点所有者 |
| `-id` | 引擎生成 | 节点 id；重复注册同一 id 为更新 |
| `-name` | 同 id | 展示名 |
| `-capabilities` | `echo,sleep,uptime` | 逗号分隔能力 |
| `-tags` | `worker,linux` | 逗号分隔标签 |
| `-heartbeat` | `3s` | 心跳间隔 |

## 语义提醒

- 引擎侧 `timeout` 从**下发**开始计时（包含排队），worker 应在超时前 `complete`。
- `command.complete` 幂等；过期结果不会覆盖已终态（例如引擎已判超时）。
- 引擎重启不会重放未完成指令；它们会被标记为失败（`engine.restarted`）。
- token 模式下，节点的注册者成为所有者，其他会话无法 pull 该节点的指令。
