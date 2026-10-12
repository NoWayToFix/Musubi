# musubi/engine — Control Hub (Go 实现)

Go 版集控引擎（Control Hub），严格遵循仓库根目录 [`docs/PROTOCOL.md`](../docs/PROTOCOL.md)（唯一契约锚点）与 [`schema/`](../schema)。仅使用 Go 标准库，无任何第三方依赖。

## 架构

```
cmd/hubd/            CLI 守护进程（入口）
internal/jsonrpc/    JSON-RPC 2.0 信封 + 方法路由（-32700/-32600/-32601/-32602）
internal/ws/         极简标准库 WebSocket 服务端（RFC6455 子集）
internal/httpapi/    HTTP 传输层：/v1/rpc /v1/events(SSE) /v1/poll /v1/health /v1/register /v1/ws
internal/hub/        Hub 核心
  engine.go           Hub 结构、方法注册、消息分发、后台看门狗
  auth.go             认证：none/token/basic、access/refresh token、TTL 1h
  nodes.go            节点注册表（注册/心跳/列出/下线、busy 预留、看门狗）
  commands.go         指令：send/batch/cancel、幂等 5min、模拟执行器 echo/fail/sleep/shell
  tasks.go            任务：create/get/list/cancel、串行/并行、进度与终态
  events.go           事件总线：原子 id/cursor、订阅过滤、非阻塞投递、长轮询历史
  negotiate.go        版本协商 + engine.info/ping/capabilities
```

依赖方向：`cmd/hubd` → `httpapi` → `hub` / `ws` / `jsonrpc`。无循环依赖。

## 构建 / 交叉编译

```bash
export PATH=$PATH:/usr/local/go/bin
cd engine

go build ./...            # 构建全部
go vet ./...              # 静态检查
go build -o hubd ./cmd/hubd

# 交叉编译（示例）
GOOS=linux   GOARCH=arm64 go build -o hubd-linux-arm64 ./cmd/hubd
GOOS=darwin  GOARCH=amd64 go build -o hubd-darwin-amd64 ./cmd/hubd
GOOS=windows GOARCH=amd64 go build -o hubd.exe ./cmd/hubd
```

## 运行

```bash
./hubd -addr :8080 -auth none -demo 3          # 匿名模式（默认）
./hubd -addr :8080 -auth token -token secret   # 静态 Token 模式
./hubd -addr :8080 -auth basic -user admin -pass pw
./hubd -verbose                                # 开启 debug 日志
./hubd -addr :8080 -demo 3 -allow-shell        # 显式开启主机 shell 执行器
```

CLI 参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `-addr` | `:8080` | 监听地址 |
| `-auth` | `none` | `none` / `token` / `basic` |
| `-token` | | token 模式静态密钥 |
| `-user` / `-pass` | | basic 模式账号口令 |
| `-demo` | `3` | 模拟节点数 `n-1..n-N`（始终在线，支持 echo/fail/sleep；`shell` 需 `-allow-shell`） |
| `-allow-shell` | `false` | 允许内置模拟执行器执行主机 shell 命令（默认关闭） |
| `-allow-origin` | | 逗号分隔的额外浏览器来源白名单，用于 `/v1/rpc`、`/v1/ws`、`/v1/register` |
| `-verbose` | `false` | 输出 debug 日志（log/slog） |

## 端点（PROTOCOL.md §6）

| 传输 | 端点 |
|---|---|
| WebSocket（主） | `ws://host/v1/ws` |
| HTTP POST（回退） | `POST /v1/rpc`（单信封或数组批处理） |
| SSE | `GET /v1/events?token=...`（15s 心跳注释行） |
| 长轮询 | `GET /v1/poll?cursor=N&timeout=30`（timeout 最大 60s） |
| 健康检查 | `GET /v1/health` |
| 节点注册 | `POST /v1/register` |

## 方法（PROTOCOL.md §5）

`engine.info` / `engine.ping` / `engine.capabilities` / `auth.login` / `auth.refresh` / `auth.logout` / `node.list` / `node.get` / `node.subscribe` / `node.unsubscribe` / `command.send` / `command.batch` / `command.cancel` / `task.create` / `task.get` / `task.list` / `task.cancel` / `task.subscribe` / `event.subscribe` / `event.unsubscribe`

> 扩展方法（PROTOCOL 之外、仅增不删的补充，用于真实节点生命周期）：`node.heartbeat`（刷新 last_seen）、`node.unregister`（下线）。

`engine.info` / `engine.ping` / `engine.capabilities` / `auth.login` / `auth.refresh` 与 `GET /v1/health` 永不要求认证（§8.5）。

## 设计要点

- **模拟执行器**：引擎内置节点方法执行器（`echo` 返回 params、`fail` 返回 `-32000 simulated failure`、`sleep` 睡眠 `params.ms` 毫秒、`shell` 用 `os/exec` 执行 `params.cmd`（字符串→`sh -c`，数组→argv），超时/取消 kill 进程）。执行期间节点置 `busy`，结束后恢复 `online`。
- **异步指令**：`command.send` 返回 `accepted`，随后 `running → succeeded/failed/timeout`（默认超时 30s，`timeout:0` 不限）；`command.batch` 同步逐个下发，失败者错误内嵌于 `command.error`。
- **幂等**：`idempotency_key` 5 分钟 TTL 内重复调用返回首次结果（command 级与 batch 级）。
- **认证**：`token`/`basic` 模式需先 `auth.login` 取得 access_token（TTL 1h，可 refresh），之后经 `Authorization: Bearer` 或 `params.auth_token` 携带；`auth.refresh` 同时签发**新的 access_token 与新的 refresh_token**，旧 token 对原子失效（不可重放）；`auth.logout` 吊销当前会话 token 及其 refresh token；`none` 模式直接匿名访问（也可 login 换匿名 token）。HTTP 层额外接受 `Authorization: Basic`（仅 basic 模式）。
- **所有权与入参校验**：订阅/指令/任务资源记录所有者（access_token 或 `basic:<user>`），非所有者 `event.unsubscribe` / `node.unsubscribe` / `command.cancel` / `task.cancel` → `-32004`（检查先于终态检查；匿名资源不校验）；`/v1/register` 拒绝与 demo 节点冲突的 id（`-32602`），真实节点同 id 重复注册仍为更新；`event_types` 非数组或含非字符串 → `-32602`；`timeout` / `sleep` 的 `ms` 为整数毫秒、范围 `[0, 2147483647]`（`0` = 不超时/立即，非整数 `-32602`，负值按 0）。
- **事件**：订阅支持 `event_types`/`node_id`/`task_id` 交集过滤；未知事件类型订阅返回 `-32031`；投递走带缓冲 channel、非阻塞（满则丢弃并记日志）；WS 连接关闭时自动清理该连接创建的订阅；SSE/长轮询各自挂一个内部订阅。
- **并发安全**：所有共享状态（节点/指令/任务/事件/认证）均以 `sync.RWMutex` 保护；列表返回快照避免响应序列化与后台执行竞争。

## curl 起爆验证

```bash
# 1. 发现 + 健康检查（无需认证）
curl -s localhost:8080/v1/health
curl -s -X POST localhost:8080/v1/rpc -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"engine.info","params":{"client_info":{"name":"curl","protocol_versions":["1.0"]}}}'

# 2. 列表节点 + 发指令（异步 accepted）
curl -s -X POST localhost:8080/v1/rpc -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"node.list","params":{"limit":10}}'
curl -s -X POST localhost:8080/v1/rpc -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":3,"method":"command.send","params":{"node_id":"n-1","method":"echo","params":{"text":"hi"}}}'

# 3. 任务 + 事件订阅（SSE 与长轮询）
curl -s -X POST localhost:8080/v1/rpc -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":4,"method":"task.create","params":{"name":"demo","commands":[{"node_id":"n-1","method":"echo","params":{"text":"a"}},{"node_id":"n-2","method":"echo","params":{"text":"b"}}]}}'
curl -s 'localhost:8080/v1/poll?cursor=0&timeout=5'
curl -N 'localhost:8080/v1/events'
```

token 模式先登录拿 token，再带 `-H "Authorization: Bearer <token>"`：

```bash
curl -s -X POST localhost:8080/v1/rpc -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"auth.login","params":{"token":"secret"}}'
```