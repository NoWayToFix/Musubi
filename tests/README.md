# 测试说明 (tests/)

Control Hub 的测试分三层，全部可由 `tests/run_all.sh` 一键执行：

```
bash tests/run_all.sh                # 契约 + 多语言冒烟 + 矩阵冒烟（默认 addr=127.0.0.1:18080, auth=none）
bash tests/run_all.sh --auth token   # token 认证模式（-token secret-token）
```

## 1. 契约测试（语言无关）

- 驱动引擎（或任何遵循 `docs/PROTOCOL.md` 的实现），用**同一套 JSON 用例**验证协议行为。
- 用例：`tests/contract/cases/*.json`，运行器：`tests/run_contract.py`（仅标准库）。

```bash
python3 tests/run_contract.py --url http://127.0.0.1:18080 [--auth token --token secret-token]
```

### 用例格式

```json
{
  "name": "描述",
  "request": {"method": "node.get", "params": {"node_id": "n-1"}},
  "expect": {"result": {"$.node.id": "n-1"}},
  "save": {"var": "$.node.id"}          // 可选：捕获字段供后续 @var 引用
}
```

- 结果校验：`expect.result` 的键是 JSON 路径（`$` 根，`$.a.b[0]` 下标，`$.length` 数组长度），值可为字面量或操作符 `{"$type": "string|number|boolean|object|array"}`。
- 错误校验：`expect.error_code`（整数，如 `-32010`）。
- 事件校验：`expect.wait_event` 订阅后等待匹配事件：`{type, node_id?, task_id?, check?}`。
- 批处理：`request.batch` 直接给数组信封，`expect.result` 用 `$[0].result.x` / `$[1].error.code` 定位。
- 跨用例变量：`"save": {"command_id": "$.command.id"}` 后可用 `"@command_id"` 引用。
- 忽略：`expect.error_code: "__skip__"` 跳过错误断言（用于不需要强校验的用例）。

## 2. 多语言一致性冒烟

三个 SDK 对同一引擎执行**完全相同的断言序列**，验证行为一致：

| 语言 | 文件 | 运行 |
|---|---|---|
| Python | `tests/compat/smoke_py.py` | `python3 tests/compat/smoke_py.py --url ...` |
| TypeScript | `tests/compat/smoke_ts.mjs` | `node tests/compat/smoke_ts.mjs`（需先构建 sdk/ts） |
| Go | `tests/compat/smoke_go/main.go` | `cd sdk/go && go run ./tests/compat/smoke_go` |

断言序列（三种语言一致）：
1. `engine.info` 版本协商为 1.0 → 2. `engine.ping` → 3. `node.list` 3 节点 → 4. `node.get` → 5. `command.send` echo 同步成功 → 6. 幂等键去重 → 7. `task.create` → 8. 事件订阅收到 `command.result` → 9. `node.get` 不存在返回 `-32010` → 10. 传输列表含 `ws`/`poll`。

任一语言 FAIL 即退出码非 0，`run_all.sh` 失败。

## 3. 兼容性矩阵冒烟

`tests/compat/matrix_smoke.sh`：验证 PROTOCOL §8 兼容性铁律——
未知请求字段忽略、未知方法 `-32601`、版本不匹配 `-32005`、SSE 事件流、长轮询结构、引擎重启后恢复、JSON-RPC 数组批处理。

## 手工冒烟（引擎独立运行时）

```bash
curl -s http://127.0.0.1:8080/v1/health
curl -s -X POST http://127.0.0.1:8080/v1/rpc -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"engine.info","params":{"client_info":{"protocol_versions":["1.0"]}}}'
```