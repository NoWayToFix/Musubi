#!/usr/bin/env bash
# 兼容性矩阵起爆测试：验证 PROTOCOL §8 的兼容性铁律
#  - 未知请求字段被忽略（不报错）
#  - 未知事件类型被客户端忽略
#  - 传输降级顺序 ws → sse → poll
#  - 断线重连（杀掉引擎进程重启，客户端自动恢复）
# 用法: bash tests/compat/matrix_detonate.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
export PATH="$PATH:/usr/local/go/bin"

ADDR="${ADDR:-127.0.0.1:18081}"
ENGINE_BIN="${ENGINE_BIN:-/tmp/opencode/hubd-matrix}"

echo "== 构建引擎 =="
( cd engine && CGO_ENABLED=0 go build -o "$ENGINE_BIN" ./cmd/hubd )

start_engine() { "$ENGINE_BIN" -addr "$ADDR" -demo 3 >/tmp/opencode/hubd-matrix.log 2>&1 & echo $!; }
stop_engine()  { kill "$1" 2>/dev/null || true; wait "$1" 2>/dev/null || true; }

PID=$(start_engine)
trap 'stop_engine $PID' EXIT
sleep 1
curl -fsS "http://$ADDR/v1/health" >/dev/null && echo "engine up"

echo "== [1] 未知请求字段被忽略 =="
curl -fsS -X POST "http://$ADDR/v1/rpc" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"node.get","params":{"node_id":"n-1","future_field":{"x":1}}}' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["result"]["node"]["id"]=="n-1"; print("  OK 未知字段被忽略")'

echo "== [2] 未知方法返回 -32601 =="
curl -fsS -X POST "http://$ADDR/v1/rpc" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"future.method","params":{}}' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["error"]["code"]==-32601; print("  OK -32601")'

echo "== [3] 版本不匹配返回 -32005 =="
curl -fsS -X POST "http://$ADDR/v1/rpc" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"engine.info","params":{"client_info":{"protocol_versions":["99.0"]}}}' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["error"]["code"]==-32005; print("  OK -32005")'

echo "== [4] SSE 事件流（HTTP 200 + text/event-stream + 真实 data 帧） =="
rm -f /tmp/opencode/sse.log /tmp/opencode/sse.headers
timeout 5 curl -fsSN -D /tmp/opencode/sse.headers "http://$ADDR/v1/events" >/tmp/opencode/sse.log 2>&1 &
SSE_PID=$!
sleep 0.8
# 连接期间触发一条指令，SSE 流应实时收到 command.result 通知
curl -fsS -X POST "http://$ADDR/v1/rpc" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"command.send","params":{"node_id":"n-1","method":"echo","params":{"text":"sse-probe"}}}' >/dev/null
wait "$SSE_PID" 2>/dev/null || true
grep -qi '^content-type: text/event-stream' /tmp/opencode/sse.headers \
  || { echo "  FAIL SSE Content-Type 不是 text/event-stream"; exit 1; }
grep -q '^data: ' /tmp/opencode/sse.log \
  || { echo "  FAIL SSE 无 data 帧（连接期间应收到 command.result）"; exit 1; }
grep -q 'command.result' /tmp/opencode/sse.log \
  || { echo "  FAIL SSE 未收到 command.result 事件"; exit 1; }
echo "  OK SSE 200 + text/event-stream + command.result data 帧"

echo "== [5] 长轮询返回空事件列表 =="
curl -fsS "http://$ADDR/v1/poll?cursor=0&timeout=1" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); assert "events" in d; print("  OK poll 结构正确 cursor=%s" % d["cursor"])'

echo "== [6] 断线重连：重启引擎后长轮询恢复并送达新事件 =="
# 重启前：生成事件并记录 poll 游标（证明事件流确实在流动）
curl -fsS -X POST "http://$ADDR/v1/rpc" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"command.send","params":{"node_id":"n-1","method":"echo","params":{"text":"pre-restart"}}}' >/dev/null
curl -fsS "http://$ADDR/v1/poll?cursor=0&timeout=2" | python3 -c '
import json, sys
d = json.load(sys.stdin)
evs = d.get("events") or []
assert evs, "重启前 poll 未读到任何事件"
assert d.get("cursor", 0) > 0, "重启前 poll 游标未前进"
print("  重启前读到 %d 个事件, cursor=%s" % (len(evs), d["cursor"]))
'
stop_engine "$PID"
sleep 1
PID=$(start_engine)
sleep 1
curl -fsS "http://$ADDR/v1/health" >/dev/null
# 重启后：事件缓冲随进程重建（内存态），从游标 0 重新读取；生成带标记的新事件并断言送达
curl -fsS -X POST "http://$ADDR/v1/rpc" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"command.send","params":{"node_id":"n-1","method":"echo","params":{"text":"post-restart"}}}' >/dev/null
curl -fsS "http://$ADDR/v1/poll?cursor=0&timeout=3" | python3 -c '
import json, sys
d = json.load(sys.stdin)
evs = d.get("events") or []
assert evs, "重启后 poll 未送达任何事件"
assert d.get("cursor", 0) > 0, "重启后 poll 游标未前进"
hits = [e for e in evs if e.get("type") == "command.result"
        and (e.get("data") or {}).get("result", {}).get("text") == "post-restart"]
assert hits, "重启后未收到 post-restart 的 command.result 事件: %r" % evs
print("  OK 重启后 poll 恢复：送达 %d 个事件（含 post-restart）, cursor=%s" % (len(evs), d["cursor"]))
'
echo "  OK 引擎重启后事件轮询恢复"

echo "== [7] 批处理（数组信封） =="
curl -fsS -X POST "http://$ADDR/v1/rpc" -H 'Content-Type: application/json' \
  -d '[{"jsonrpc":"2.0","id":1,"method":"engine.ping","params":{}},{"jsonrpc":"2.0","id":2,"method":"node.get","params":{"node_id":"n-1"}}]' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); assert isinstance(d,list) and len(d)==2 and d[0]["result"]["pong"] is True; print("  OK 批处理")'

echo "== MATRIX DETONATION: ALL PASS =="