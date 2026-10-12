#!/usr/bin/env bash
# Control Hub 全量测试编排：
#   1. 构建引擎（仓库根无 go.mod，须在 engine/ 内执行）
#   2. 契约测试 auth=none
#   3. 契约测试 auth=token 第二轮
#   4. Schema 结构校验
#   5. 多语言一致性起爆（Python / TypeScript / Go，核心场景等价覆盖）
#   6. 兼容性矩阵起爆（独立端口与引擎实例）
# 用法: bash tests/run_all.sh [--url :18080] [--auth none]
set -euo pipefail
cd "$(dirname "$0")/.."

export PATH="$PATH:/usr/local/go/bin"
ADDR="${ADDR:-127.0.0.1:18080}"
TOKEN="${TOKEN:-secret-token}"
ENGINE_BIN="${ENGINE_BIN:-/tmp/opencode/hubd-test}"
PID=""

cleanup() {
  [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "== [1/6] 构建引擎 (CGO_ENABLED=0, 单二进制) =="
( cd engine && CGO_ENABLED=0 go build -o "$ENGINE_BIN" ./cmd/hubd )

last_health=""
start_engine() { # $1 = auth mode (none|token)
  ARGS=(-addr "$ADDR" -demo 3 -verbose)
  if [ "$1" = "token" ]; then ARGS+=(-auth token -token "$TOKEN"); fi
  "$ENGINE_BIN" "${ARGS[@]}" >/tmp/opencode/hubd-test.log 2>&1 &
  PID=$!
  sleep 1
  curl -fsS "http://$ADDR/v1/health" >/dev/null
  echo "  engine up (auth=$1)"
}
stop_engine() {
  [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  PID=""
}

echo "== [2/6] 契约测试 auth=none =="
start_engine none
python3 tests/run_contract.py --url "http://$ADDR" --auth none
stop_engine

echo "== [3/6] 契约测试 auth=token 第二轮 =="
start_engine token
python3 tests/run_contract.py --url "http://$ADDR" --auth token --token "$TOKEN"
stop_engine

echo "== [4/6] Schema 结构校验 =="
start_engine none
python3 tests/schema_validate.py --url "http://$ADDR"

echo "== [5/6] 多语言一致性起爆（核心场景等价覆盖） =="
python3 tests/compat/detonate_py.py --url "http://$ADDR"
if [ -f sdk/ts/dist/index.mjs ]; then
  HUB_URL="http://$ADDR" node tests/compat/detonate_ts.mjs
else
  echo "  skip TS 起爆（sdk/ts 未构建，先执行: cd sdk/ts && npm install && npm run build）"
fi
( cd tests/compat/detonate_go && HUB_URL="http://$ADDR" GOFLAGS=-mod=mod go run . )
stop_engine
trap - EXIT

echo "== [6/6] 兼容性矩阵起爆（独立端口 18081） =="
bash tests/compat/matrix_detonate.sh

echo
echo "== ALL TESTS PASSED =="