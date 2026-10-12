#!/usr/bin/env python3
"""Schema 结构校验：验证引擎响应/事件符合 schema/*.json 契约（轻量实现，无第三方依赖）。

用法: python3 tests/schema_validate.py [--url http://127.0.0.1:8080]
"""
import argparse
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def check(name, cond, detail=""):
    if not cond:
        print("FAIL %s: %s" % (name, detail), file=sys.stderr)
        sys.exit(1)
    print("PASS", name)


def rpc(url, method, params=None, token=None):
    body = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params is not None:
        body["params"] = params
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url + "/v1/rpc", data=json.dumps(body).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def command_result_error_contract(cmd):
    """PROTOCOL §4.2：result 与 error 互斥，且与 status 一一对应。

    注意：引擎序列化 result/error 时不带 omitempty —— 两个键**始终存在**
    （缺省时为 null），因此必须断言「值非空」，而不是「键存在」。

    succeeded            → result 非空 且 error 为空
    failed               → error 非空 且 result 为空
    cancelled / timeout  → 两者均为空（终态但不属于成功/失败产物）
    pending/accepted/running → 两者均为空（尚未产生终态产物）
    """
    status = cmd.get("status")
    result = cmd.get("result")
    error = cmd.get("error")
    if status == "succeeded":
        return result is not None and error is None
    if status == "failed":
        return error is not None and result is None
    if status in ("pending", "accepted", "running", "cancelled", "timeout"):
        return result is None and error is None
    return False  # 未知 status 一律拒绝


def _selftest_result_error_contract():
    """变异体自检：证明 command_result_error_contract 真的能失败。

    防止再次出现「永真断言」——下列损坏的响应全部必须被拒绝。
    """
    broken = [
        {"status": "succeeded", "result": None, "error": None},               # 成功却无 result
        {"status": "succeeded", "result": {"text": "x"}, "error": {"code": -32000, "message": "e"}},  # 成功却带 error
        {"status": "failed", "result": {"text": "x"}, "error": None},         # 失败却带 result
        {"status": "failed", "result": None, "error": None},                  # 失败却无 error
        {"status": "accepted", "result": {"text": "x"}, "error": None},       # 未终态却带 result
        {"status": "accepted", "result": None, "error": {"code": -32000, "message": "e"}},  # 未终态却带 error
        {"status": "cancelled", "result": {"text": "x"}, "error": None},      # cancelled 不应带 result
        {"status": "timeout", "result": None, "error": {"code": -32000, "message": "e"}},   # timeout 不应带 error
        {"status": "future-status", "result": None, "error": None},           # 未知 status
    ]
    for f in broken:
        assert not command_result_error_contract(f), "变异体未被拒绝: %r" % (f,)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    args = ap.parse_args()
    url = args.url.rstrip("/")

    # 先自证断言本身可失败（否则后面的在线断言没有意义）
    _selftest_result_error_contract()
    print("PASS 自检：result/error 契约断言可拒绝变异体")

    # --- engine.info ---
    r = rpc(url, "engine.info", {"client_info": {"protocol_versions": ["1.0"]}})
    assert "error" not in r, r
    s = r["result"]
    check("server_info.protocol_version", isinstance(s["protocol_version"], str))
    check("server_info.server{name,version}", isinstance(s["server"]["name"], str) and isinstance(s["server"]["version"], str))
    check("server_info.supported_versions", isinstance(s["supported_versions"], list) and "1.0" in s["supported_versions"])
    check("server_info.encodings", "json" in s["encodings"])
    check("server_info.transports", set(["ws", "http", "sse", "poll"]).issubset(set(s["transports"])))
    check("server_info.capabilities[].id/version", all("id" in c and "version" in c for c in s["capabilities"]))
    check("server_info.session", isinstance(s["session"]["authenticated"], bool))
    check("server_info.server_time", "T" in s["server_time"] and s["server_time"].endswith("Z"))

    # --- node.list ---
    r = rpc(url, "node.list", {})
    nodes = r["result"]["nodes"]
    check("node[].required字段", all("id" in n and "name" in n and "status" in n for n in nodes))
    check("node[].status枚举", all(n["status"] in ("online", "busy", "offline", "unknown") for n in nodes))
    check("node[].id唯一", len({n["id"] for n in nodes}) == len(nodes))

    # --- command.send (echo) ---
    r = rpc(url, "command.send", {"node_id": "n-1", "method": "echo", "params": {"text": "x"}})
    cmd = r["result"]["command"]
    check("command.status枚举", cmd["status"] in ("pending", "accepted", "running", "succeeded", "failed", "cancelled", "timeout"))
    check("command.result与error契约(按status)", command_result_error_contract(cmd),
          repr({"status": cmd.get("status"), "result": cmd.get("result"), "error": cmd.get("error")}))
    check("command 时间戳", cmd["created_at"].endswith("Z"))

    # --- command.batch（同步终态：succeeded + failed 混合，覆盖 §4.2 终态分支） ---
    r = rpc(url, "command.batch", {"commands": [
        {"node_id": "n-1", "method": "echo", "params": {"text": "x"}},
        {"node_id": "n-2", "method": "fail"},
    ]})
    cmds = r["result"]["commands"]
    check("command.batch 终态 [succeeded, failed]",
          [c.get("status") for c in cmds] == ["succeeded", "failed"], repr(cmds))
    check("command.result与error契约(终态混合)",
          all(command_result_error_contract(c) for c in cmds),
          repr([(c.get("status"), c.get("result"), c.get("error")) for c in cmds]))

    # --- task.create ---
    r = rpc(url, "task.create", {"name": "s", "commands": [{"node_id": "n-1", "method": "echo"}]})
    t = r["result"]["task"]
    check("task.progress∈[0,1]", 0 <= t["progress"] <= 1)
    check("task.command_ids", isinstance(t["command_ids"], list) and len(t["command_ids"]) == 1)

    # --- 错误结构 ---
    r = rpc(url, "node.get", {"node_id": "nope"})
    e = r["error"]
    check("error{code,message,data}", isinstance(e["code"], int) and isinstance(e["message"], str) and isinstance(e.get("data"), (dict, type(None))))

    print("== SCHEMA VALIDATE: ALL PASS ==")


if __name__ == "__main__":
    main()