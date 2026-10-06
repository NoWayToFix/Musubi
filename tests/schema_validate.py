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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    args = ap.parse_args()
    url = args.url.rstrip("/")

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
    check("command.result与error互斥", ("result" in cmd) != ("error" in cmd) or cmd.get("status") in ("pending", "accepted", "running"))
    check("command 时间戳", cmd["created_at"].endswith("Z"))

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