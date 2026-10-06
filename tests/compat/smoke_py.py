#!/usr/bin/env python3
"""多语言一致性冒烟测试（Python SDK 版）。

与 smoke_ts.mjs、smoke_go/main.go 执行完全相同的断言序列，用于验证
三种 SDK 面对同一契约得到一致行为：

  1  engine.info 协商协议版本 / 传输能力
  2  engine.ping
  3  node.list / node.get
  4  command.send 异步接受 + 事件驱动等待终态
  5  command.send 幂等键去重
  6  command.batch 同步批量（成功 + 失败混合）
  7  task.create 异步创建 + 事件驱动等待终态
  8  错误码 node.not_found (-32010)
  9  订阅释放 (event.unsubscribe)
 10  WS 不可用时自动降级 HTTP（transport=http 强制路径）

用法: python3 tests/compat/smoke_py.py [--url http://127.0.0.1:8080]
"""
import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from hub import HubClient, HubError  # noqa: E402

PASSED = []


def check(name, cond, detail=""):
    if not cond:
        print("FAIL %s: %s" % (name, detail), file=sys.stderr)
        sys.exit(1)
    PASSED.append(name)
    print("PASS", name)


def wait_for(sub, predicate, timeout=15):
    """从订阅事件流中等待满足条件的事件。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            ev = sub.get(timeout=0.5)
        except Exception:  # queue.Empty
            continue
        if predicate(ev):
            return ev
    raise AssertionError("等待事件超时")


def run(url, transport):
    tag = transport.upper()
    iksuffix = "%s-%d" % (tag, os.getpid())
    c = HubClient(url, timeout=10, transport=transport)

    # 1. 版本协商与能力发现
    info = c.info({"name": "smoke-py", "version": "1.0.0",
                   "protocol_versions": ["1.0"], "encodings": ["json"],
                   "transports": ["ws", "http"]})
    check("%s engine.info 协商" % tag, info.get("protocol_version") == "1.0", repr(info))
    check("%s 传输能力含 ws/http" % tag,
          "ws" in info.get("transports", []) and "http" in info.get("transports", []),
          repr(info.get("transports")))

    # 2. ping
    check("%s engine.ping" % tag, c.ping().get("pong") is True, repr(c.ping()))

    # 3. 节点
    nodes = c.node_list()
    check("%s node.list 返回 3 个节点" % tag, len(nodes.get("nodes", [])) == 3, repr(nodes))
    node = c.node_get("n-1")
    check("%s node.get" % tag, node["node"]["id"] == "n-1", repr(node))
    check("%s node.get 能力非空" % tag, len(node["node"].get("capabilities", [])) >= 1, repr(node))

    # 4. command.send 异步：订阅先行，等待 command.result
    sub = c.subscribe({"event_types": ["command.result", "command.failed"], "node_id": "n-1"})
    sent = c.command_send("n-1", "echo", {"text": "hi"}, timeout=5000)
    cmd_id = sent["command"]["id"]
    ev = wait_for(sub, lambda e: e.get("type") == "command.result"
                  and e.get("data", {}).get("id") == cmd_id)
    check("%s command.send 事件终态 succeeded" % tag,
          ev["data"]["status"] == "succeeded" and ev["data"]["result"]["text"] == "hi", repr(ev))

    # 5. 幂等键：同键二次下发复用首次结果
    ik = "smoke-ik-" + iksuffix
    first = c.command_send("n-1", "echo", {"text": "dedupe"}, idempotency_key=ik)
    wait_for(sub, lambda e: e.get("type") == "command.result"
             and e.get("data", {}).get("id") == first["command"]["id"])
    dup = c.command_send("n-1", "echo", {"text": "changed"}, idempotency_key=ik)
    check("%s command.send 幂等去重" % tag,
          dup["command"]["id"] == first["command"]["id"]
          and dup["command"]["result"]["text"] == "dedupe", repr(dup))

    # 6. command.batch 同步返回终态（含失败项，不抛异常）
    batch = c.command_batch([{"node_id": "n-2", "method": "echo", "params": {"text": "b"}},
                             {"node_id": "n-2", "method": "fail"}])
    st = [cmd.get("status") for cmd in batch.get("commands", [])]
    check("%s command.batch 混合终态" % tag, st == ["succeeded", "failed"], repr(batch))

    # 7. task.create 异步：等待 task.completed
    tsub = c.subscribe({"event_types": ["task.completed", "task.failed"]})
    task = c.task_create("smoke-task-" + tag, [{"node_id": "n-3", "method": "echo"}])
    task_id = task["task"]["id"]
    tev = wait_for(tsub, lambda e: e.get("task_id") == task_id
                   and e.get("type") in ("task.completed", "task.failed"))
    check("%s task.create 终态 succeeded" % tag, tev["data"]["status"] == "succeeded", repr(tev))
    check("%s task.get 与事件一致" % tag, c.task_get(task_id)["task"]["status"] == "succeeded")

    # 8. 错误码：未知节点
    try:
        c.node_get("n-999")
        check("%s node.not_found -32010" % tag, False, "未抛出 HubError")
    except HubError as e:
        check("%s node.not_found -32010" % tag, e.code == -32010, str(e))

    # 9. 订阅释放
    sub.close()
    check("%s event.unsubscribe" % tag, sub.id is None, "sub.id 应清空")
    tsub.close()

    c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    args = ap.parse_args()

    # 事件订阅依赖长轮询；再跑一轮强制 HTTP 调用路径验证传输无关性。
    run(args.url, "poll")
    run(args.url, "http")
    print("PYTHON SDK SMOKE: ALL PASS (%d)" % len(PASSED))


if __name__ == "__main__":
    main()
