#!/usr/bin/env python3
"""Control Hub 契约测试运行器（语言无关，仅标准库）。

用法:
  python3 tests/run_contract.py [--url http://127.0.0.1:8080] [--auth none|token|basic]
                                [--token SECRET] [--user U] [--pass P] [--cases tests/contract/cases]

说明:
  - 运行前需先启动 engine（tests/run_all.sh 会自动完成）。
  - 用例格式见 tests/README.md。
  - auth 模式下运行器自动先 auth.login 并携带 access_token。
"""
import argparse
import json
import re
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "sdk" / "python"))


def match(actual, expected):
    """操作符断言：{"$type": "string|number|boolean|object|array"}，否则精确相等。"""
    if isinstance(expected, dict) and set(expected.keys()) == {"$type"}:
        t = expected["$type"]
        if t == "number":
            return isinstance(actual, (int, float)) and not isinstance(actual, bool)
        if t == "string":
            return isinstance(actual, str)
        if t == "boolean":
            return isinstance(actual, bool)
        if t == "object":
            return isinstance(actual, dict)
        if t == "array":
            return isinstance(actual, list)
    return actual == expected


class Runner:
    def __init__(self, url, auth, token, user, password):
        self.url = url.rstrip("/")
        self.auth = auth
        self.token = token
        self.user = user
        self.password = password
        self.access_token = None
        self.refresh_token = None
        self.ctx = {}          # 跨用例变量（@var 引用 / save 捕获）
        self.cursor = 0        # 长轮询游标
        self.next_id = 1000

    # ---------- 传输 ----------
    def rpc(self, payload, timeout=30):
        body = json.dumps(payload).encode()
        headers = {"Content-Type": "application/json"}
        if self.access_token:
            headers["Authorization"] = "Bearer " + self.access_token
        req = urllib.request.Request(self.url + "/v1/rpc", data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())

    def poll(self, timeout=3):
        url = "%s/v1/poll?cursor=%d&timeout=%d" % (self.url, self.cursor, timeout)
        if self.access_token:
            url += "&token=" + self.access_token
        with urllib.request.urlopen(url, timeout=timeout + 5) as resp:
            data = json.loads(resp.read().decode())
        self.cursor = data.get("cursor") or self.cursor
        return data.get("events") or []

    def call(self, method, params=None, timeout=30):
        req = {"jsonrpc": "2.0", "id": self.next_id, "method": method}
        self.next_id += 1
        if params is not None:
            req["params"] = params
        resp = self.rpc(req, timeout)
        if isinstance(resp, list):
            for item in resp:
                if item.get("id") == req["id"]:
                    resp = item
                    break
        if "error" in resp:
            return {"__error__": resp["error"]}
        return resp.get("result", {})

    # ---------- 工具 ----------
    # 路径段：键名 + 任意多个下标，例如 "commands[0]" 或 "[2]"。
    _SEG_RE = re.compile(r"(\w*)((?:\[\d+\])*)")

    @classmethod
    def resolve(cls, data, path):
        """解析 $.a.b[0].c / $[0].result 形式的 JSON 路径。

        支持 $.a.length（对数组取长度）与多级下标 $.a[0][1].b。
        路径不存在时抛出 AssertionError，便于报告为用例失败而非运行错误。
        """
        if path == "$":
            return data
        rest = path[1:]              # 去掉根标识 '$'
        if rest.startswith("."):
            rest = rest[1:]
        cur = data
        for seg in rest.split("."):
            m = cls._SEG_RE.fullmatch(seg)
            if m is None:
                raise AssertionError("路径 %s 中存在无法解析的片段: %s" % (path, seg))
            key, idx = m.group(1), m.group(2)
            if key:
                if key == "length" and isinstance(cur, list):
                    cur = len(cur)
                elif isinstance(cur, dict) and key in cur:
                    cur = cur[key]
                else:
                    raise AssertionError("路径 %s 在 %r 中不存在字段 %s" % (path, cur, key))
            for ix in re.findall(r"\[(\d+)\]", idx):
                i = int(ix)
                if not isinstance(cur, list) or i >= len(cur):
                    raise AssertionError("路径 %s 越界: 索引 %d 不存在" % (path, i))
                cur = cur[i]
        return cur

    def deref(self, value):
        """把 @var 引用解析为 ctx 中的值（递归）。"""
        if isinstance(value, str) and value.startswith("@") and value[1:] in self.ctx:
            return self.ctx[value[1:]]
        if isinstance(value, dict):
            return {k: self.deref(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.deref(v) for v in value]
        return value

    def apply_save(self, result, save):
        for name, path in (save or {}).items():
            self.ctx[name] = self.resolve(result, path)

    # ---------- 等待事件 ----------
    def wait_event(self, spec, timeout=15):
        """通过长轮询等待匹配事件。spec: {type, node_id?, task_id?, check?, timeout?}"""
        deadline = time.time() + spec.get("timeout", timeout)
        node_id = self.deref(spec.get("node_id"))
        task_id = self.deref(spec.get("task_id"))
        while time.time() < deadline:
            for ev in self.poll(2):
                if ev.get("type") != spec["type"]:
                    continue
                if node_id and ev.get("node_id") != node_id:
                    continue
                if task_id and ev.get("task_id") != task_id:
                    continue
                for path, expected in (spec.get("check") or {}).items():
                    if not match(self.resolve(ev, path), self.deref(expected)):
                        break
                else:
                    return ev
        raise AssertionError("等待事件超时: %s" % json.dumps(spec))

    # ---------- 用例执行 ----------
    def run_case(self, case):
        name = case.get("name", "?")
        expect = case.get("expect", {})
        result = None
        err = None

        # Mode-gated cases (e.g. auth.* in token mode) are skipped unless the
        # runner's auth mode matches.
        mode = case.get("mode")
        if mode and mode != self.auth:
            print("  SKIP  %s (mode=%s, current auth=%s)" % (name, mode, self.auth))
            return True

        if "batch" in case["request"]:
            result = self.rpc(self.deref(case["request"]["batch"]))
            # A batch response is an array; per-item errors are part of the
            # result (asserted via $[i].error.code). Only a top-level error
            # object (malformed batch) is treated as a method error.
            if isinstance(result, dict) and "error" in result:
                err = result["error"]
                result = None
        else:
            req = {"method": case["request"]["method"]}
            if "params" in case["request"]:
                req["params"] = self.deref(case["request"]["params"])
            result = self.call(req["method"], req.get("params"))
            if "__error__" in result:
                err = result["__error__"]
                result = None

        # 校验错误码
        if expect.get("error_code") == "__skip__":
            pass
        elif "error_code" in expect:
            assert err is not None, "%s: 期望错误 %s 但成功了: %r" % (name, expect["error_code"], result)
            assert err["code"] == expect["error_code"], \
                "%s: 错误码不符 期望 %s 实际 %s (%s)" % (name, expect["error_code"], err["code"], err.get("message"))
        else:
            assert err is None, "%s: 意外错误 %s" % (name, err)

        # 校验结果字段
        if "result" in expect:
            assert result is not None, "%s: 期望有 result 但没有" % name
            for path, expected in expect["result"].items():
                actual = self.resolve(result, path)
                assert match(actual, self.deref(expected)), \
                    "%s: 字段不符 %s 期望 %r 实际 %r" % (name, path, expected, actual)

        # 保存变量（事件等待前完成，供 wait_event 的 @var 使用）
        if "save" in case and result is not None:
            self.apply_save(result, case["save"])

        # 等待事件（请求完成之后，从服务端游标缓冲读取）
        we = expect.get("wait_event")
        if we:
            sub = self.call("event.subscribe", {
                "event_types": [we["type"]],
                "node_id": self.deref(we.get("node_id")),
                "task_id": self.deref(we.get("task_id")),
            })
            assert "__error__" not in sub, "event.subscribe 失败: %s" % sub
            ev = self.wait_event(we)
            for path, expected in (we.get("check") or {}).items():
                actual = self.resolve(ev, path)
                assert match(actual, self.deref(expected)), \
                    "%s: 事件字段不符 %s 期望 %r 实际 %r" % (name, path, expected, actual)

        print("  PASS  %s" % name)
        return True

    # ---------- 会话 ----------
    def setup_auth(self):
        if self.auth == "none":
            return
        if self.auth == "token":
            params = {"token": self.token}
        else:
            params = {"username": self.user, "password": self.password}
        r = self.call("auth.login", params)
        assert "__error__" not in r, "auth.login 失败: %s" % r
        self.access_token = r["access_token"]
        self.refresh_token = r.get("refresh_token")
        print("  AUTH  %s 登录成功" % self.auth)

    def run(self, cases_dir):
        print("== Control Hub 契约测试 ==")
        print("url: %s  auth: %s" % (self.url, self.auth))
        self.setup_auth()
        total = passed = 0
        for f in sorted(Path(cases_dir).glob("*.json")):
            cases = json.loads(f.read_text())
            if isinstance(cases, dict):
                cases = [cases]
            print("-- %s" % f.name)
            for case in cases:
                total += 1
                try:
                    self.run_case(case)
                    passed += 1
                except AssertionError as e:
                    print("  FAIL  %s: %s" % (case.get("name", "?"), e))
                except Exception as e:  # noqa: BLE001
                    print("  ERROR %s: %r" % (case.get("name", "?"), e))
        print("== 结果: %d/%d 通过 ==" % (passed, total))
        return passed == total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    ap.add_argument("--auth", default="none", choices=["none", "token", "basic"])
    ap.add_argument("--token", default="secret-token")
    ap.add_argument("--user", default="admin")
    ap.add_argument("--pass", dest="password", default="admin")
    ap.add_argument("--cases", default=str(ROOT / "tests" / "contract" / "cases"))
    args = ap.parse_args()
    r = Runner(args.url, args.auth, args.token, args.user, args.password)
    ok = r.run(args.cases)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()