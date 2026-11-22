#!/usr/bin/env python3
"""Python agent built on the Python SDK Agent runtime.

    PYTHONPATH=sdk/python python3 examples/agents/python/agent.py \
        --url http://127.0.0.1:8080 --id py-1

The engine sees a node with platform.runtime="python".
"""
import argparse
import os
import socket
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from hub import Agent, AgentHandlerError  # noqa: E402
from hub.errors import ERROR_INTERNAL_ERROR  # noqa: E402


def csv(value):
    return [s.strip() for s in value.split(",") if s.strip()]


def main():
    parser = argparse.ArgumentParser(description="Musubi Python agent")
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--token", default=None)
    parser.add_argument("--id", default="py-agent-1")
    parser.add_argument("--name", default=None)
    parser.add_argument("--capabilities", default="echo,sys.info,sleep,fail")
    parser.add_argument("--tags", default="agent,python")
    parser.add_argument("--heartbeat", type=float, default=10.0)
    parser.add_argument("--pull-wait", type=int, default=5000)
    parser.add_argument("--concurrency", type=int, default=2)
    args = parser.parse_args()

    agent = Agent(
        url=args.url,
        token=args.token,
        node_id=args.id,
        name=args.name or args.id,
        capabilities=csv(args.capabilities),
        tags=csv(args.tags),
        heartbeat=args.heartbeat,
        pull_wait=args.pull_wait,
        concurrency=args.concurrency,
        on_error=lambda err: print("agent:", err, file=sys.stderr),
    )

    @agent.handle("sys.info")
    def sys_info(_params):
        return {
            "node_id": agent.node_id,
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "python_version": sys.version.split()[0],
            "executable": sys.executable,
        }

    @agent.handle("sleep")
    def sleep(params):
        ms = int(params.get("ms", 0))
        time.sleep(ms / 1000.0)
        return {"slept_ms": ms}

    @agent.handle("fail")
    def fail(params):
        raise AgentHandlerError(
            ERROR_INTERNAL_ERROR, params.get("message", "requested failure"))

    print("python agent starting:", args.id, "->", args.url)
    agent.start()
    try:
        agent.wait()
    except KeyboardInterrupt:
        pass
    finally:
        agent.stop()


if __name__ == "__main__":
    main()
