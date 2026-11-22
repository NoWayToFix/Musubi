"""Agent — the reference worker runtime for a real node (PROTOCOL.md §5.3).

One platform, one SDK: create an Agent in a Python process, register command
handlers, call start()/run_forever(). The Agent registers itself with a
platform descriptor, heartbeats, long-polls ``command.pull``, executes
handlers, reports ``command.complete`` and unregisters on shutdown.
"""
import logging
import platform as _platform
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError

from . import __version__
from .client import HubClient
from .errors import (
    ERROR_INTERNAL_ERROR,
    ERROR_METHOD_NOT_FOUND,
    ERROR_NODE_NOT_FOUND,
    ERROR_NODE_OFFLINE,
    HubError,
)

log = logging.getLogger("musubi.agent")

_TERMINAL = ("succeeded", "failed")


class AgentHandlerError(Exception):
    """Handler error carrying a protocol error code/message."""

    def __init__(self, code, message, data=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data or {}


def detect_platform():
    """Returns the self-reported platform descriptor of this Python process."""
    system = _platform.system().lower()
    os_name = {
        "darwin": "darwin",
        "windows": "windows",
        "linux": "linux",
    }.get(system, system or "unknown")
    return {
        "os": os_name,
        "arch": _platform.machine(),
        "runtime": "python",
        "runtime_version": _platform.python_version(),
        "agent_version": __version__,
    }


class Agent(object):
    """A registered node driven by the Python SDK.

    Args:
        url/token/transport/client: connection (client reuses an existing
            HubClient and is not closed by the Agent).
        node_id: node id; the engine generates one when omitted.
        name/capabilities/tags/metadata/platform: registration identity.
        heartbeat: heartbeat interval seconds (default 10; core watchdog 15).
        pull_wait: command.pull long-poll milliseconds (default 5000).
        concurrency: parallel pull loops (default 1).
        handler_timeout: per-command handler timeout seconds (None = none).
        on_error: optional callback for non-fatal background errors.
    """

    def __init__(self, url=None, token=None, transport="auto", client=None,
                 node_id=None, name=None, capabilities=None, tags=None,
                 metadata=None, platform=None, heartbeat=10.0, pull_wait=5000,
                 concurrency=1, handler_timeout=None, on_error=None,
                 **client_kwargs):
        self._client = client or HubClient(url=url, token=token,
                                           transport=transport, **client_kwargs)
        self._own_client = client is None
        self._node_id_opt = node_id
        self._name = name
        self._capabilities = list(capabilities or [])
        self._tags = list(tags or [])
        self._metadata = metadata
        self._platform = platform or detect_platform()
        self._heartbeat = max(float(heartbeat), 0.1)
        self._pull_wait = int(pull_wait)
        self._concurrency = max(int(concurrency), 1)
        self._handler_timeout = handler_timeout
        self._on_error = on_error

        self._handlers = {}
        self._handlers_lock = threading.Lock()
        self._stop = threading.Event()
        self._reg_lock = threading.Lock()
        self._node = None
        self._supervisor = None

        self.handle("echo", lambda params: params)
        self.handle("platform.info", lambda _params: {
            "node_id": self.node_id,
            "platform": self._platform,
            "agent": {"runtime": "python", "version": __version__},
        })

    # ------------------------------------------------------------------
    # handlers
    # ------------------------------------------------------------------

    def handle(self, method, fn=None):
        """Registers a handler; usable directly or as a decorator.

        ``agent.handle("sys.info", fn)`` or::

            @agent.handle("sys.info")
            def sys_info(params):
                ...
        """
        if fn is None:
            def decorate(func):
                self.handle(method, func)
                return func
            return decorate
        with self._handlers_lock:
            self._handlers[method] = fn
        return fn

    def _handler(self, method):
        with self._handlers_lock:
            return self._handlers.get(method)

    # ------------------------------------------------------------------
    # identity
    # ------------------------------------------------------------------

    @property
    def node_id(self):
        node = self._node
        return (node or {}).get("id") or self._node_id_opt

    @property
    def node(self):
        return dict(self._node) if self._node else None

    @property
    def client(self):
        return self._client

    def _register_params(self):
        p = {"platform": self._platform}
        if self.node_id:
            p["node_id"] = self.node_id
        if self._name:
            p["name"] = self._name
        if self._capabilities:
            p["capabilities"] = list(self._capabilities)
        if self._tags:
            p["tags"] = list(self._tags)
        if self._metadata is not None:
            p["metadata"] = self._metadata
        return p

    def _register_once(self):
        res = self._client.node_register(**self._register_params())
        node = res.get("node")
        if node:
            self._node = node

    def _register_with_retry(self):
        delay = 1.0
        while not self._stop.is_set():
            try:
                self._register_once()
                return True
            except Exception as exc:  # noqa: BLE001 - background retry loop
                if self._stop.is_set():
                    return False
                self._report(exc)
                if self._stop.wait(delay):
                    return False
                delay = min(delay * 2, 30.0)
        return False

    def _reregister(self):
        with self._reg_lock:
            if self._stop.is_set():
                return
            try:
                self._register_once()
            except Exception as exc:  # noqa: BLE001
                self._report(exc)

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    def start(self):
        """Starts the agent in a background supervisor thread; returns self."""
        if self._supervisor and self._supervisor.is_alive():
            return self
        self._supervisor = threading.Thread(
            target=self._run, name="musubi-agent", daemon=True)
        self._supervisor.start()
        return self

    def run_forever(self):
        """Runs the agent in the current thread until stop() is called."""
        self._run()

    def wait(self, timeout=None):
        """Waits for a started agent to finish (after stop())."""
        if self._supervisor and self._supervisor is not threading.current_thread():
            self._supervisor.join(timeout)

    def stop(self, timeout=5.0):
        """Signals shutdown, waits briefly, then unregisters."""
        self._stop.set()
        self.wait(timeout)

    def _run(self):
        try:
            if not self._register_with_retry():
                return
            heartbeat = threading.Thread(
                target=self._heartbeat_loop, name="musubi-heartbeat", daemon=True)
            heartbeat.start()
            workers = []
            for i in range(self._concurrency):
                t = threading.Thread(
                    target=self._pull_loop, name="musubi-pull-%d" % i, daemon=True)
                t.start()
                workers.append(t)

            self._stop.wait()
            for t in workers:
                t.join(timeout=2.0)
            heartbeat.join(timeout=2.0)
            self._unregister()
        finally:
            if self._own_client:
                try:
                    self._client.close()
                except Exception:  # noqa: BLE001 - best effort
                    pass

    def _heartbeat_loop(self):
        while not self._stop.wait(self._heartbeat):
            node_id = self.node_id
            if not node_id:
                continue
            try:
                self._client.node_heartbeat(node_id)
            except HubError as exc:
                if self._stop.is_set():
                    return
                if exc.code in (ERROR_NODE_NOT_FOUND, ERROR_NODE_OFFLINE):
                    self._reregister()
                else:
                    self._report(exc)
            except Exception as exc:  # noqa: BLE001
                if self._stop.is_set():
                    return
                self._report(exc)

    def _pull_loop(self):
        backoff = 1.0
        while not self._stop.is_set():
            node_id = self.node_id
            if not node_id:
                self._stop.wait(backoff)
                continue
            try:
                res = self._client.command_pull(
                    node_id, limit=1, wait=self._pull_wait)
                backoff = 1.0
                commands = res.get("commands") or []
                if not commands:
                    self._stop.wait(0.05)
                    continue
                for cmd in commands:
                    self._execute(cmd)
            except HubError as exc:
                if self._stop.is_set():
                    return
                if exc.code in (ERROR_NODE_NOT_FOUND, ERROR_NODE_OFFLINE):
                    self._reregister()
                    continue
                self._report(exc)
                self._stop.wait(backoff)
                backoff = min(backoff * 2, 30.0)
            except Exception as exc:  # noqa: BLE001
                if self._stop.is_set():
                    return
                self._report(exc)
                self._stop.wait(backoff)
                backoff = min(backoff * 2, 30.0)

    # ------------------------------------------------------------------
    # execution
    # ------------------------------------------------------------------

    def _execute(self, cmd):
        method = cmd.get("method") or ""
        handler = self._handler(method)
        if handler is None:
            self._complete(cmd, "failed", error={
                "code": ERROR_METHOD_NOT_FOUND,
                "message": "method not supported: %s" % method,
            })
            return
        params = cmd.get("params") or {}
        try:
            result = self._invoke(handler, params)
        except Exception as exc:  # noqa: BLE001 - reported as command failure
            self._complete(cmd, "failed", error=self._error_from(exc))
            return
        self._complete(cmd, "succeeded", result=result)

    def _invoke(self, handler, params):
        if not self._handler_timeout:
            return handler(params)
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(handler, params)
            try:
                return future.result(timeout=float(self._handler_timeout))
            except FutureTimeoutError:
                future.cancel()
                raise AgentHandlerError(
                    ERROR_INTERNAL_ERROR,
                    "handler timed out after %ss" % self._handler_timeout)
        finally:
            executor.shutdown(wait=False)

    def _complete(self, cmd, status, result=None, error=None):
        if status not in _TERMINAL:
            return
        node_id = cmd.get("node_id") or self.node_id
        command_id = cmd.get("id")
        if not node_id or not command_id:
            return
        p = {"node_id": node_id, "command_id": command_id, "status": status}
        if result is not None:
            p["result"] = result
        if error is not None:
            p["error"] = error
        try:
            self._client.command_complete(**p)
        except Exception as exc:  # noqa: BLE001
            if not self._stop.is_set():
                self._report(exc)

    @staticmethod
    def _error_from(exc):
        if isinstance(exc, AgentHandlerError):
            out = {"code": exc.code, "message": exc.message}
            if exc.data:
                out["data"] = exc.data
            return out
        if isinstance(exc, HubError):
            return {"code": exc.code, "message": exc.message}
        return {"code": ERROR_INTERNAL_ERROR, "message": str(exc)}

    def _unregister(self):
        node_id = self.node_id
        if not node_id:
            return
        try:
            self._client.node_unregister(node_id)
        except Exception as exc:  # noqa: BLE001
            self._report(exc)

    def _report(self, exc):
        if self._on_error is not None:
            try:
                self._on_error(exc)
                return
            except Exception:  # noqa: BLE001
                pass
        log.warning("musubi agent: %s", exc)
