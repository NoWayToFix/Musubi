"""HubClient: JSON-RPC 2.0 client for the Control Hub (docs/PROTOCOL.md).

Python 3.8+, zero third-party dependencies by default:

- Calls use ``urllib.request`` (HTTP POST /v1/rpc) with https, proxy, CA
  bundle and ``verify=False`` support.
- Events use long-polling on /v1/poll from a background thread with
  exponential-backoff reconnect running in the same thread.
- If the optional ``websocket-client`` package is installed and
  ``transport="ws"`` is set, a WebSocket transport is used; import failure
  degrades gracefully to HTTP + long-poll (never a hard dependency).

Compatibility: unknown fields in responses/events are ignored, missing fields
are tolerated, and unknown event types are delivered as-is (consumers must
ignore them per PROTOCOL.md §4.5).
"""

import json
import random
import ssl
import threading
import time
import urllib.error
import urllib.request
from queue import Empty, Queue

from .errors import HubError, ERROR_PROTOCOL_VERSION_MISMATCH

PROTOCOL_VERSION = "1.0"
DEFAULT_URL = "http://127.0.0.1:8080"
OFFLINE_QUEUE_LIMIT = 1000
EVENT_DEDUP_WINDOW = 5000

# Transport modes.
TRANSPORT_AUTO = "auto"
TRANSPORT_WS = "ws"
TRANSPORT_HTTP = "http"
TRANSPORT_POLL = "poll"


def _json_defaults(obj):
    """JSON encoder fallback for non-JSON-native values."""
    return str(obj)


class HubClient(object):
    """A JSON-RPC 2.0 client for the Control Hub.

    Args:
        url:            hub base URL (default http://127.0.0.1:8080).
        token:          optional static bearer token.
        transport:      "auto" | "ws" | "http" | "poll" (default "auto").
        timeout:        per-request timeout in seconds (default 30.0).
        max_retries:    reconnect retries; None = unlimited (default None).
        reconnect_base: exponential backoff base in seconds (default 1.0;
                        factor 2, max 30s, ±20% jitter).
        verify:         verify TLS certificates (default True).
        ca_bundle:      path to a CA bundle for custom CAs.
        proxy:          proxy URL, e.g. http://proxy:8080 (None = env default).
    """

    def __init__(self, url=None, token=None, transport=TRANSPORT_AUTO,
                 timeout=30.0, max_retries=None, reconnect_base=1.0,
                 verify=True, ca_bundle=None, proxy=None):
        self.url = (url or DEFAULT_URL).rstrip("/")
        self.token = token
        self.transport = transport
        self.timeout = timeout
        self.max_retries = max_retries  # None = unlimited
        self.reconnect_base = max(reconnect_base, 0.1)
        self.verify = verify
        self.ca_bundle = ca_bundle
        self.proxy = proxy

        self._lock = threading.RLock()
        self._id = 0
        self._server = None          # negotiated ServerInfo dict
        self._transport = "http"     # resolved transport ("ws" or "http")
        self._closed = False

        # Long-poll state (transport == "poll" or http-without-sse).
        self._poll_loop = None
        self._poll_cursor = 0
        self._poll_thread = None

        # Active subscriptions (id -> Subscription).
        self._subs = {}

        # Optional offline queue (PROTOCOL.md §6.2), see TODO below.
        self._offline = []

    # ------------------------------------------------------------------
    # low-level transport plumbing
    # ------------------------------------------------------------------

    def _ssl_context(self):
        if getattr(self, "_ssl_ctx", None) is not None:
            return self._ssl_ctx
        if not self.verify:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        elif self.ca_bundle:
            ctx = ssl.create_default_context(cafile=self.ca_bundle)
        else:
            ctx = ssl.create_default_context()
        self._ssl_ctx = ctx
        return ctx

    def _opener(self):
        if getattr(self, "_opener_obj", None) is not None:
            return self._opener_obj
        handlers = []
        if self.proxy:
            handlers.append(urllib.request.ProxyHandler({
                "http": self.proxy, "https": self.proxy}))
        if self.url.startswith("https"):
            # SSL context must be injected via HTTPSHandler (urllib does not
            # accept a `context` kwarg on OpenerDirector.open).
            handlers.append(urllib.request.HTTPSHandler(context=self._ssl_context()))
        opener = urllib.request.build_opener(*handlers)
        self._opener_obj = opener
        return opener

    def _http_post(self, payload):
        """POST one JSON-RPC envelope to /v1/rpc and return the decoded response."""
        req = urllib.request.Request(
            self.url + "/v1/rpc",
            data=json.dumps(payload, default=_json_defaults).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)
        try:
            with self._opener().open(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            # Some servers return JSON-RPC errors with non-2xx status; try to
            # decode the body anyway.
            raw = e.read()
            if not raw:
                raise HubError(e.code, "HTTP error %s" % e.code)
        except urllib.error.URLError as e:
            raise HubError(0, "network error: %s" % e.reason)
        return json.loads(raw.decode("utf-8"))

    def _resolve_transport(self):
        """Negotiate transport via engine.info (HTTP POST, never auth-required)."""
        if self.transport in (TRANSPORT_HTTP, TRANSPORT_POLL):
            return
        if self.transport == TRANSPORT_WS:
            # Forced WS: only use it when the optional websocket-client is
            # importable; otherwise calls degrade to HTTP.
            self._transport = TRANSPORT_WS if self._ws_available() else TRANSPORT_HTTP
            return
        if self._server is not None:
            return
        res = self._http_post({"jsonrpc": "2.0", "id": 0, "method": "engine.info",
                               "params": {}})
        if "error" in res:
            raise HubError(res["error"]["code"], res["error"].get("message", "error"),
                           res["error"].get("data"))
        si = res.get("result", {})
        transports = set(si.get("transports", []) or [])
        with self._lock:
            self._server = si
            if "ws" in transports and self._ws_available():
                self._transport = TRANSPORT_WS
            else:
                self._transport = TRANSPORT_HTTP

    def _backoff(self, retries):
        """Sleep with exponential backoff; return False when budget exhausted."""
        if self.max_retries is not None and retries >= self.max_retries:
            return False
        base = self.reconnect_base
        delay = min(base * (2 ** retries), 30.0)
        jitter = delay * 0.2
        time.sleep(max(0.1, delay + random.uniform(-jitter, jitter)))
        return True

    # ------------------------------------------------------------------
    # JSON-RPC calls
    # ------------------------------------------------------------------

    def _next_id(self):
        with self._lock:
            self._id += 1
            return self._id

    def call(self, method, params=None):
        """Perform a JSON-RPC request and return the result dict.

        Raises:
            HubError: on JSON-RPC error (use .code / .is_auth() / etc).
        """
        with self._lock:
            self._resolve_transport()
        req = {"jsonrpc": "2.0", "id": self._next_id(), "method": method}
        if params is not None:
            req["params"] = params

        if self._transport == TRANSPORT_WS:
            try:
                return self._call_ws(req)
            except HubError:
                raise  # authoritative JSON-RPC error; never re-send over HTTP
            except Exception:
                pass  # connection-level failure -> degrade to HTTP (§6.1)
        return self._call_http(req)

    def _call_http(self, req):
        env = self._http_post(req)
        if "error" in env and env["error"] is not None:
            err = env["error"]
            raise HubError(err.get("code", 0), err.get("message", "error"),
                           err.get("data"))
        return env.get("result") or {}

    # ------------------------------------------------------------------
    # engine.* / auth.*
    # ------------------------------------------------------------------

    def info(self, client_info=None):
        """Perform version negotiation; returns the ServerInfo dict.

        Raises HubError(-32005) when there is no protocol version overlap.
        """
        ci = dict(client_info) if client_info else {}
        ci.setdefault("name", "musubi-hub-sdk")
        ci.setdefault("version", "1.0.0")
        ci.setdefault("protocol_versions", [PROTOCOL_VERSION])
        ci.setdefault("encodings", ["json"])
        ci.setdefault("transports", ["ws", "http"])
        res = self.call("engine.info", {"client_info": ci})
        supported = res.get("supported_versions") or [PROTOCOL_VERSION]
        client_versions = ci["protocol_versions"]
        best = None
        for sv in supported:
            if sv in client_versions and (best is None or _version_newer(sv, best)):
                best = sv
        if best is None:
            raise HubError(
                ERROR_PROTOCOL_VERSION_MISMATCH,
                "protocol.version.mismatch",
                {"supported": supported, "client": client_versions},
            )
        res["protocol_version"] = best
        with self._lock:
            self._server = res
        return res

    def ping(self):
        """engine.ping."""
        return self.call("engine.ping", {})

    def capabilities(self):
        """engine.capabilities; returns {"capabilities": [...], "features": {...}}."""
        return self.call("engine.capabilities", {})

    def login(self, params=None):
        """auth.login; params per mode: {"token":...} | {"username","password"}."""
        params = params or {}
        res = self.call("auth.login", params)
        if res.get("access_token"):
            with self._lock:
                self.token = res["access_token"]
        return res

    def refresh(self, refresh_token):
        """auth.refresh; returns the new access_token result."""
        res = self.call("auth.refresh", {"refresh_token": refresh_token})
        if res.get("access_token"):
            with self._lock:
                self.token = res["access_token"]
        return res

    def logout(self):
        """auth.logout (revokes the current token)."""
        self.call("auth.logout", {})

    # ------------------------------------------------------------------
    # node.* / command.* / task.*
    # ------------------------------------------------------------------

    def node_list(self, node_filter=None, limit=None, offset=None):
        """node.list; node_filter: {status?, tags?}."""
        params = {}
        if node_filter:
            params["filter"] = node_filter
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        return self.call("node.list", params)

    def node_get(self, node_id):
        """node.get; raises HubError(-32010) if not found."""
        return self.call("node.get", {"node_id": node_id})

    def command_send(self, node_id, method, params=None, timeout=None,
                     idempotency_key=None):
        """command.send; raises HubError(-32011) if the node is offline."""
        p = {"node_id": node_id, "method": method}
        if params:
            p["params"] = params
        if timeout is not None:
            p["timeout"] = timeout
        if idempotency_key:
            p["idempotency_key"] = idempotency_key
        return self.call("command.send", p)

    def command_batch(self, commands, idempotency_key=None):
        """command.batch; commands: list of {node_id, method, params?, timeout?}."""
        p = {"commands": commands}
        if idempotency_key:
            p["idempotency_key"] = idempotency_key
        return self.call("command.batch", p)

    def command_cancel(self, command_id):
        """command.cancel; raises HubError(-32014) if already terminal."""
        return self.call("command.cancel", {"command_id": command_id})

    def task_create(self, name, commands, parallel=False):
        """task.create; starts immediately."""
        return self.call("task.create",
                         {"name": name, "commands": commands, "parallel": parallel})

    def task_get(self, task_id):
        """task.get; raises HubError(-32020) if not found."""
        return self.call("task.get", {"task_id": task_id})

    def task_list(self, task_filter=None, limit=None, offset=None):
        """task.list; task_filter: {status?}."""
        params = {}
        if task_filter:
            params["filter"] = task_filter
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        return self.call("task.list", params)

    def task_cancel(self, task_id):
        """task.cancel; raises HubError(-32021) if already terminal."""
        return self.call("task.cancel", {"task_id": task_id})

    # ------------------------------------------------------------------
    # subscriptions
    # ------------------------------------------------------------------

    def subscribe(self, filters=None, on_event=None):
        """Subscribe to events; returns a Subscription.

        Filters: {event_types?: [str], node_id?: str, task_id?: str}.
        Events are delivered via Subscription.events (a queue.Queue) and the
        optional ``on_event`` callback. Deduplication by event.id (window 5000).
        """
        sub = Subscription(self, filters, on_event)
        sub._open()
        with self._lock:
            self._subs[sub.id] = sub
        # Events are delivered via long-poll (per PROTOCOL.md §6.1 poll
        # fallback). A WebSocket event pump is not required for this SDK;
        # subscriptions over ws still receive events via long-poll.
        self._start_poll_loop()
        return sub

    def _unsubscribe(self, subscription_id):
        try:
            self.call("event.unsubscribe", {"subscription_id": subscription_id})
        except HubError:
            pass  # already gone / subscription.not_found is fine on close
        with self._lock:
            self._subs.pop(subscription_id, None)

    # ------------------------------------------------------------------
    # long-poll loop
    # ------------------------------------------------------------------

    def _start_poll_loop(self):
        with self._lock:
            if self._poll_thread is not None and self._poll_thread.is_alive():
                return
            self._poll_loop = True
            self._poll_thread = threading.Thread(
                target=self._poll_worker, name="hub-poll", daemon=True)
            self._poll_thread.start()

    def _poll_worker(self):
        """Background long-poll loop with exponential backoff (cursor-based)."""
        retries = 0
        seen = _Dedup(EVENT_DEDUP_WINDOW)
        while True:
            with self._lock:
                if self._closed or not self._poll_loop:
                    return
            try:
                events, cursor = self._poll_once(self._poll_cursor)
            except HubError as e:
                if not self._backoff(retries):
                    return
                retries += 1
                continue
            retries = 0
            with self._lock:
                if cursor > self._poll_cursor:
                    self._poll_cursor = cursor
            for ev in events:
                if not seen.add(ev.get("id")):
                    continue  # dedup window (5000)
                self._dispatch_event(None, ev)

    def _poll_once(self, cursor):
        """One long-poll request; returns (events, new_cursor)."""
        params = {"cursor": cursor, "timeout": 30}
        if self.token:
            params["token"] = self.token
        import urllib.parse
        qs = urllib.parse.urlencode(params)
        req = urllib.request.Request(self.url + "/v1/poll?" + qs, method="GET")
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)
        try:
            with self._opener().open(req, timeout=40) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            raw = e.read()
        except urllib.error.URLError as e:
            raise HubError(0, "poll network error: %s" % e.reason)
        data = json.loads(raw.decode("utf-8"))
        return data.get("events") or [], data.get("cursor", cursor)

    # ------------------------------------------------------------------
    # WebSocket transport (optional)
    # ------------------------------------------------------------------

    def _ws_available(self):
        if self._transport != TRANSPORT_WS:
            return False
        try:
            import websocket  # noqa: F401  (optional extra)
        except ImportError:
            return False
        return True

    def _ws_url(self):
        if self.url.startswith("https"):
            return "wss://" + self.url[len("https://"):] + "/v1/ws"
        return "ws://" + self.url[len("http://"):] + "/v1/ws"

    def _call_ws(self, req):
        """Best-effort WebSocket call; caller falls back to HTTP on failure."""
        import websocket  # optional extra
        ws = websocket.create_connection(
            self._ws_url(),
            timeout=self.timeout,
            header=["Authorization: Bearer %s" % self.token] if self.token else [],
            sslopt={"cert_reqs": ssl.CERT_NONE} if not self.verify else {},
        )
        try:
            ws.send(json.dumps(req, default=_json_defaults))
            raw = ws.recv()
        finally:
            ws.close()
        env = json.loads(raw)
        if "error" in env and env["error"] is not None:
            err = env["error"]
            raise HubError(err.get("code", 0), err.get("message", "error"),
                           err.get("data"))
        return env.get("result") or {}

    def _dispatch_event(self, subscription_id, ev):
        """Deliver one event to matching subscriptions (unknown types pass through)."""
        with self._lock:
            subs = list(self._subs.values())
        for sub in subs:
            if sub._matches(ev):
                sub._deliver(ev)

    # ------------------------------------------------------------------
    # offline queue (optional, PROTOCOL.md §6.2)
    # ------------------------------------------------------------------

    def queue_write(self, request):
        """Enqueue a write operation while offline (limit 1000, FIFO).

        TODO: wire this into :meth:`call` so command.send / task.create /
        node.* calls made while disconnected are queued and replayed after
        reconnect. The default is "at-most-once" — transport errors surface.
        """
        with self._lock:
            if len(self._offline) >= OFFLINE_QUEUE_LIMIT:
                return False
            self._offline.append(request)
            return True

    def close(self):
        """Shut down background loops and release resources."""
        with self._lock:
            self._closed = True
            self._poll_loop = False
        for sub in list(self._subs.values()):
            sub.close()


def _version_newer(a, b):
    """True when version a > b ("MAJOR.MINOR" semantics)."""
    if not b:
        return True
    def parts(v):
        p = v.split(".")
        return int(p[0]), int(p[1]) if len(p) > 1 else 0
    return parts(a) > parts(b)


class Subscription(object):
    """An active event subscription.

    Attributes:
        id:      server-assigned subscription id.
        events:  queue.Queue of delivered HubEvent dicts.
    """

    def __init__(self, client, filters=None, on_event=None):
        self._client = client
        self.id = None
        self.filters = filters or {}
        self.events = Queue()
        self.on_event = on_event
        self._closed = False

    def _open(self):
        params = {}
        if self.filters.get("event_types"):
            params["event_types"] = self.filters["event_types"]
        if self.filters.get("node_id"):
            params["node_id"] = self.filters["node_id"]
        if self.filters.get("task_id"):
            params["task_id"] = self.filters["task_id"]
        res = self._client.call("event.subscribe", params)
        self.id = res.get("subscription_id")

    def _matches(self, ev):
        f = self.filters
        if f.get("event_types") and ev.get("type") not in f["event_types"]:
            return False
        if f.get("node_id") and ev.get("node_id") != f["node_id"]:
            return False
        if f.get("task_id") and ev.get("task_id") != f["task_id"]:
            return False
        return True

    def _deliver(self, ev):
        self.events.put(ev)
        if self.on_event is not None:
            try:
                self.on_event(ev)
            except Exception:
                pass  # callbacks must never break the delivery loop

    def unsubscribe(self):
        """Unsubscribe from the hub (event.unsubscribe)."""
        if self.id is not None:
            self._client._unsubscribe(self.id)
            self.id = None

    def close(self):
        """Alias for unsubscribe(); idempotent."""
        self.unsubscribe()

    def get(self, timeout=None):
        """Blocking read of the next event; raises queue.Empty on timeout."""
        return self.events.get(timeout=timeout)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class _Dedup(object):
    """Remembers event ids within a sliding window (PROTOCOL.md §6.2: 5000)."""

    def __init__(self, limit):
        self.limit = limit
        self._ids = set()
        self._order = []

    def add(self, event_id):
        if not event_id:
            return True
        if event_id in self._ids:
            return False
        self._ids.add(event_id)
        self._order.append(event_id)
        if len(self._order) > self.limit:
            for old in self._order[:len(self._order) - self.limit]:
                self._ids.discard(old)
            self._order = self._order[len(self._order) - self.limit:]
        return True