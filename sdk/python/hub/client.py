"""HubClient: JSON-RPC 2.0 client for the Control Hub (docs/PROTOCOL.md).

Python 3.8+, zero third-party dependencies by default:

- Calls use ``urllib.request`` (HTTP POST /v1/rpc) with https, proxy, CA
  bundle and ``verify=False`` support.
- Events use SSE (GET /v1/events) when the server advertises it, otherwise
  long-polling on /v1/poll, from a background thread with exponential-backoff
  reconnect (PROTOCOL.md §6.1 fallback order ws -> sse -> poll). The loop is
  resilient: transient failures are logged and retried; terminal auth
  failures (401/403) stop delivery and are observable via
  ``HubClient.poll_error`` and ``Subscription.error``.
- Writes made while disconnected are buffered in the offline queue (§6.2,
  limit 1000, FIFO) and replayed in order once connectivity returns
  (``queue_offline=True`` by default; engine.*/auth.*/event.* never queue).
- If the optional ``websocket-client`` package is installed, calls may use
  the WebSocket transport (server-advertised or ``transport="ws"``); import
  failure degrades gracefully to HTTP (never a hard dependency). A WS request
  that provably never reached the server (connect/send-phase failure) falls
  back to HTTP; after the frame was written the transport error is surfaced
  instead of re-sent, so a command/task can never execute twice.

Compatibility: unknown fields in responses/events are ignored, missing fields
are tolerated, and unknown event types are delivered as-is (consumers must
ignore them per PROTOCOL.md §4.5).
"""

import json
import logging
import random
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from queue import Empty, Queue

from .errors import (
    ERROR_AUTH_REQUIRED,
    ERROR_AUTH_FORBIDDEN,
    ERROR_AUTH_EXPIRED,
    ERROR_AUTH_INVALID,
    ERROR_PROTOCOL_VERSION_MISMATCH,
    HubError,
)

_log = logging.getLogger(__name__)
_log.addHandler(logging.NullHandler())

PROTOCOL_VERSION = "1.0"
DEFAULT_URL = "http://127.0.0.1:8080"
OFFLINE_QUEUE_LIMIT = 1000
EVENT_DEDUP_WINDOW = 5000

# Transport modes.
TRANSPORT_AUTO = "auto"
TRANSPORT_WS = "ws"
TRANSPORT_HTTP = "http"
TRANSPORT_POLL = "poll"

# Time after which a healthy SSE stream resets the reconnect backoff.
SSE_HEALTHY_LIFETIME = 30.0


class _WsNotSent(Exception):
    """WebSocket failure before the request frame was written (connect or
    send phase): the request provably never reached the server, so falling
    back to HTTP (PROTOCOL.md §6.1) cannot duplicate it."""


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
        queue_offline:  buffer eligible writes (not engine.*/auth.*/event.*)
                        made while disconnected and replay them in order on
                        reconnect (PROTOCOL.md §6.2; default True).
    """

    def __init__(self, url=None, token=None, transport=TRANSPORT_AUTO,
                 timeout=30.0, max_retries=None, reconnect_base=1.0,
                 verify=True, ca_bundle=None, proxy=None, queue_offline=True):
        self.url = (url or DEFAULT_URL).rstrip("/")
        self.token = token
        self.transport = transport
        self.timeout = timeout
        self.max_retries = max_retries  # None = unlimited
        self.reconnect_base = max(reconnect_base, 0.1)
        self.verify = verify
        self.ca_bundle = ca_bundle
        self.proxy = proxy
        self.queue_offline = queue_offline

        self._lock = threading.RLock()
        self._id = 0
        self._server = None          # negotiated ServerInfo dict
        self._transport = "http"     # resolved transport ("ws" or "http")
        self._closed = False
        self._refresh_token = None   # rotated together with the access token

        # Event delivery state (SSE or long-poll; see _start_event_loop).
        self._poll_loop = None
        self._poll_cursor = 0
        self._poll_thread = None
        self._poll_error = None      # terminal delivery error (observable)

        # SSE state (transport != "ws" and the server advertises "sse").
        self._sse_loop_on = False
        self._sse_thread = None
        self._sse_resp = None

        # Active subscriptions (id -> Subscription).
        self._subs = {}

        # Offline queue (PROTOCOL.md §6.2), drained on reconnect.
        self._offline = []
        self._draining = False

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
            if e.code in (502, 503, 504):
                # Gateway error: the upstream never received the request, so
                # re-sending / queueing it cannot duplicate an execution.
                raise HubError(e.code, "HTTP error %s" % e.code, {"phase": "connect"})
            if not raw:
                raise HubError(e.code, "HTTP error %s" % e.code)
        except urllib.error.URLError as e:
            # Connect/request-send failure: the request never reached the
            # server (safe to re-send / queue, PROTOCOL.md §6.2).
            raise HubError(0, "network error: %s" % e.reason, {"phase": "connect"})
        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError as e:
            # The request was sent (the server answered), so this is NOT
            # re-sendable: surface it as a HubError instead of a raw
            # JSONDecodeError.
            raise HubError(0, "invalid response body: %s" % e)

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

    @staticmethod
    def _resendable(err):
        """True when a transport failure proves the request never reached the
        server (connect phase), so re-sending / queueing it cannot duplicate
        an execution (PROTOCOL.md §6.2 at-most-once default)."""
        return isinstance(err, HubError) and err.data.get("phase") == "connect"

    def call(self, method, params=None):
        """Perform a JSON-RPC request and return the result dict.

        Raises:
            HubError: on JSON-RPC error (use .code / .is_auth() / etc).

        Network semantics (PROTOCOL.md §6.2):
            - Transport failures that prove the request never executed
              (connect phase) fall back / retry; failures after the request
              was written are surfaced (HubError with ``data["phase"] ==
              "post-send"``) so a command/task can never run twice.
            - With ``queue_offline`` (default), eligible writes that fail
              this way are buffered (limit 1000, FIFO) and replayed in order
              once connectivity returns; the raised HubError then carries
              ``data["queued"] = True``.
            - A call rejected with auth.expired/auth.invalid while a refresh
              token is stored is retried once after refreshing both tokens.
        """
        with self._lock:
            try:
                self._resolve_transport()
            except HubError as e:
                if self._resendable(e) and self._queue_if_offline(method, params):
                    raise HubError(0, "transport offline; %s queued for replay" % method,
                                   {"queued": True})
                raise
        req = {"jsonrpc": "2.0", "id": self._next_id(), "method": method}
        if params is not None:
            req["params"] = params
        try:
            result = self._send_with_retry(req, method)
        except HubError as e:
            if self._resendable(e) and self._queue_if_offline(method, params):
                raise HubError(0, "transport offline; %s queued for replay" % method,
                               {"queued": True})
            raise
        # Connectivity just proved good: replay anything queued while offline.
        self._maybe_drain_offline()
        return result

    def _send_with_retry(self, req, method):
        """Send ``req`` once, retrying once after a token refresh on auth
        errors. auth.refresh rotates BOTH tokens (PROTOCOL.md §3.5), so a
        call failing with the just-rotated-away access token succeeds on
        retry with the new one. auth.* calls never retry: they are the
        refresh path itself.
        """
        try:
            return self._send_once(req)
        except HubError as e:
            if (method.startswith("auth.")
                    or not self._refresh_token
                    or e.code not in (ERROR_AUTH_EXPIRED, ERROR_AUTH_INVALID)):
                raise
            with self._lock:
                token_before = self.token
            try:
                self.refresh()
            except HubError:
                pass  # refresh failed: fall through, surface the original
            with self._lock:
                if self.token == token_before:
                    raise  # rotation produced nothing new: surface the error
            return self._send_once(req)

    def _send_once(self, req):
        """Send one envelope over the resolved transport (no queue/retry).

        WS fallback classification (§6.1): only connect-phase or send-phase
        failures — the request frame provably never reached the server —
        degrade to HTTP. After the frame was written the server may already
        have executed the request, so the transport error is raised as an
        authoritative HubError instead of being re-sent (duplicate
        execution). The two cases are distinguished inside :meth:`_call_ws`
        by *where* the failure happened relative to the single frame write.
        """
        if self._transport == TRANSPORT_WS:
            try:
                return self._call_ws(req)
            except HubError:
                raise  # authoritative JSON-RPC error; never re-send over HTTP
            except _WsNotSent:
                pass  # connect/send-phase failure -> degrade to HTTP (§6.1)
            except Exception as e:
                raise HubError(0, "ws error after send; not re-sent: %s" % e,
                               {"phase": "post-send"})
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
        with self._lock:
            if res.get("access_token"):
                self.token = res["access_token"]
            if res.get("refresh_token"):
                self._refresh_token = res["refresh_token"]
        return res

    def refresh(self, refresh_token=None):
        """auth.refresh; returns the new token result.

        When the engine rotates (PROTOCOL.md §3.5), the response carries BOTH
        a new access_token and a new refresh_token and invalidates the old
        pair: both stored values are updated here. With no argument the
        stored refresh token is used.
        """
        rt = refresh_token or self._refresh_token
        if not rt:
            raise HubError(ERROR_AUTH_REQUIRED, "no refresh_token available")
        res = self.call("auth.refresh", {"refresh_token": rt})
        with self._lock:
            if res.get("access_token"):
                self.token = res["access_token"]
            if res.get("refresh_token"):
                # Rotation: the old refresh token is now invalid — replace it.
                self._refresh_token = res["refresh_token"]
        return res

    def logout(self):
        """auth.logout (revokes the current token)."""
        self.call("auth.logout", {})
        with self._lock:
            self._refresh_token = None

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
    # seats (PROTOCOL.md §4.7)
    # ------------------------------------------------------------------

    def seat_list(self, seat_filter=None, limit=None, offset=None):
        """seat.list; seat_filter: {kind?: "standing"|"vacant", node_id?}."""
        params = {}
        if seat_filter:
            params["filter"] = seat_filter
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        return self.call("seat.list", params)

    def seat_create(self, name=None, bind_policy=None, expect=None):
        """seat.create; creates a vacant seat (bind_policy: "auto"|"manual")."""
        p = {}
        if name:
            p["name"] = name
        if bind_policy:
            p["bind_policy"] = bind_policy
        if expect is not None:
            p["expect"] = expect
        return self.call("seat.create", p)

    def seat_get(self, seat_id):
        """seat.get; raises HubError(-32050) if not found."""
        return self.call("seat.get", {"seat_id": seat_id})

    def seat_delete(self, seat_id):
        """seat.delete; only vacant seats; raises HubError(-32051) otherwise."""
        return self.call("seat.delete", {"seat_id": seat_id})

    def seat_bind(self, seat_id, node_id):
        """seat.bind; manual fill; raises HubError(-32051)/(-32052)."""
        return self.call("seat.bind", {"seat_id": seat_id, "node_id": node_id})

    def seat_unbind(self, seat_id):
        """seat.unbind; release the occupant, keep the shell (idempotent)."""
        return self.call("seat.unbind", {"seat_id": seat_id})

    # ------------------------------------------------------------------
    # subscriptions
    # ------------------------------------------------------------------

    def subscribe(self, filters=None, on_event=None):
        """Subscribe to events; returns a Subscription.

        Filters: {event_types?: [str], node_id?: str, task_id?: str}.
        Events are delivered via Subscription.events (a queue.Queue) and the
        optional ``on_event`` callback. Deduplication by event.id (window 5000).

        Events arrive over the transport-level stream (SSE, or long-poll when
        SSE is unavailable — PROTOCOL.md §6.1), so subscriptions are matched
        client-side and no event.subscribe round-trip is made (an unread
        server-side subscription would only fill up and drop events).
        Delivery failures surface via :attr:`poll_error` and
        ``Subscription.error``.
        """
        sub = Subscription(self, filters, on_event)
        sub._open_local()
        with self._lock:
            self._subs[sub.id] = sub
        self._start_event_loop()
        return sub

    def _unsubscribe(self, subscription_id):
        if not str(subscription_id).startswith("local-"):
            try:
                self.call("event.unsubscribe", {"subscription_id": subscription_id})
            except HubError:
                pass  # already gone / subscription.not_found is fine on close
        with self._lock:
            self._subs.pop(subscription_id, None)

    # ------------------------------------------------------------------
    # event delivery: SSE (preferred) with long-poll fallback (§6.1)
    # ------------------------------------------------------------------

    @property
    def poll_error(self):
        """Terminal event-delivery error, or None while delivering.

        Set when the background delivery loop stopped permanently: an auth
        rejection (poll/SSE HTTP 401/403, surfaced as a HubError whose
        ``.is_auth()`` is True) or exhausting ``max_retries`` on transient
        failures. Delivery is down while this is non-None; the affected
        subscriptions carry the same error in ``Subscription.error``. A
        later :meth:`subscribe` (e.g. after re-login) restarts delivery.
        """
        with self._lock:
            return self._poll_error

    def _stop_event_delivery(self, error):
        """Record a terminal delivery failure instead of dying silently."""
        _log.error("event delivery stopped: %s", error)
        with self._lock:
            self._poll_loop = False
            self._sse_loop_on = False
            self._poll_error = error
            subs = list(self._subs.values())
        for sub in subs:
            sub._set_error(error)

    def _sse_available(self):
        """True when the negotiated server advertises the SSE transport."""
        with self._lock:
            si = self._server
        if si is None:
            return False
        if "sse" in (si.get("transports") or []):
            return True
        return (si.get("features") or {}).get("sse") is True

    def _start_event_loop(self):
        """Start event delivery: SSE when available, long-poll otherwise
        (PROTOCOL.md §6.1 fallback order ws -> sse -> poll; this SDK uses the
        WS transport for calls only, events use the HTTP fallbacks)."""
        with self._lock:
            if self._closed:
                return
            use_sse = (self._transport != TRANSPORT_WS
                       and self.transport != TRANSPORT_POLL
                       and self._sse_available())
        if use_sse:
            try:
                self._start_sse_loop()
                return
            except Exception as e:
                _log.warning("sse unavailable (%r); falling back to long-poll", e)
        self._start_poll_loop()

    def _start_poll_loop(self):
        with self._lock:
            if self._poll_thread is not None and self._poll_thread.is_alive():
                return
            self._poll_loop = True
            self._poll_error = None
            self._poll_thread = threading.Thread(
                target=self._poll_worker, name="hub-poll", daemon=True)
            self._poll_thread.start()

    def _poll_worker(self):
        """Background long-poll loop with exponential backoff (cursor-based).

        Resilience (PROTOCOL.md §6.2): any per-iteration failure — network
        drop, proxy 5xx, non-JSON body — is logged and retried with backoff;
        one bad response can never kill event delivery. The loop only stops
        on close() or a terminal auth failure (401/403), which is surfaced
        via :attr:`poll_error` instead of spinning or dying silently.
        """
        retries = 0
        seen = _Dedup(EVENT_DEDUP_WINDOW)
        while True:
            with self._lock:
                if self._closed or not self._poll_loop:
                    return
            try:
                events, cursor = self._poll_once(self._poll_cursor)
            except HubError as e:
                if e.is_auth():
                    self._stop_event_delivery(e)  # terminal: don't spin
                    return
                error = e
            except Exception as e:  # JSONDecodeError, socket errors, anything
                error = e
            else:
                error = None
            if error is not None:
                retries += 1
                _log.warning("poll error (retry %d): %r", retries, error)
                with self._lock:
                    # The connection was lost, so the engine may have
                    # restarted: a stale cursor could starve us forever (the
                    # restarted bus returns empty results for cursors it
                    # never issued). Reset to 0; event-id dedup absorbs the
                    # replays on a healthy engine (window 5000).
                    self._poll_cursor = 0
                if not self._backoff(retries - 1):
                    self._stop_event_delivery(HubError(
                        0, "event delivery stopped: poll gave up after %d "
                           "retries (last error: %r)" % (retries, error)))
                    return
                continue
            retries = 0
            with self._lock:
                if cursor > self._poll_cursor:
                    self._poll_cursor = cursor
            for ev in events:
                if not seen.add(ev.get("id")):
                    continue  # dedup window (5000)
                self._dispatch_event(None, ev)
            # Connectivity proved good: replay writes queued while offline.
            self._maybe_drain_offline()

    def _poll_once(self, cursor):
        """One long-poll request; returns (events, new_cursor)."""
        params = {"cursor": cursor, "timeout": 30}
        if self.token:
            params["token"] = self.token
        qs = urllib.parse.urlencode(params)
        req = urllib.request.Request(self.url + "/v1/poll?" + qs, method="GET")
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)
        try:
            with self._opener().open(req, timeout=40) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                # Terminal: the server rejected our credentials. Surfaced as
                # an auth HubError so the worker stops instead of spinning
                # (a plain-text or JSON error body is not a poll envelope).
                raise HubError(
                    ERROR_AUTH_REQUIRED if e.code == 401 else ERROR_AUTH_FORBIDDEN,
                    "poll http %s" % e.code)
            raw = e.read()
            if e.code in (502, 503, 504):
                # Gateway error: the engine never saw the request.
                raise HubError(e.code, "poll http %s" % e.code, {"phase": "connect"})
            if not raw:
                raise HubError(e.code, "poll http %s" % e.code)
            try:
                data = json.loads(raw.decode("utf-8"))
            except ValueError:
                raise HubError(e.code, "poll http %s: non-JSON body" % e.code)
            if isinstance(data, dict) and data.get("error"):
                err = data["error"]
                raise HubError(err.get("code", 0), err.get("message", "poll error"),
                               err.get("data"))
            raise HubError(e.code, "poll http %s" % e.code)
        except urllib.error.URLError as e:
            raise HubError(0, "poll network error: %s" % e.reason, {"phase": "connect"})
        try:
            data = json.loads(raw.decode("utf-8"))
        except ValueError:
            # Proxy/error-page garbage: transient transport failure (the
            # worker logs it, resets the cursor and retries).
            raise HubError(0, "poll: non-JSON response body")
        return data.get("events") or [], data.get("cursor", cursor)

    # ------------------------------------------------------------------
    # SSE transport (GET /v1/events, PROTOCOL.md §6)
    # ------------------------------------------------------------------

    def _start_sse_loop(self):
        """Open the SSE stream synchronously (so subscribe() cannot miss
        events between subscribe and stream) and spawn the reader thread.
        Raises on connect failure; the caller falls back to long-poll."""
        with self._lock:
            if self._sse_thread is not None and self._sse_thread.is_alive():
                return
            resp = self._sse_connect()
            self._sse_loop_on = True
            self._poll_error = None
            self._sse_resp = resp
            self._sse_thread = threading.Thread(
                target=self._sse_worker, args=(resp,), name="hub-sse", daemon=True)
            self._sse_thread.start()

    def _sse_connect(self):
        """Open one GET /v1/events stream (no overall timeout: this is a
        stream; the socket timeout only bounds silence between frames, and
        the engine sends a heartbeat every 15s)."""
        url = self.url + "/v1/events"
        if self.token:
            url += "?" + urllib.parse.urlencode({"token": self.token})
        req = urllib.request.Request(url, method="GET")
        req.add_header("Accept", "text/event-stream")
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)
        try:
            return self._opener().open(req, timeout=60)
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                raise HubError(
                    ERROR_AUTH_REQUIRED if e.code == 401 else ERROR_AUTH_FORBIDDEN,
                    "sse http %s" % e.code)
            raise HubError(0, "sse http %s" % e.code)
        except urllib.error.URLError as e:
            raise HubError(0, "sse network error: %s" % e.reason)

    def _sse_worker(self, resp):
        """Consume SSE streams until close(): reconnect with exponential
        backoff on any drop (a clean EOF is still a drop). Terminal auth
        failures stop delivery (observable via :attr:`poll_error`)."""
        retries = 0
        seen = _Dedup(EVENT_DEDUP_WINDOW)
        while True:
            # Connect phase (the first stream is passed in already open).
            while resp is None:
                with self._lock:
                    if self._closed or not self._sse_loop_on:
                        return
                try:
                    resp = self._sse_connect()
                except HubError as e:
                    if e.is_auth():
                        self._stop_event_delivery(e)  # terminal: don't spin
                        return
                    error = e
                except Exception as e:
                    error = e
                else:
                    error = None
                if error is not None:
                    retries += 1
                    _log.warning("sse connect failed (retry %d): %r", retries, error)
                    if not self._backoff(retries - 1):
                        self._stop_event_delivery(HubError(
                            0, "event delivery stopped: sse connect gave up "
                               "after %d retries (last error: %r)"
                               % (retries, error)))
                        return
            with self._lock:
                self._sse_resp = resp
            # Stream phase.
            started = time.time()
            error = self._sse_read(resp, seen)
            lifetime = time.time() - started
            self._sse_drop(resp)
            resp = None
            with self._lock:
                if self._closed or not self._sse_loop_on:
                    return
            if isinstance(error, HubError) and error.is_auth():
                self._stop_event_delivery(error)  # terminal: don't spin
                return
            if error is not None:
                _log.warning("sse stream dropped after %.0fs: %r", lifetime, error)
            if lifetime >= SSE_HEALTHY_LIFETIME:
                retries = 0  # a long-lived stream proves connectivity
            retries += 1
            if not self._backoff(retries - 1):
                self._stop_event_delivery(HubError(
                    0, "event delivery stopped: sse gave up after %d retries "
                       "(last error: %r)" % (retries, error)))
                return

    def _sse_read(self, resp, seen):
        """Consume one SSE response body, dispatching events as they arrive.

        Frames are ``data: <JSON-RPC notification>``; ``:``-prefixed lines
        are heartbeat comments and are ignored. Returns None on clean EOF or
        the exception that ended the stream (the worker reconnects).
        """
        data_lines = []
        while True:
            with self._lock:
                if self._closed or not self._sse_loop_on:
                    return None
            try:
                line = resp.readline()
            except Exception as e:
                return e
            if not line:
                return None  # clean EOF: the stream ended; reconnect
            line = line.decode("utf-8", "replace").rstrip("\r\n")
            if line.startswith(":"):
                continue  # heartbeat comment — ignored
            if line.startswith("data:"):
                payload = line[len("data:"):]
                if payload.startswith(" "):
                    payload = payload[1:]
                data_lines.append(payload)
                continue
            if not line and data_lines:
                ev = self._sse_parse_event("\n".join(data_lines))
                data_lines = []
                if ev is not None and seen.add(ev.get("id")):
                    self._dispatch_event(None, ev)

    @staticmethod
    def _sse_parse_event(payload):
        """Parse one SSE data frame: a JSON-RPC notification whose method is
        'event' and whose params.event is the HubEvent (PROTOCOL.md §6).
        Unknown or malformed notifications are ignored (§4.5)."""
        try:
            env = json.loads(payload)
        except ValueError:
            return None
        if not isinstance(env, dict) or env.get("method") != "event":
            return None
        params = env.get("params")
        if not isinstance(params, dict):
            return None
        ev = params.get("event")
        return ev if isinstance(ev, dict) else None

    def _sse_drop(self, resp):
        """Forget and close one SSE response (unblocks the reader on close)."""
        with self._lock:
            if resp is not None and resp is self._sse_resp:
                self._sse_resp = None
        try:
            resp.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # WebSocket transport (optional)
    # ------------------------------------------------------------------

    def _ws_available(self):
        """True when the optional websocket-client extra is importable.

        Deliberately independent of the resolved transport: callers check
        this *during* transport negotiation, before ``self._transport`` is
        set to "ws" (checking the resolved transport here made WS
        unreachable and silently degraded everything to HTTP).
        """
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
        """Best-effort WebSocket call.

        Raises _WsNotSent when the failure happened before the request frame
        was written (connect or send phase) — the caller degrades to HTTP.
        Failures after the frame was written (recv/decode) are raised as an
        authoritative HubError: the server may already have executed the
        request, and re-sending it could run the command/task twice
        (PROTOCOL.md §6.2 at-most-once semantics).
        """
        import websocket  # optional extra
        try:
            ws = websocket.create_connection(
                self._ws_url(),
                timeout=self.timeout,
                header=["Authorization: Bearer %s" % self.token] if self.token else [],
                sslopt={"cert_reqs": ssl.CERT_NONE} if not self.verify else {},
            )
        except Exception as e:
            raise _WsNotSent("ws connect failed: %s" % e)
        try:
            try:
                ws.send(json.dumps(req, default=_json_defaults))
            except Exception as e:
                raise _WsNotSent("ws send failed: %s" % e)
            raw = ws.recv()
        finally:
            try:
                ws.close()
            except Exception:
                pass
        try:
            env = json.loads(raw)
        except ValueError as e:
            raise HubError(0, "ws: invalid response: %s" % e, {"phase": "post-send"})
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
    # offline queue (PROTOCOL.md §6.2)
    # ------------------------------------------------------------------

    def _queue_if_offline(self, method, params):
        """Buffer a write for replay after reconnect (PROTOCOL.md §6.2).

        Only eligible when ``queue_offline`` is enabled and the method is a
        write: engine.*, auth.* and event.* are session-scoped and never
        replayed. Returns True when queued (limit 1000, FIFO; a full queue
        rejects the new entry and the transport error surfaces as before).
        """
        if not self.queue_offline:
            return False
        if method.startswith(("engine.", "auth.", "event.")):
            return False
        with self._lock:
            if len(self._offline) >= OFFLINE_QUEUE_LIMIT:
                return False
            self._offline.append({"method": method, "params": params})
            return True

    def queue_write(self, request):
        """Manually enqueue a write request ``{"method": ..., "params": ...}``
        for replay after reconnect (limit 1000, FIFO; False when full).

        This is the manual API; with ``queue_offline`` (default) calls made
        through :meth:`call` (e.g. command.send / task.create) while
        disconnected are queued automatically and replayed in order once
        connectivity returns.
        """
        with self._lock:
            if len(self._offline) >= OFFLINE_QUEUE_LIMIT:
                return False
            self._offline.append(request)
            return True

    def _maybe_drain_offline(self):
        """Replay queued writes in order (FIFO) once.

        Called whenever connectivity is re-proved (a successful call or a
        successful long-poll). Re-entrant and concurrent calls fold into the
        running drain. An item failing with a connect-phase error puts the
        remaining items back at the queue head (order preserved) and waits
        for the next reconnect; an authoritative JSON-RPC error means the
        server saw the write, so the item is dropped and draining continues.
        """
        with self._lock:
            if self._draining or not self._offline:
                return
            self._draining = True
            pending = self._offline
            self._offline = []
        try:
            for i, item in enumerate(pending):
                method = item.get("method")
                req = {"jsonrpc": "2.0", "id": self._next_id(), "method": method}
                if item.get("params") is not None:
                    req["params"] = item["params"]
                try:
                    self._send_with_retry(req, method)
                    _log.info("offline queue: replayed %s", method)
                except HubError as e:
                    if self._resendable(e):
                        # Still offline: restore the remainder (in order).
                        with self._lock:
                            self._offline = pending[i:] + self._offline
                        return
                    _log.warning("offline queue: %s replay failed: %s", method, e)
                except Exception as e:
                    with self._lock:
                        self._offline = pending[i:] + self._offline
                    _log.warning("offline queue: replay interrupted: %r", e)
                    return
        finally:
            with self._lock:
                self._draining = False

    def close(self):
        """Shut down background loops and release resources."""
        with self._lock:
            self._closed = True
            self._poll_loop = False
            self._sse_loop_on = False
            resp = self._sse_resp
            self._sse_resp = None
        if resp is not None:
            try:
                resp.close()
            except Exception:
                pass
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
        id:      subscription id. Events arrive over the shared
                 transport-level stream (SSE / long-poll) and are matched
                 client-side, so ids are local ("local-N"); only a raw
                 event.subscribe call returns a server-assigned id.
        events:  queue.Queue of delivered HubEvent dicts.
        error:   terminal delivery error (HubError) or None; set together
                 with ``HubClient.poll_error`` when the background delivery
                 loop stopped permanently.
    """

    def __init__(self, client, filters=None, on_event=None):
        self._client = client
        self.id = None
        self.filters = filters or {}
        self.events = Queue()
        self.on_event = on_event
        self.error = None
        self._closed = False

    def _open_local(self):
        """Register a transport-level subscription (SSE / long-poll §6.1)."""
        self.id = "local-" + str(self._client._next_id())

    def _set_error(self, error):
        """Record a terminal delivery error (delivery has stopped)."""
        self.error = error

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