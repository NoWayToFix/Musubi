#!/usr/bin/env python3
"""Self-contained logic tests for the Python SDK (no network, no engine).

Exercises the pure-logic paths with fakes:

- WS availability ordering (must not depend on the resolved transport),
- WS resend classification (connect/send-phase vs after-the-frame-was-written),
- poll cursor reset on reconnect + terminal auth stop (worker loop),
- offline queue: bounding, FIFO drain, mid-drain re-queue, auto-queue on
  connect-phase failures,
- SSE frame parsing and dispatch,
- auth.refresh rotating both tokens + retry-on-rotated-token.

Run:  python3 -m unittest sdk/python/tests/test_client_logic.py -v
"""

import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import hub.client as hub_client  # noqa: E402
from hub import HubClient, HubError, Subscription  # noqa: E402
from hub.client import _Dedup, _WsNotSent  # noqa: E402
from hub.errors import ERROR_AUTH_EXPIRED, ERROR_AUTH_REQUIRED  # noqa: E402


class fake_websocket(object):
    """Context manager forcing `import websocket` to succeed (or fail)."""

    def __init__(self, available=True):
        self._available = available
        self._saved = None

    def __enter__(self):
        self._saved = sys.modules.get("websocket")
        sys.modules["websocket"] = (types.ModuleType("websocket")
                                    if self._available else None)
        return self

    def __exit__(self, *exc):
        if self._saved is None:
            sys.modules.pop("websocket", None)
        else:
            sys.modules["websocket"] = self._saved
        return False


def make_client(**kw):
    kw.setdefault("transport", "http")  # forced: _resolve_transport is a no-op
    return HubClient(**kw)


class WsAvailabilityTest(unittest.TestCase):
    """B4: WS must be selectable; availability is not transport-dependent."""

    def test_available_independent_of_resolved_transport(self):
        c = make_client()
        c._transport = "http"  # the old bug: _ws_available() was False here
        with fake_websocket(available=True):
            self.assertTrue(c._ws_available())

    def test_unavailable_without_extra(self):
        c = make_client()
        c._transport = "http"
        with fake_websocket(available=False):
            self.assertFalse(c._ws_available())

    def _negotiate(self):
        """engine.info advertising ws, via a stubbed HTTP POST."""
        c = make_client(transport="auto")
        c._http_post = lambda payload: {
            "result": {"transports": ["ws", "http", "sse", "poll"]}}
        c._resolve_transport()
        return c._transport

    def test_negotiation_picks_ws_when_importable(self):
        with fake_websocket(available=True):
            self.assertEqual(self._negotiate(), "ws")

    def test_negotiation_degrades_to_http_without_extra(self):
        with fake_websocket(available=False):
            self.assertEqual(self._negotiate(), "http")


class ResendClassificationTest(unittest.TestCase):
    """M3: fall back to HTTP only when the frame provably never went out."""

    def _ws_client(self):
        c = make_client()
        c._transport = "ws"
        c._ws_available = lambda: True
        return c

    def test_connect_phase_failure_falls_back_to_http(self):
        c = self._ws_client()
        http_calls = []
        c._call_ws = lambda req: (_ for _ in ()).throw(_WsNotSent("refused"))
        c._call_http = lambda req: (http_calls.append(req) or {"ok": True})
        self.assertEqual(c.call("engine.ping"), {"ok": True})
        self.assertEqual(len(http_calls), 1)

    def test_send_phase_failure_falls_back_to_http(self):
        c = self._ws_client()
        http_calls = []
        def boom(req):
            raise _WsNotSent("send failed")
        c._call_ws = boom
        c._call_http = lambda req: (http_calls.append(req) or {"ok": True})
        self.assertEqual(c.call("engine.ping"), {"ok": True})
        self.assertEqual(len(http_calls), 1)

    def test_post_send_failure_is_never_re_sent(self):
        c = self._ws_client()
        http_calls = []
        def boom(req):
            raise Exception("connection reset during recv")
        c._call_ws = boom
        c._call_http = lambda req: http_calls.append(req)
        with self.assertRaises(HubError) as cm:
            c.call("command.send", {"node_id": "n-1"})
        self.assertEqual(cm.exception.data.get("phase"), "post-send")
        self.assertEqual(http_calls, [])  # no duplicate execution

    def test_ws_jsonrpc_error_is_authoritative(self):
        c = self._ws_client()
        http_calls = []
        def rpc_error(req):
            raise HubError(-32011, "node.offline")
        c._call_ws = rpc_error
        c._call_http = lambda req: http_calls.append(req)
        with self.assertRaises(HubError) as cm:
            c.call("command.send", {"node_id": "n-1"})
        self.assertEqual(cm.exception.code, -32011)
        self.assertEqual(http_calls, [])

    def test_resendable_classification(self):
        self.assertTrue(HubClient._resendable(
            HubError(0, "refused", {"phase": "connect"})))
        self.assertFalse(HubClient._resendable(
            HubError(0, "bad body", {"phase": "post-send"})))
        self.assertFalse(HubClient._resendable(
            HubError(0, "no phase")))
        self.assertFalse(HubClient._resendable(
            HubError(-32001, "auth.required")))


class PollWorkerTest(unittest.TestCase):
    """E4 + M1: cursor reset on reconnect, resilient loop, terminal auth."""

    def _worker_client(self):
        c = make_client()
        c._backoff = lambda retries: True  # no sleeping in tests
        c._poll_loop = True
        return c

    def test_cursor_reset_on_reconnect_then_advance(self):
        c = self._worker_client()
        c._poll_cursor = 100  # stale cursor from a previous engine instance
        seen_cursors = []

        def poll_once(cursor):
            seen_cursors.append(cursor)
            if len(seen_cursors) == 1:
                raise HubError(0, "connection refused", {"phase": "connect"})
            if len(seen_cursors) == 2:
                raise HubError(0, "still down")
            c._poll_loop = False  # end the loop after this success
            return [], 5

        c._poll_once = poll_once
        c._poll_worker()
        # Failure reset the stale cursor to 0 before each retry; success
        # advanced it to the server cursor.
        self.assertEqual(seen_cursors, [100, 0, 0])
        self.assertEqual(c._poll_cursor, 5)
        self.assertIsNone(c.poll_error)

    def test_auth_error_stops_loop_terminally(self):
        c = self._worker_client()
        calls = []

        def poll_once(cursor):
            calls.append(cursor)
            raise HubError(ERROR_AUTH_REQUIRED, "auth.required")

        c._poll_once = poll_once
        c._poll_worker()
        self.assertEqual(len(calls), 1)  # no spinning
        self.assertIsNotNone(c.poll_error)
        self.assertTrue(c.poll_error.is_auth())

    def test_max_retries_exhaustion_is_observable(self):
        c = self._worker_client()
        c._backoff = lambda retries: False  # budget exhausted immediately
        c._poll_once = lambda cursor: (_ for _ in ()).throw(
            HubError(0, "down", {"phase": "connect"}))
        c._poll_worker()
        self.assertIsNotNone(c.poll_error)
        self.assertIn("gave up", c.poll_error.message)

    def test_stop_event_delivery_marks_subscriptions(self):
        c = make_client()
        sub = Subscription(c, {"event_types": ["node.updated"]})
        sub._open_local()
        c._subs[sub.id] = sub
        err = HubError(ERROR_AUTH_REQUIRED, "auth.required")
        c._stop_event_delivery(err)
        self.assertIs(c.poll_error, err)
        self.assertIs(sub.error, err)


class OfflineQueueTest(unittest.TestCase):
    """H5: bounded FIFO offline queue, drained in order on reconnect."""

    def setUp(self):
        self._old_limit = hub_client.OFFLINE_QUEUE_LIMIT
        hub_client.OFFLINE_QUEUE_LIMIT = 3

    def tearDown(self):
        hub_client.OFFLINE_QUEUE_LIMIT = self._old_limit

    def test_queue_write_is_bounded(self):
        c = make_client()
        for _ in range(3):
            self.assertTrue(c.queue_write({"method": "command.send", "params": {}}))
        self.assertFalse(c.queue_write({"method": "command.send", "params": {}}))
        self.assertEqual(len(c._offline), 3)

    def test_drain_replays_in_order(self):
        c = make_client()
        sent = []
        c._send_with_retry = lambda req, method: sent.append(method)
        c.queue_write({"method": "command.send", "params": {"n": 1}})
        c.queue_write({"method": "task.create", "params": {"n": 2}})
        c.queue_write({"method": "node.list", "params": {}})
        c._maybe_drain_offline()
        self.assertEqual(sent, ["command.send", "task.create", "node.list"])
        self.assertEqual(c._offline, [])

    def test_drain_requeues_remainder_on_connect_failure(self):
        c = make_client()
        sent = []

        def send(req, method):
            if method == "task.create":
                raise HubError(0, "down again", {"phase": "connect"})
            sent.append(method)

        c._send_with_retry = send
        c.queue_write({"method": "command.send", "params": {}})
        c.queue_write({"method": "task.create", "params": {}})
        c.queue_write({"method": "node.list", "params": {}})
        c._maybe_drain_offline()
        # The failed item and everything after it stay queued, in order.
        self.assertEqual(sent, ["command.send"])
        self.assertEqual([i["method"] for i in c._offline],
                         ["task.create", "node.list"])

    def test_drain_drops_authoritative_errors_and_continues(self):
        c = make_client()
        sent = []

        def send(req, method):
            sent.append(method)
            if method == "command.send":
                raise HubError(-32011, "node.offline")  # server saw it

        c._send_with_retry = send
        c.queue_write({"method": "command.send", "params": {}})
        c.queue_write({"method": "task.create", "params": {}})
        c._maybe_drain_offline()
        self.assertEqual(sent, ["command.send", "task.create"])
        self.assertEqual(c._offline, [])

    def test_connect_phase_failure_queues_write(self):
        c = make_client()
        def down(req):
            raise HubError(0, "connection refused", {"phase": "connect"})
        c._send_once = down
        with self.assertRaises(HubError) as cm:
            c.call("command.send", {"node_id": "n-1"})
        self.assertTrue(cm.exception.data.get("queued"))
        self.assertEqual(c._offline,
                         [{"method": "command.send", "params": {"node_id": "n-1"}}])

    def test_engine_and_auth_calls_never_queue(self):
        c = make_client()
        def down(req):
            raise HubError(0, "connection refused", {"phase": "connect"})
        c._send_once = down
        for method in ("engine.ping", "auth.login", "event.subscribe"):
            with self.assertRaises(HubError) as cm:
                c.call(method, {})
            self.assertFalse(cm.exception.data.get("queued"))
        self.assertEqual(c._offline, [])

    def test_queue_disabled_surfaces_transport_error(self):
        c = make_client(queue_offline=False)
        def down(req):
            raise HubError(0, "connection refused", {"phase": "connect"})
        c._send_once = down
        with self.assertRaises(HubError) as cm:
            c.call("command.send", {"node_id": "n-1"})
        self.assertFalse(cm.exception.data.get("queued"))
        self.assertEqual(c._offline, [])


class SseTest(unittest.TestCase):
    """H2: SSE frame parsing and client-side dispatch."""

    FEED = [
        ": heartbeat\n",               # comment -> ignored
        "\n",
        'data: {"jsonrpc":"2.0","method":"event","params":{"subscription_id":'
        '"srv","event":{"id":"e1","type":"node.updated","node_id":"n-1"}}}\n',
        "\n",
        'data: {"jsonrpc":"2.0","method":"other"}\n',  # unknown -> ignored
        "\n",
        "data: not-json\n",            # malformed -> ignored
        "\n",
        "",                            # clean EOF
    ]

    class FakeBody(object):
        def __init__(self, lines):
            self._lines = [l.encode("utf-8") for l in lines]
            self.closed = False

        def readline(self):
            return self._lines.pop(0) if self._lines else b""

        def close(self):
            self.closed = True

    def test_parse_event(self):
        good = ('{"jsonrpc":"2.0","method":"event","params":'
                '{"subscription_id":"s","event":{"id":"e1","type":"t"}}}')
        ev = HubClient._sse_parse_event(good)
        self.assertEqual(ev["id"], "e1")
        self.assertIsNone(HubClient._sse_parse_event('{"method":"other"}'))
        self.assertIsNone(HubClient._sse_parse_event("not-json"))

    def test_read_dispatches_and_ignores_heartbeats(self):
        c = make_client()
        c._sse_loop_on = True
        sub = Subscription(c, {"event_types": ["node.updated"]})
        sub._open_local()
        c._subs[sub.id] = sub
        body = self.FakeBody(self.FEED)
        error = c._sse_read(body, _Dedup(100))
        self.assertIsNone(error)  # clean EOF
        self.assertTrue(body.closed is False)  # close() is the worker's job
        ev = sub.events.get(timeout=1)
        self.assertEqual(ev["id"], "e1")
        self.assertEqual(ev["type"], "node.updated")
        # exactly one event was delivered (heartbeats/unknown filtered out)
        with self.assertRaises(Exception):
            sub.events.get(timeout=0.05)

    def test_sse_available_requires_negotiated_server(self):
        c = make_client()
        self.assertFalse(c._sse_available())  # nothing negotiated yet
        c._server = {"transports": ["http", "sse"], "features": {}}
        self.assertTrue(c._sse_available())
        c._server = {"transports": ["http"], "features": {"sse": True}}
        self.assertTrue(c._sse_available())
        c._server = {"transports": ["http"], "features": {"sse": False}}
        self.assertFalse(c._sse_available())


class AuthRotationTest(unittest.TestCase):
    """Refresh rotation: both tokens updated; rotated-away token retried."""

    def test_refresh_rotates_both_tokens(self):
        c = make_client()
        c.token = "at1"
        c._refresh_token = "rt1"
        seen = []

        def fake_http(req):
            seen.append(req)
            if req["method"] == "auth.refresh":
                self.assertEqual(req["params"], {"refresh_token": "rt1"})
                return {"access_token": "at2", "refresh_token": "rt2"}
            raise AssertionError("unexpected %s" % req["method"])

        c._call_http = fake_http
        res = c.refresh()
        self.assertEqual(res["access_token"], "at2")
        self.assertEqual(c.token, "at2")
        self.assertEqual(c._refresh_token, "rt2")

    def test_refresh_without_token_raises(self):
        c = make_client()
        with self.assertRaises(HubError) as cm:
            c.refresh()
        self.assertEqual(cm.exception.code, ERROR_AUTH_REQUIRED)

    def test_call_retries_once_after_rotation(self):
        c = make_client()
        c.token = "at1"
        c._refresh_token = "rt1"
        sends = []

        def fake_http(req):
            sends.append(req["method"])
            if req["method"] == "auth.refresh":
                return {"access_token": "at2", "refresh_token": "rt2"}
            # First command.send runs with the just-rotated-away token.
            if len([m for m in sends if m == "command.send"]) == 1:
                self.assertEqual(c.token, "at1")
                raise HubError(ERROR_AUTH_EXPIRED, "auth.expired")
            return {"command": {"id": "c1"}}

        c._call_http = fake_http
        res = c.call("command.send", {"node_id": "n-1"})
        self.assertEqual(res, {"command": {"id": "c1"}})
        self.assertEqual(sends, ["command.send", "auth.refresh", "command.send"])
        self.assertEqual(c.token, "at2")
        self.assertEqual(c._refresh_token, "rt2")

    def test_call_fails_hard_when_refresh_changes_nothing(self):
        c = make_client()
        c.token = "at1"
        c._refresh_token = "rt1"

        def fake_http(req):
            if req["method"] == "auth.refresh":
                return {}  # rotation produced nothing new
            raise HubError(ERROR_AUTH_EXPIRED, "auth.expired")

        c._call_http = fake_http
        with self.assertRaises(HubError) as cm:
            c.call("command.send", {"node_id": "n-1"})
        self.assertEqual(cm.exception.code, ERROR_AUTH_EXPIRED)

    def test_auth_methods_never_recurse_into_refresh(self):
        c = make_client()
        c.token = "at1"
        c._refresh_token = "rt1"
        calls = []

        def fake_http(req):
            calls.append(req["method"])
            raise HubError(ERROR_AUTH_EXPIRED, "auth.expired")

        c._call_http = fake_http
        with self.assertRaises(HubError):
            c.call("auth.refresh", {"refresh_token": "rt1"})
        self.assertEqual(calls, ["auth.refresh"])  # exactly one attempt


class SubscriptionLocalTest(unittest.TestCase):
    def test_local_subscription_needs_no_rpc(self):
        c = make_client()
        sub = Subscription(c, {"event_types": ["node.updated"]})
        sub._open_local()
        self.assertTrue(sub.id.startswith("local-"))
        # _unsubscribe for local ids must not attempt the RPC.
        c.call = lambda *a, **kw: (_ for _ in ()).throw(
            AssertionError("no RPC for local subscriptions"))
        c._subs[sub.id] = sub
        c._unsubscribe(sub.id)
        self.assertEqual(c._subs, {})


if __name__ == "__main__":
    unittest.main()
