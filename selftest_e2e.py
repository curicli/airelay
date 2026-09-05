#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
airelay end-to-end self-test: spin up a set of deliberately misbehaving fake upstreams
plus a real proxy process, hit it with a real HTTP client, and prove the proxy really
does swallow the failures.

    python3 selftest_e2e.py            # run every case
    python3 selftest_e2e.py -v         # also print the proxy's internal event log

Needs to be able to listen on a local port. In a restricted sandbox (bind denied) this
script says so and exits; run selftest.py for the logic-level checks, which need no
socket at all.

Misbehaviour covered (all of it stuff real relays actually do):
  - HTTP 500 / 502 / 529
  - HTTP 200 + text/event-stream, then event: error halfway through the stream
  - HTTP 200, connection cut right after message_start (a truncated response)
  - the cut lands after half the content (gated cannot fix it, buffered can)
  - HTTP 200 + {"type":"error"}: a fake success
  - 401 (credentials) -> must move to the backup channel instead of retrying in place
  - 400 (the request itself is invalid) -> must be handed back verbatim, never retried
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import airelay  # noqa: E402

VERBOSE = "-v" in sys.argv or "--verbose" in sys.argv

# ---------------------------------------------------------------- SSE fixtures


def sse(event, data) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")


MSG_START = sse("message_start", {
    "type": "message_start",
    "message": {"id": "msg_test", "type": "message", "role": "assistant",
                "model": "claude-opus-5", "content": [], "stop_reason": None,
                "usage": {"input_tokens": 10, "output_tokens": 0}},
})
CB_START = sse("content_block_start",
               {"type": "content_block_start", "index": 0,
                "content_block": {"type": "text", "text": ""}})
CB_STOP = sse("content_block_stop", {"type": "content_block_stop", "index": 0})
MSG_DELTA = sse("message_delta", {"type": "message_delta",
                                  "delta": {"stop_reason": "end_turn"},
                                  "usage": {"output_tokens": 5}})
MSG_STOP = sse("message_stop", {"type": "message_stop"})
PING = sse("ping", {"type": "ping"})
ERR_OVERLOADED = sse("error", {"type": "error",
                               "error": {"type": "overloaded_error",
                                         "message": "upstream overloaded"}})

PIECES = ["Hello", " world", "!"]
EXPECTED_TEXT = "".join(PIECES)


def deltas() -> bytes:
    return b"".join(
        sse("content_block_delta",
            {"type": "content_block_delta", "index": 0,
             "delta": {"type": "text_delta", "text": p}})
        for p in PIECES
    )


def good_stream() -> bytes:
    return MSG_START + CB_START + PING + deltas() + CB_STOP + MSG_DELTA + MSG_STOP


GOOD_JSON = json.dumps({
    "id": "msg_test", "type": "message", "role": "assistant", "model": "claude-opus-5",
    "content": [{"type": "text", "text": EXPECTED_TEXT}],
    "stop_reason": "end_turn", "usage": {"input_tokens": 10, "output_tokens": 5},
}).encode("utf-8")


# ---------------------------------------------------------------- fake upstreams


class MockHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_a):
        pass

    def do_POST(self):
        mock = self.server.mock
        length = int(self.headers.get("Content-Length") or 0)
        _ = self.rfile.read(length) if length else b""
        scenario = self.headers.get("X-Scenario", "ok_stream")
        case = self.headers.get("X-Case", "default")
        n = mock.hit(case)

        if mock.force:
            scenario = mock.force

        if scenario == "ok_stream":
            self._stream(good_stream())
        elif scenario == "ok_json":
            self._json(200, GOOD_JSON)

        elif scenario == "fail2_then_ok":
            if n <= 2:
                self._json(500 if n == 1 else 529,
                           b'{"type":"error","error":{"type":"api_error","message":"Server error"}}')
            else:
                self._stream(good_stream())

        elif scenario == "sse_error_then_ok":
            if n <= 2:
                self._stream(MSG_START + CB_START + ERR_OVERLOADED)
            else:
                self._stream(good_stream())

        elif scenario == "truncate_before_text_then_ok":
            if n <= 2:
                self._stream(MSG_START + CB_START, truncate=True)
            else:
                self._stream(good_stream())

        elif scenario == "truncate_midtext_then_ok":
            if n <= 2:
                self._stream(MSG_START + CB_START
                             + sse("content_block_delta",
                                   {"type": "content_block_delta", "index": 0,
                                    "delta": {"type": "text_delta", "text": "Hel"}}),
                             truncate=True)
            else:
                self._stream(good_stream())

        elif scenario == "fake_200_error_then_ok":
            if n <= 1:
                self._json(200, b'{"type":"error","error":{"type":"overloaded_error",'
                                b'"message":"fake success"}}')
            else:
                self._json(200, GOOD_JSON)

        elif scenario == "always_401":
            self._json(401, b'{"type":"error","error":{"type":"authentication_error",'
                            b'"message":"bad key"}}')
        elif scenario == "always_500":
            self._json(500, b'{"type":"error","error":{"type":"api_error","message":"boom"}}')
        elif scenario == "bad_request":
            self._json(400, b'{"type":"error","error":{"type":"invalid_request_error",'
                            b'"message":"max_tokens is required"}}')
        elif scenario == "count_tokens":
            self._json(200, b'{"input_tokens":12}')
        else:
            self._json(500, b'{"type":"error","error":{"type":"api_error",'
                            b'"message":"unknown scenario"}}')

    def _json(self, status, body: bytes):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def _stream(self, payload: bytes, truncate: bool = False):
        """No Content-Length; the close is the end marker. Exactly the shape of real SSE."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        try:
            self.wfile.write(payload)
            self.wfile.flush()
        except OSError:
            return
        if truncate:
            try:
                self.connection.close()   # cut it, rudely
            except OSError:
                pass


class QuietServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass  # the fake upstreams cut connections on purpose; these errors are expected


class Mock:
    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.counters = {}
        self.total = 0
        self.force = None
        self.server = QuietServer(("127.0.0.1", 0), MockHandler)
        self.server.mock = self
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def hit(self, case) -> int:
        with self.lock:
            self.total += 1
            self.counters[case] = self.counters.get(case, 0) + 1
            return self.counters[case]

    def hits(self, case) -> int:
        with self.lock:
            return self.counters.get(case, 0)


# ---------------------------------------------------------------- start the proxy


def start_proxy(upstream_specs, **overrides) -> airelay.ProxyServer:
    raw = {
        "listen": {"host": "127.0.0.1", "port": 0},
        "upstreams": upstream_specs,
        "retry": {"max_attempts": 8, "initial_backoff": 0.05, "max_backoff": 0.2,
                  "backoff_multiplier": 2.0, "jitter": 0.3},
        "timeouts": {"connect": 5, "read": 15, "stream_idle": 5, "total": 45},
        "stream": {"mode": "gated", "keepalive_interval": 0.5,
                   "optimistic_commit_after": 0.4, "max_gate_wait": 30},
        "breaker": {"failure_threshold": 3, "cooldown_seconds": 0.5,
                    "max_cooldown_seconds": 2, "rotate_cooldown_seconds": 1},
        "log": {"file": None, "echo": VERBOSE},
    }
    for key, val in overrides.items():
        if isinstance(val, dict) and isinstance(raw.get(key), dict):
            raw[key].update(val)
        else:
            raw[key] = val
    cfg = airelay.Config(raw, source="<selftest>")
    log = airelay.Logger(None, echo=VERBOSE)
    srv = airelay.ProxyServer(cfg, log)
    threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.05},
                     daemon=True).start()
    srv.actual_port = srv.server_address[1]
    return srv


# ---------------------------------------------------------------- test client


def parse_events(data: bytes):
    events, buf = [], data
    while True:
        raw, buf = airelay.split_sse_event(buf)
        if raw is None:
            break
        events.append((airelay.sse_event_type(raw), airelay.sse_event_data(raw)))
    return events


def call(srv, scenario, case, *, stream=True, path="/v1/messages", timeout=60):
    payload = json.dumps({
        "model": "claude-opus-5", "max_tokens": 64, "stream": bool(stream),
        "messages": [{"role": "user", "content": "hi"}],
    }).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
        "x-api-key": "sk-ant-api-test",
        "X-Scenario": scenario,
        "X-Case": case,
    }
    conn = http.client.HTTPConnection("127.0.0.1", srv.actual_port, timeout=timeout)
    conn.request("POST", path, body=payload, headers=headers)
    resp = conn.getresponse()
    chunks = []
    while True:
        chunk = resp.read1(65536)
        if not chunk:
            break
        chunks.append(chunk)
    body = b"".join(chunks)
    status, headers_out = resp.status, dict((k.lower(), v) for k, v in resp.getheaders())
    conn.close()
    return status, headers_out, body


def text_of(events) -> str:
    out = []
    for etype, data in events:
        if etype == "content_block_delta":
            try:
                out.append(json.loads(data)["delta"].get("text", ""))
            except (ValueError, KeyError):
                pass
    return "".join(out)


# ---------------------------------------------------------------- assertion helpers

PASS, FAIL = [], []


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
        print(f"  ✅ {name}")
    else:
        FAIL.append((name, detail))
        print(f"  ❌ {name}\n       {detail}")


def section(title):
    print(f"\n{title}")


# ---------------------------------------------------------------- cases


def can_bind() -> str:
    """Can we listen on a local port? A restricted sandbox answers bind with EPERM."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("127.0.0.1", 0))
        finally:
            s.close()
        return ""
    except OSError as exc:
        return str(exc)


def main():
    why = can_bind()
    if why:
        print("skipping the end-to-end test: this environment does not allow listening on a local port (%s)." % why)
        print("usually a sandbox policy. For the full logic-level check run: python3 selftest.py")
        print("run this script again from your own (non-sandboxed) terminal to finish end-to-end verification.")
        return 0

    flaky = Mock("flaky")
    solid = Mock("solid")

    one = [{"name": "flaky", "base_url": f"http://127.0.0.1:{flaky.port}", "auth": "passthrough"}]
    two = [
        {"name": "flaky", "base_url": f"http://127.0.0.1:{flaky.port}", "auth": "passthrough"},
        {"name": "solid", "base_url": f"http://127.0.0.1:{solid.port}", "auth": "sk-ant-api-backup"},
    ]

    gated = start_proxy(one)
    buffered = start_proxy(one, stream={"mode": "buffered"})
    rotating = start_proxy(two)
    impatient = start_proxy(one,
                            retry={"max_attempts": 3, "initial_backoff": 0.4},
                            stream={"optimistic_commit_after": 0.05, "keepalive_interval": 0.1})
    time.sleep(0.3)

    # ---- 1. baseline
    section("1. Baseline: no side effects at all when the upstream is fine")
    status, hdrs, body = call(gated, "ok_stream", "c1")
    events = parse_events(body)
    check("a streaming request passes straight through", status == 200 and text_of(events) == EXPECTED_TEXT,
          f"status={status} text={text_of(events)!r}")
    check("exactly one message_start and one message_stop",
          [e for e, _ in events].count("message_start") == 1
          and [e for e, _ in events].count("message_stop") == 1,
          str([e for e, _ in events]))
    status, hdrs, body = call(gated, "ok_json", "c1b", stream=False)
    check("a non-streaming request passes straight through",
          status == 200 and json.loads(body)["content"][0]["text"] == EXPECTED_TEXT,
          f"status={status} body={body[:200]!r}")

    # ---- 2. status-code failures
    section("2. HTTP 500 / 529 -- the Server error you actually see")
    status, hdrs, body = call(gated, "fail2_then_ok", "c2")
    events = parse_events(body)
    errs = [e for e, _ in events if e == "error"]
    check("succeeds after retrying; the client gets the whole body",
          status == 200 and text_of(events) == EXPECTED_TEXT,
          f"status={status} text={text_of(events)!r}")
    check("the client saw no error events at all", not errs, str(errs))
    check("it really did retry 3 times (upstream was hit 3 times)", flaky.hits("c2") == 3, f"hits={flaky.hits('c2')}")
    check("response headers name the channel used and the attempt count",
          hdrs.get("x-airelay-attempt") == "3" and hdrs.get("x-airelay-upstream") == "flaky",
          str({k: v for k, v in hdrs.items() if k.startswith("x-airelay")}))

    # ---- 3. 200 + event: error
    section("3. HTTP 200 but event: error in the stream (the nastiest relay trick)")
    status, hdrs, body = call(gated, "sse_error_then_ok", "c3")
    events = parse_events(body)
    check("silently retried, body complete",
          status == 200 and text_of(events) == EXPECTED_TEXT and
          not [e for e, _ in events if e == "error"],
          f"events={[e for e, _ in events]} text={text_of(events)!r}")
    check("only the message_start of the successful attempt was released",
          [e for e, _ in events].count("message_start") == 1,
          str([e for e, _ in events]))

    # ---- 4. cut before any content
    section("4. Connection cut before content appears (a truncated response)")
    status, hdrs, body = call(gated, "truncate_before_text_then_ok", "c4")
    events = parse_events(body)
    check("the gate held the half response back; body complete after a silent retry",
          status == 200 and text_of(events) == EXPECTED_TEXT and
          not [e for e, _ in events if e == "error"],
          f"events={[e for e, _ in events]} text={text_of(events)!r}")

    # ---- 5. cut mid-content: gated cannot fix it, buffered can
    section("5. The cut lands after half the content -- where gated and buffered part ways")
    before = gated.stats.snapshot()["unrecoverable_midstream"]
    status, hdrs, body = call(gated, "truncate_midtext_then_ok", "c5a")
    events = parse_events(body)
    after = gated.stats.snapshot()["unrecoverable_midstream"]
    err_msgs = [d for e, d in events if e == "error"]
    check("gated: tells the client the truth instead of faking success",
          after == before + 1 and err_msgs and "airelay" in err_msgs[0],
          f"unrecoverable {before}->{after} errs={err_msgs}")
    check("gated: content already released is never sent twice",
          text_of(events) == "Hel", f"text={text_of(events)!r}")

    status, hdrs, body = call(buffered, "truncate_midtext_then_ok", "c5b")
    events = parse_events(body)
    check("buffered: the same failure is fully recovered, invisible to the client",
          status == 200 and text_of(events) == EXPECTED_TEXT and
          not [e for e, _ in events if e == "error"],
          f"events={[e for e, _ in events]} text={text_of(events)!r}")

    # ---- 6. fake success
    section("6. HTTP 200 + {\"type\":\"error\"}: a fake success")
    status, hdrs, body = call(gated, "fake_200_error_then_ok", "c6", stream=False)
    check("recognised as a fake success and retried",
          status == 200 and json.loads(body).get("type") == "message",
          f"status={status} body={body[:200]!r}")

    # ---- 7. channel rotation
    section("7. 401 credentials -- rotate, do not retry in place")
    solid.force = "ok_stream"
    status, hdrs, body = call(rotating, "always_401", "c7")
    events = parse_events(body)
    check("moved to the backup channel and succeeded",
          status == 200 and text_of(events) == EXPECTED_TEXT
          and hdrs.get("x-airelay-upstream") == "solid",
          f"status={status} upstream={hdrs.get('x-airelay-upstream')} text={text_of(events)!r}")
    check("the broken channel was cooled down by the breaker",
          not rotating.pool.upstreams[0].available(),
          str(rotating.pool.upstreams[0].snapshot()))
    solid.force = None

    # ---- 8. what must not be retried
    section("8. 400 invalid request -- no retry, handed back verbatim")
    status, hdrs, body = call(gated, "bad_request", "c8", stream=False)
    check("400 passes through unchanged", status == 400 and b"max_tokens is required" in body,
          f"status={status} body={body[:200]!r}")
    check("upstream hit once, no pointless retries", flaky.hits("c8") == 1, f"hits={flaky.hits('c8')}")

    # ---- 9. how it ends when everything really is down
    section("9. Upstream down the whole time -- a clean ending once retries run out")
    status, hdrs, body = call(impatient, "always_500", "c9")
    events = parse_events(body)
    err_msgs = [d for e, d in events if e == "error"]
    check("headers committed early, connection held with pings, then an error event",
          status == 200 and err_msgs and "airelay" in err_msgs[0],
          f"status={status} events={[e for e, _ in events]}")
    check("retries are bounded by max_attempts", flaky.hits("c9") == 3, f"hits={flaky.hits('c9')}")
    check("keepalive pings were sent while backing off",
          any(e == "ping" for e, _ in events), str([e for e, _ in events]))

    # ---- 10. other endpoints and the admin surface
    section("10. Non-messages endpoints and the admin endpoints")
    status, hdrs, body = call(gated, "count_tokens", "c10", stream=False,
                              path="/v1/messages/count_tokens")
    check("count_tokens passes through", status == 200 and json.loads(body)["input_tokens"] == 12,
          f"status={status} body={body[:120]!r}")

    conn = http.client.HTTPConnection("127.0.0.1", gated.actual_port, timeout=5)
    conn.request("GET", "/__airelay/stats")
    resp = conn.getresponse()
    stats = json.loads(resp.read())
    conn.close()
    check("/__airelay/stats is readable and counted the silent recoveries",
          stats["totals"]["invisible_recoveries"] >= 4,
          json.dumps(stats["totals"], ensure_ascii=False))

    # ---- summary
    print("\n" + "=" * 68)
    print(f"passed {len(PASS)}, failed {len(FAIL)}")
    if FAIL:
        print("\nfailures:")
        for name, detail in FAIL:
            print(f"  - {name}\n      {detail}")
    print("=" * 68)

    g = gated.stats.snapshot()
    print(f"\ngated proxy totals: requests {g['requests']}  retries {g['retries']}  "
          f"silently recovered {g['invisible_recoveries']}  unrecoverable {g['unrecoverable_midstream']}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
