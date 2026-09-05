#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
airelay logic self-test — no network, no listening socket, all in-process.

How it works: Upstream.connect is swapped for a fake http.client connection that
emits scripted bad responses. On the client side a fake BaseHTTPRequestHandler
catches the real chunked output, which is then parsed back into SSE events so we can

assert on what the client actually saw. That is the thing that matters: under every
way an upstream can break, did the client see an error, did it get the whole body,

    python3 selftest.py         # run
    python3 selftest.py -v      # also print airelay's own log

End-to-end tests (real sockets, real HTTP parsing) live in selftest_e2e.py.
"""

import io
import http.client
import json
import os
import socket
import sys
import threading
import time
import itertools

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import airelay  # noqa: E402

VERBOSE = "-v" in sys.argv or "--verbose" in sys.argv
FAILS = []
CHECKS = 0
_CASE_SEQ = itertools.count(1)


# ---------------------------------------------------------------- SSE fixtures


def ev(name: str, obj: dict) -> bytes:
    return ("event: %s\ndata: %s\n\n" % (name, json.dumps(obj, ensure_ascii=False))).encode("utf-8")


E_START = ev("message_start", {"type": "message_start", "message": {
    "id": "msg_test", "type": "message", "role": "assistant", "model": "claude-opus-5",
    "content": [], "stop_reason": None, "usage": {"input_tokens": 9, "output_tokens": 1}}})
E_CBS = ev("content_block_start", {"type": "content_block_start", "index": 0,
                                   "content_block": {"type": "text", "text": ""}})
E_PING = ev("ping", {"type": "ping"})
E_CB_STOP = ev("content_block_stop", {"type": "content_block_stop", "index": 0})
E_M_DELTA = ev("message_delta", {"type": "message_delta",
                                 "delta": {"stop_reason": "end_turn"},
                                 "usage": {"output_tokens": 3}})
E_M_STOP = ev("message_stop", {"type": "message_stop"})


def E_TEXT(t: str) -> bytes:
    return ev("content_block_delta", {"type": "content_block_delta", "index": 0,
                                      "delta": {"type": "text_delta", "text": t}})


def E_ERROR(etype: str, msg: str) -> bytes:
    return ev("error", {"type": "error", "error": {"type": etype, "message": msg}})


PARTS = ["Hel", "lo ", "world"]
FULL_TEXT = "".join(PARTS)
GOOD_STREAM = [E_START, E_PING, E_CBS] + [E_TEXT(p) for p in PARTS] + [E_CB_STOP, E_M_DELTA, E_M_STOP]

SSE_HEADERS = [("Content-Type", "text/event-stream; charset=utf-8"),
               ("Cache-Control", "no-cache")]
JSON_HEADERS = [("Content-Type", "application/json")]

MSG_JSON = json.dumps({
    "id": "msg_test", "type": "message", "role": "assistant", "model": "claude-opus-5",
    "content": [{"type": "text", "text": FULL_TEXT}],
    "stop_reason": "end_turn", "usage": {"input_tokens": 9, "output_tokens": 3},
}, ensure_ascii=False).encode("utf-8")


def err_json(etype: str, msg: str) -> bytes:
    return json.dumps({"type": "error", "error": {"type": etype, "message": msg}},
                      ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------- fake upstream


class FakeSock:
    def __init__(self):
        self.timeout = None

    def settimeout(self, t):
        self.timeout = t

    def close(self):
        pass


class FakeResponse:
    """A good-enough stand-in for http.client.HTTPResponse."""

    def __init__(self, status, headers, chunks, raise_at_end=None):
        self.status = status
        self.reason = ""
        self._headers = list(headers)
        self._chunks = [c for c in chunks if c]
        self._raise_at_end = raise_at_end
        self._raised = False

    def getheaders(self):
        return list(self._headers)

    def getheader(self, name, default=None):
        for k, v in self._headers:
            if k.lower() == name.lower():
                return v
        return default

    def read1(self, n=-1):
        if self._chunks:
            return self._chunks.pop(0)
        if self._raise_at_end is not None and not self._raised:
            self._raised = True
            raise self._raise_at_end
        return b""

    def read(self, n=-1):
        out = b""
        while True:
            c = self.read1()
            if not c:
                return out
            out += c

    def close(self):
        pass


def sse_resp(chunks, raise_at_end=None, headers=None):
    return FakeResponse(200, headers or SSE_HEADERS, chunks, raise_at_end)


def json_resp(status, body, headers=None, extra=()):
    return FakeResponse(status, list(headers or JSON_HEADERS) + list(extra), [body])


# scenario(n) -> FakeResponse. n counts this case's hits on this channel from 1.
SCENARIOS = {}


def scenario(name):
    def deco(fn):
        SCENARIOS[name] = fn
        return fn
    return deco


@scenario("ok_stream")
def _ok_stream(n):
    return sse_resp(GOOD_STREAM)


@scenario("ok_json")
def _ok_json(n):
    return json_resp(200, MSG_JSON)


@scenario("fail2_then_ok")
def _fail2(n):
    if n == 1:
        return json_resp(500, err_json("api_error", "Server error"))
    if n == 2:
        return json_resp(529, err_json("overloaded_error", "Overloaded"))
    return sse_resp(GOOD_STREAM)


@scenario("cf_520_then_ok")
def _cf520(n):
    if n == 1:
        return json_resp(520, b"<html>Cloudflare: Web server returned an unknown error</html>",
                         headers=[("Content-Type", "text/html")])
    return sse_resp(GOOD_STREAM)


@scenario("sse_error_then_ok")
def _sse_err(n):
    if n == 1:
        return sse_resp([E_START, E_CBS, E_ERROR("overloaded_error", "Overloaded")])
    return sse_resp(GOOD_STREAM)


@scenario("truncate_pre")
def _trunc_pre(n):
    """Dies before any content (behind the gate), so it can be retried silently."""
    if n == 1:
        return sse_resp([E_START, E_PING, E_CBS])
    return sse_resp(GOOD_STREAM)


@scenario("truncate_mid")
def _trunc_mid(n):
    """Dies after some content is out — gated cannot save this, buffered can."""
    if n == 1:
        return sse_resp([E_START, E_CBS, E_TEXT("Hel")],
                        raise_at_end=ConnectionResetError("upstream reset"))
    return sse_resp(GOOD_STREAM)


@scenario("fake200_then_ok")
def _fake200(n):
    if n == 1:
        return json_resp(200, err_json("overloaded_error", "Overloaded, please retry"))
    return json_resp(200, MSG_JSON)


@scenario("empty200_then_ok")
def _empty200(n):
    if n == 1:
        return json_resp(200, b"")
    return json_resp(200, MSG_JSON)


@scenario("net_flap_then_ok")
def _net_flap(n):
    if n == 1:
        raise ConnectionResetError("connection reset by peer")
    if n == 2:
        raise socket.timeout("timed out")
    return sse_resp(GOOD_STREAM)


@scenario("rate_limited_then_ok")
def _rl(n):
    if n == 1:
        return json_resp(429, err_json("rate_limit_error", "Too many requests"),
                         extra=[("retry-after", "0.4")])
    return sse_resp(GOOD_STREAM)


@scenario("always_401")
def _a401(n):
    return json_resp(401, err_json("authentication_error", "invalid x-api-key"))


@scenario("always_500")
def _a500(n):
    return json_resp(500, err_json("api_error", "Server error"))


@scenario("bad_request")
def _bad(n):
    return json_resp(400, err_json("invalid_request_error", "max_tokens: must be positive"))


@scenario("count_tokens")
def _ct(n):
    return json_resp(200, b'{"input_tokens": 12}')


# --- model-aware scenarios: these answer based on which model the request asked for ---


def _sent_model(row=None):
    """Which model went out in the last upstream request (scenarios are single-threaded)."""
    row = row if row is not None else BACKEND.last()
    if not row:
        return None
    try:
        return (json.loads(row[4]) or {}).get("model")
    except (ValueError, TypeError):
        return None


@scenario("no_opus")
def _no_opus(n):
    """The channel is perfectly healthy, it just does not carry opus — the classic 404."""
    model = _sent_model() or ""
    if "opus" in model:
        return json_resp(404, err_json("not_found_error", f"model: {model} not found"))
    return sse_resp(GOOD_STREAM)


@scenario("no_model_at_all")
def _no_model(n):
    """Claims no model exists at all — used to walk a fallback chain to its end."""
    model = _sent_model() or ""
    return json_resp(404, err_json("not_found_error", f"model {model} does not exist"))


@scenario("opus_overloaded")
def _opus_overloaded(n):
    """opus overloaded site-wide (the channel itself is fine): only a model switch helps."""
    if "opus" in (_sent_model() or ""):
        return json_resp(529, err_json("overloaded_error", "Overloaded"))
    return sse_resp(GOOD_STREAM)


@scenario("mid_rejected")
def _mid_rejected(n):
    """No opus, and the middle rung of the chain is rejected with a 400."""
    model = _sent_model() or ""
    if "opus" in model:
        return json_resp(404, err_json("not_found_error", f"model {model} not found"))
    if "mid" in model:
        return json_resp(400, err_json("invalid_request_error",
                                       "thinking.budget_tokens: unsupported here"))
    return sse_resp(GOOD_STREAM)


class Backend:
    def __init__(self):
        self.lock = threading.Lock()
        self.hits = {}       # (case, upstream) -> count
        self.requests = []   # (upstream, method, url, headers, body)
        self.pin = {}        # upstream -> forced scenario (to build a healthy spare)

    def reset(self):
        with self.lock:
            self.hits.clear()
            self.requests.clear()
            self.pin.clear()

    def hit(self, case, up):
        with self.lock:
            key = (case, up)
            self.hits[key] = self.hits.get(key, 0) + 1
            return self.hits[key]

    def count(self, case, up=None):
        with self.lock:
            if up is not None:
                return self.hits.get((case, up), 0)
            return sum(v for (c, _), v in self.hits.items() if c == case)

    def record(self, *row):
        with self.lock:
            self.requests.append(row)

    def last(self, up=None):
        with self.lock:
            for row in reversed(self.requests):
                if up is None or row[0] == up:
                    return row
        return None


BACKEND = Backend()


class FakeConn:
    def __init__(self, up_name):
        self.up = up_name
        self.sock = FakeSock()
        self._maker = None
        self._n = 0

    def request(self, method, url, body=None, headers=None):
        headers = headers or {}
        low = {k.lower(): v for k, v in headers.items()}
        case = low.get("x-case", "default")
        scen = BACKEND.pin.get(self.up) or low.get("x-scenario", "ok_stream")
        self._n = BACKEND.hit(case, self.up)
        BACKEND.record(self.up, method, url, dict(headers), body)
        self._maker = SCENARIOS[scen]

    def getresponse(self):
        return self._maker(self._n)

    def close(self):
        pass


def fake_connect(self, cfg):
    return FakeConn(self.name)


airelay.Upstream.connect = fake_connect


# ---------------------------------------------------------------- fake client


class BreakingBytesIO(io.BytesIO):
    """Breaks the pipe on write number N, to simulate a client closing its window."""

    def __init__(self, break_after=None):
        super().__init__()
        self.break_after = break_after
        self.writes = 0

    def write(self, data):
        self.writes += 1
        if self.break_after is not None and self.writes > self.break_after:
            raise BrokenPipeError("client closed connection")
        return super().write(data)


class FakeHandler:
    """The handful of BaseHTTPRequestHandler methods ClientWriter actually uses."""

    def __init__(self, break_after=None):
        self.status = None
        self.headers_out = []
        self.headers_done = False
        self.wfile = BreakingBytesIO(break_after)

    def send_response(self, status, message=None):
        self.status = status

    def send_header(self, key, val):
        self.headers_out.append((key, str(val)))

    def end_headers(self):
        self.headers_done = True


def dechunk(data: bytes) -> bytes:
    out, i = b"", 0
    while True:
        j = data.find(b"\r\n", i)
        if j < 0:
            break
        try:
            size = int(data[i:j].split(b";")[0], 16)
        except ValueError:
            break
        if size == 0:
            break
        out += data[j + 2:j + 2 + size]
        i = j + 2 + size + 2
    return out


class Result:
    def __init__(self, handler: FakeHandler, case: str):
        self.case = case
        self.status = handler.status
        self.headers = {k.lower(): v for k, v in handler.headers_out}
        raw = handler.wfile.getvalue()
        self.chunked = self.headers.get("transfer-encoding", "").lower() == "chunked"
        self.body = dechunk(raw) if self.chunked else raw
        self.events = []
        buf = self.body
        while True:
            one, buf = airelay.split_sse_event(buf)
            if one is None:
                break
            self.events.append((airelay.sse_event_type(one), airelay.sse_event_data(one)))

    @property
    def types(self):
        return [t for t, _ in self.events]

    @property
    def text(self):
        out = []
        for t, data in self.events:
            if t != "content_block_delta":
                continue
            try:
                obj = json.loads(data)
            except ValueError:
                continue
            out.append((obj.get("delta") or {}).get("text") or "")
        return "".join(out)

    @property
    def error_events(self):
        return [d for t, d in self.events if t == "error"]

    def json(self):
        try:
            return json.loads(self.body)
        except ValueError:
            return None


# ---------------------------------------------------------------- test harness


DEFAULT_UPSTREAMS = [{"name": "flaky", "base_url": "http://up1.invalid", "auth": "key-one"}]


class Harness:
    def __init__(self, upstreams=None, **over):
        raw = {
            "listen": {"host": "127.0.0.1", "port": 0},
            "upstreams": upstreams or DEFAULT_UPSTREAMS,
            "retry": {"max_attempts": 8, "initial_backoff": 0.05, "max_backoff": 0.2,
                      "backoff_multiplier": 2.0, "jitter": 0.2,
                      "respect_retry_after": True, "max_retry_after": 5},
            "timeouts": {"connect": 2, "read": 5, "stream_idle": 5, "total": 20},
            "stream": {"mode": "gated", "keepalive_interval": 0.05,
                       "optimistic_commit_after": 999, "max_gate_wait": 300},
            "breaker": {"failure_threshold": 3, "cooldown_seconds": 0.5,
                        "max_cooldown_seconds": 2, "rotate_cooldown_seconds": 1.0},
            "log": {"file": None, "echo": VERBOSE},
        }
        for key, val in over.items():
            if isinstance(val, dict) and isinstance(raw.get(key), dict):
                raw[key].update(val)
            else:
                raw[key] = val
        self.cfg = airelay.Config(raw, source="<selftest>")
        self.log = airelay.Logger(None, echo=VERBOSE)
        self.stats = airelay.Stats()
        self.pool = airelay.Pool(self.cfg, self.log)
        self.router = airelay.Router(self.cfg, self.pool, self.log, self.stats)

    def up(self, name):
        for u in self.cfg.upstreams:
            if u.name == name:
                return u
        raise KeyError(name)

    def call(self, scen, *, streaming=True, path="/v1/messages", model="claude-opus-5",
             headers=None, break_after=None, case=None):
        case = case or "%s-%d" % (scen, next(_CASE_SEQ))
        body = json.dumps({"model": model, "max_tokens": 64, "stream": streaming,
                           "messages": [{"role": "user", "content": "hi"}]}).encode("utf-8")
        hdrs = {"Content-Type": "application/json", "Accept": "text/event-stream",
                "anthropic-version": "2023-06-01", "x-api-key": "client-token-xyz",
                "X-Scenario": scen, "X-Case": case}
        if headers:
            hdrs.update(headers)
        h = FakeHandler(break_after=break_after)
        writer = airelay.ClientWriter(h)
        ex = airelay.Exchange(case, "POST", path, hdrs, body, streaming, writer)
        self.router.execute(ex)
        return Result(h, case)


def section(title):
    print("\n" + title)


# ------------------------------------------------- the real HTTP layer (still no socket)


class _ParseSock:
    """A fake socket for http.client.HTTPResponse; only makefile is needed."""

    def __init__(self, data: bytes):
        self.data = data

    def makefile(self, *_a, **_kw):
        return io.BytesIO(self.data)


class HTTPResult:
    """Read the raw HTTP bytes the Handler wrote back with a real HTTP parser."""

    def __init__(self, raw: bytes, method: str):
        resp = http.client.HTTPResponse(_ParseSock(raw), method=method)
        resp.begin()
        self.status = resp.status
        self.headers = {k.lower(): v for k, v in resp.getheaders()}
        self.body = resp.read()
        resp.close()
        self.chunked = self.headers.get("transfer-encoding", "").lower() == "chunked"
        self.ctype = self.headers.get("content-type", "")
        self.events = []
        buf = self.body
        while True:
            one, buf = airelay.split_sse_event(buf)
            if one is None:
                break
            self.events.append((airelay.sse_event_type(one), airelay.sse_event_data(one)))

    types = Result.types
    text = Result.text
    error_events = Result.error_events
    json = Result.json


class LoopbackHandler(airelay.Handler):
    """Run the real BaseHTTPRequestHandler.handle(), with rfile/wfile in memory."""

    def __init__(self, raw_request: bytes, server):
        self.rfile = io.BytesIO(raw_request)
        self.wfile = io.BytesIO()
        self.connection = None
        self.request = None
        self.client_address = ("127.0.0.1", 54321)
        self.server = server
        self.handle()


def raw_call(h: Harness, method: str, path: str, body: bytes = b"", headers=None,
             no_content_length=False) -> HTTPResult:
    hdrs = {"Host": "127.0.0.1:8787", "Accept": "*/*", "anthropic-version": "2023-06-01",
            "x-api-key": "client-token-xyz", "Connection": "close"}
    hdrs.update(headers or {})
    if body and not no_content_length:
        hdrs["Content-Length"] = str(len(body))
    lines = ["%s %s HTTP/1.1" % (method, path)]
    lines += ["%s: %s" % (k, v) for k, v in hdrs.items()]
    raw = ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8") + body
    hd = LoopbackHandler(raw, h)
    return HTTPResult(hd.wfile.getvalue(), method)


def check(label, cond, extra=""):
    global CHECKS
    CHECKS += 1
    if cond:
        print("  ✓ %s" % label)
    else:
        FAILS.append(label)
        print("  ✗ %s   %s" % (label, extra))


# ---------------------------------------------------------------- cases


def t01_baseline():
    section("1. Baseline: when the upstream is fine, stay out of the way")
    h = Harness()
    r = h.call("ok_stream")
    check("streaming 200 passes through", r.status == 200 and r.chunked, r.status)
    check("body intact", r.text == FULL_TEXT, repr(r.text))
    check("event order intact", r.types[-1] == "message_stop" and "message_start" in r.types, r.types)
    check("upstream hit exactly once", BACKEND.count(r.case) == 1, BACKEND.count(r.case))
    check("source channel tagged", r.headers.get("x-airelay-upstream") == "flaky", r.headers)

    r = h.call("ok_json", streaming=False)
    check("non-streaming 200 passes through", r.status == 200 and not r.chunked, r.status)
    check("non-streaming body identical", r.body == MSG_JSON)
    check("succeeded=2", h.stats.succeeded == 2, h.stats.succeeded)
    check("retries=0", h.stats.retries == 0, h.stats.retries)


def t02_status_retry():
    section("2. 500 / 529 / Cloudflare 520: back off and retry, client none the wiser")
    h = Harness()
    r = h.call("fail2_then_ok")
    check("200 in the end", r.status == 200, r.status)
    check("client saw not one error event", r.error_events == [], r.error_events)
    check("body intact", r.text == FULL_TEXT, repr(r.text))
    check("upstream hit 3 times", BACKEND.count(r.case) == 3, BACKEND.count(r.case))
    check("counted as an invisible recovery", h.stats.invisible_recoveries == 1, h.stats.invisible_recoveries)
    check("attempt header = 3", r.headers.get("x-airelay-attempt") == "3", r.headers)

    r = h.call("cf_520_then_ok")
    check("Cloudflare 520 retries too", r.text == FULL_TEXT and r.error_events == [], r.types)


def t03_sse_error_event():
    section("3. 200 + event:error before content: retried away, never shown to the client")
    h = Harness()
    r = h.call("sse_error_then_ok")
    check("200 in the end", r.status == 200, r.status)
    check("no error event leaked to the client", r.error_events == [], r.error_events)
    check("body intact", r.text == FULL_TEXT, repr(r.text))
    check("retried once", BACKEND.count(r.case) == 2, BACKEND.count(r.case))
    check("no duplicate message_start", r.types.count("message_start") == 1, r.types)


def t04_truncate_pre_content():
    section("4. Stream dies before content: the gate holds it back, retry silently")
    h = Harness()
    r = h.call("truncate_pre")
    check("200 in the end", r.status == 200, r.status)
    check("body intact and delivered once", r.text == FULL_TEXT, repr(r.text))
    check("no error event", r.error_events == [], r.error_events)
    check("message_start appears once", r.types.count("message_start") == 1, r.types)
    check("retried once", BACKEND.count(r.case) == 2, BACKEND.count(r.case))


def t05_truncate_mid_gated_vs_buffered():
    section("5. Stream dies mid-content: gated cannot fix it (says so honestly), buffered can")
    hg = Harness()
    r = hg.call("truncate_mid")
    check("gated: client got a truncated body", r.text == "Hel", repr(r.text))
    check("gated: a valid error event was appended", len(r.error_events) >= 1, r.error_events)
    check("gated: the error event is parseable JSON",
          bool(r.error_events) and (json.loads(r.error_events[-1]).get("type") == "error"),
          r.error_events)
    check("gated: counted in unrecoverable_midstream", hg.stats.unrecoverable_midstream == 1,
          hg.stats.unrecoverable_midstream)
    check("gated: did not blindly resend content", BACKEND.count(r.case) == 1, BACKEND.count(r.case))

    hb = Harness(stream={"mode": "buffered"})
    r = hb.call("truncate_mid")
    check("buffered: body intact", r.text == FULL_TEXT, repr(r.text))
    check("buffered: no error events", r.error_events == [], r.error_events)
    check("buffered: recovered invisibly", hb.stats.invisible_recoveries == 1,
          hb.stats.invisible_recoveries)
    check("buffered: unrecoverable=0", hb.stats.unrecoverable_midstream == 0,
          hb.stats.unrecoverable_midstream)
    check("buffered: retried once", BACKEND.count(r.case) == 2, BACKEND.count(r.case))


def t06_fake_200():
    section("6. A relay specialty: HTTP 200 with an error body, or an empty body")
    h = Harness()
    r = h.call("fake200_then_ok", streaming=False)
    check("fake 200 caught and retried", r.status == 200 and r.body == MSG_JSON, r.body[:80])
    check("hit twice", BACKEND.count(r.case) == 2, BACKEND.count(r.case))

    r = h.call("empty200_then_ok", streaming=False)
    check("empty-body 200 caught and retried", r.body == MSG_JSON, r.body[:80])
    check("hit twice", BACKEND.count(r.case) == 2, BACKEND.count(r.case))


def t07_network_flap():
    section("7. Connection reset / read timeout: retryable")
    h = Harness()
    r = h.call("net_flap_then_ok")
    check("200 in the end", r.status == 200, r.status)
    check("body intact", r.text == FULL_TEXT, repr(r.text))
    check("hit 3 times", BACKEND.count(r.case) == 3, BACKEND.count(r.case))
    check("no error events for the client", r.error_events == [], r.error_events)


def t08_retry_after():
    section("8. 429 with retry-after: wait as long as it says")
    h = Harness()
    t0 = time.monotonic()
    r = h.call("rate_limited_then_ok")
    waited = time.monotonic() - t0
    check("succeeded in the end", r.text == FULL_TEXT, repr(r.text))
    check("waited >= retry-after (0.4s)", waited >= 0.38, "%.3fs" % waited)
    check("did not overshoot", waited < 2.0, "%.3fs" % waited)


def t09_rotation_and_breaker():
    section("9. Channel rotation + breaker cooldown")
    ups = [{"name": "broken", "base_url": "http://up1.invalid", "auth": "key-one"},
           {"name": "backup", "base_url": "http://up2.invalid", "auth": "key-two"}]
    h = Harness(upstreams=ups)
    BACKEND.pin["backup"] = "ok_stream"      # the spare channel is always healthy

    r = h.call("always_401")
    check("401 rotates immediately and succeeds", r.text == FULL_TEXT, repr(r.text))
    check("the broken channel was tried once", BACKEND.count(r.case, "broken") == 1,
          BACKEND.count(r.case, "broken"))
    check("the spare picked it up", BACKEND.count(r.case, "backup") == 1,
          BACKEND.count(r.case, "backup"))
    check("rotation counted", h.stats.rotations >= 1, h.stats.rotations)
    check("the broken channel went into cooldown", not h.up("broken").available(),
          h.up("broken").cooldown_until - airelay._now())
    check("cooldown length came from rotate_cooldown",
          h.up("broken").cooldown_until - airelay._now() > 0.5,
          h.up("broken").cooldown_until - airelay._now())

    r2 = h.call("always_401")
    check("during cooldown it goes straight to the spare",
          BACKEND.count(r2.case, "broken") == 0 and r2.text == FULL_TEXT,
          BACKEND.count(r2.case, "broken"))

    # repeated 500s -> only trips the breaker once the threshold is reached
    h2 = Harness(upstreams=[dict(ups[0])], retry={"max_attempts": 4})
    r3 = h2.call("always_500")
    check("one channel used up max_attempts", BACKEND.count(r3.case, "broken") == 4,
          BACKEND.count(r3.case, "broken"))
    check("consecutive-failure count correct", h2.up("broken").consec_fail == 4, h2.up("broken").consec_fail)
    check("breaker trips past the threshold", not h2.up("broken").available())


def t10_sticky_primary():
    section("10. sticky_primary: no drifting while the primary is healthy (keeps prompt cache)")
    ups = [{"name": "primary", "base_url": "http://up1.invalid", "auth": "k1"},
           {"name": "spare", "base_url": "http://up2.invalid", "auth": "k2"}]
    h = Harness(upstreams=ups)
    BACKEND.pin["spare"] = "ok_stream"
    for _ in range(3):
        r = h.call("ok_stream")
        check("always on the primary", BACKEND.count(r.case, "spare") == 0 and
              BACKEND.count(r.case, "primary") == 1, r.headers.get("x-airelay-upstream"))
    check("rotations still 0", h.stats.rotations == 0, h.stats.rotations)


def t11_fatal_passthrough():
    section("11. Client-side 400s: passed straight back, never retried")
    h = Harness()
    r = h.call("bad_request")
    check("status stays 400", r.status == 400, r.status)
    body = r.json()
    check("error body passed through verbatim",
          bool(body) and body.get("error", {}).get("type") == "invalid_request_error", r.body)
    check("upstream hit exactly once", BACKEND.count(r.case) == 1, BACKEND.count(r.case))
    check("failed=1", h.stats.failed == 1, h.stats.failed)
    check("the channel was not blamed", h.up("flaky").available())


def t12_exhausted_paths():
    section("12. When it really cannot be saved: fail validly instead of hanging")
    # A) headers not committed yet -> standard JSON error + 503
    h = Harness(retry={"max_attempts": 3}, stream={"optimistic_commit_after": 999})
    r = h.call("always_500")
    check("A: 503 while uncommitted", r.status == 503, r.status)
    body = r.json()
    check("A: a valid Anthropic error body",
          bool(body) and body.get("type") == "error" and "airelay" in
          body.get("error", {}).get("message", ""), r.body[:120])
    check("A: used all 3 attempts", BACKEND.count(r.case) == 3, BACKEND.count(r.case))

    # B) already optimistically committed (client is waiting on pings) -> close with an SSE error
    h2 = Harness(retry={"max_attempts": 4},
                 stream={"optimistic_commit_after": 0.01, "keepalive_interval": 0.05})
    r2 = h2.call("always_500")
    check("B: committed 200 + SSE early", r2.status == 200 and r2.chunked, r2.status)
    check("B: pings went out during retries", r2.types.count("ping") >= 1, r2.types)
    check("B: ends with an error event", r2.types[-1] == "error", r2.types)
    check("B: the error event is valid JSON",
          bool(r2.error_events) and json.loads(r2.error_events[-1])["error"]["type"] == "api_error",
          r2.error_events)
    check("B: no fabricated content", r2.text == "", repr(r2.text))


def t13_invisible_long_retry():
    section("13. The key scenario: only pings during a long retry, then the complete body")
    h = Harness(retry={"max_attempts": 8, "initial_backoff": 0.15, "max_backoff": 0.3},
                stream={"optimistic_commit_after": 0.01, "keepalive_interval": 0.05})
    r = h.call("fail2_then_ok")
    check("status 200", r.status == 200, r.status)
    check("pings arrived during retries", r.types.count("ping") >= 2, r.types.count("ping"))
    check("zero error events", r.error_events == [], r.error_events)
    check("body intact", r.text == FULL_TEXT, repr(r.text))
    check("exactly one message_start", r.types.count("message_start") == 1, r.types)
    check("closes with message_stop", r.types[-1] == "message_stop", r.types[-3:])
    check("the client never needs a try again", h.stats.failed == 0 and h.stats.succeeded == 1,
          (h.stats.failed, h.stats.succeeded))


def t14_client_disconnect():
    section("14. Client hangs up: wrap up cleanly, no error spam, not an upstream fault")
    h = Harness()
    r = h.call("ok_stream", break_after=2)
    check("recognised as a client disconnect", h.stats.client_disconnects == 1, h.stats.client_disconnects)
    check("not counted as an upstream failure", h.up("flaky").consec_fail == 0, h.up("flaky").consec_fail)
    check("nothing resent", BACKEND.count(r.case) == 1, BACKEND.count(r.case))


def t15_headers_and_auth():
    section("15. Request header rewriting: never send two credentials at once (guaranteed 401)")
    h = Harness(upstreams=[{"name": "xkey", "base_url": "http://a.invalid",
                            "auth": "sk-ant-api-TEST"}])
    h.call("ok_json", streaming=False)
    hd = {k.lower(): v for k, v in BACKEND.last("xkey")[3].items()}
    check("sk-ant-api uses x-api-key", hd.get("x-api-key") == "sk-ant-api-TEST", hd.get("x-api-key"))
    check("no Authorization header", "authorization" not in hd, hd)
    check("the client token was not forwarded", "client-token-xyz" not in json.dumps(hd), hd)
    check("identity encoding forced", hd.get("accept-encoding") == "identity", hd.get("accept-encoding"))
    check("anthropic-version filled in", "anthropic-version" in hd, hd)
    check("airelay marker set", hd.get("x-airelay") == airelay.VERSION, hd.get("x-airelay"))

    h = Harness(upstreams=[{"name": "relay", "base_url": "http://b.invalid", "auth": "sk-relay-1"}])
    h.call("ok_json", streaming=False)
    hd = {k.lower(): v for k, v in BACKEND.last("relay")[3].items()}
    check("relays default to Bearer", hd.get("authorization") == "Bearer sk-relay-1", hd.get("authorization"))
    check("no x-api-key", "x-api-key" not in hd, hd)

    h = Harness(upstreams=[{"name": "pt", "base_url": "http://c.invalid", "auth": "passthrough"}])
    h.call("ok_json", streaming=False)
    hd = {k.lower(): v for k, v in BACKEND.last("pt")[3].items()}
    check("passthrough forwards the client credential", hd.get("x-api-key") == "client-token-xyz", hd.get("x-api-key"))
    check("passthrough adds no Authorization", "authorization" not in hd, hd)

    h = Harness(upstreams=[{"name": "oat", "base_url": "http://d.invalid",
                            "auth": "sk-ant-oat01-xyz"}])
    h.call("ok_json", streaming=False)
    hd = {k.lower(): v for k, v in BACKEND.last("oat")[3].items()}
    check("OAuth tokens use Bearer", hd.get("authorization") == "Bearer sk-ant-oat01-xyz",
          hd.get("authorization"))


def t16_model_map_and_prefix():
    section("16. model_map rewriting + a base_url with a path prefix")
    h = Harness(upstreams=[{"name": "aliased", "base_url": "http://e.invalid/relay/v9",
                            "auth": "k", "model_map": {"claude-opus-5": "opus-5-alias"}}])
    h.call("ok_json", streaming=False)
    up, method, url, hd, body = BACKEND.last("aliased")
    check("prefix prepended to the path", url == "/relay/v9/v1/messages", url)
    check("model mapped", json.loads(body)["model"] == "opus-5-alias", body[:80])
    check("every other field untouched", json.loads(body)["max_tokens"] == 64, body[:80])

    h2 = Harness()
    h2.call("ok_json", streaming=False)
    check("body untouched when there is no model_map",
          json.loads(BACKEND.last("flaky")[4])["model"] == "claude-opus-5")


def t17_other_paths():
    section("17. Paths other than /v1/messages are proxied too")
    h = Harness()
    r = h.call("count_tokens", streaming=False, path="/v1/messages/count_tokens")
    check("count_tokens works", r.status == 200 and r.json() == {"input_tokens": 12}, r.body)
    check("path not mangled", BACKEND.last()[2] == "/v1/messages/count_tokens", BACKEND.last()[2])


def t18_classifiers():
    section("18. Classifier unit tests (status codes / error bodies)")
    cs = airelay.classify_status
    for code in (429, 500, 502, 503, 504, 520, 522, 524, 529, 408):
        check("HTTP %d is retryable" % code, cs(code) == "retry", cs(code))
    for code in (401, 402, 403, 404, 407):
        check("HTTP %d rotates" % code, cs(code) == "rotate", cs(code))
    for code in (400, 413, 422):
        check("HTTP %d is fatal" % code, cs(code) == "fatal", cs(code))

    cep = airelay.classify_error_payload
    check("overloaded_error → retry",
          cep(err_json("overloaded_error", "x").decode())[0] == "retry")
    check("authentication_error → rotate",
          cep(err_json("authentication_error", "x").decode())[0] == "rotate")
    check("invalid_request_error → fatal",
          cep(err_json("invalid_request_error", "x").decode())[0] == "fatal")
    check("non-JSON keeps the default", cep("<html>502 Bad Gateway</html>", default="retry")[0] == "retry")
    check("detect_json_error catches a fake 200",
          airelay.detect_json_error(err_json("overloaded_error", "x"))[0] == "retry")
    check("detect_json_error lets a normal message through", airelay.detect_json_error(MSG_JSON) is None)

    pra = airelay.parse_retry_after
    check("retry-after in seconds", pra("12") == 12.0, pra("12"))
    check("retry-after with decimals", pra("0.5") == 0.5, pra("0.5"))
    future = pra("Wed, 21 Oct 2099 07:28:00 GMT")
    check("retry-after accepts an HTTP date", future is not None and future > 0, future)
    check("retry-after in the past is not negative", pra("Wed, 21 Oct 1999 07:28:00 GMT") == 0.0,
          pra("Wed, 21 Oct 1999 07:28:00 GMT"))
    check("retry-after garbage -> None", pra("soon") is None, pra("soon"))
    check("retry-after empty -> None", pra(None) is None)


def t19_secrets():
    section("19. Secret handling: never stored in plaintext, redacted in logs")
    os.environ["AIRELAY_TEST_KEY"] = "sk-secret-value"
    check("${ENV} resolves", airelay.resolve_secret("${AIRELAY_TEST_KEY}") == "sk-secret-value")
    check("passthrough -> sentinel", airelay.resolve_secret("passthrough") is airelay.PASSTHROUGH)
    p = os.path.join(os.environ.get("TMPDIR", "."), "airelay_key_test.txt")
    with open(p, "w", encoding="utf-8") as fh:
        fh.write("  sk-from-file\n")
    try:
        check("file: resolves and strips whitespace", airelay.resolve_secret("file:" + p) == "sk-from-file",
              airelay.resolve_secret("file:" + p))
    finally:
        os.unlink(p)
    check("redact keeps head and tail for recognition", airelay.redact("sk-ant-api-0123456789abcdef")
          == "sk-ant…cdef", airelay.redact("sk-ant-api-0123456789abcdef"))
    check("redact does not leak the middle",
          "0123456789" not in airelay.redact("sk-ant-api-0123456789abcdef"),
          airelay.redact("sk-ant-api-0123456789abcdef"))
    check("redact hides short strings entirely", airelay.redact("abc") == "…", airelay.redact("abc"))
    check("redacting passthrough stays readable", "passthrough" in airelay.redact(airelay.PASSTHROUGH),
          airelay.redact(airelay.PASSTHROUGH))
    try:
        airelay.resolve_secret("${AIRELAY_MISSING_KEY_XYZ}")
        check("a missing env var must raise", False)
    except airelay.ConfigError:
        check("a missing env var must raise", True)


def t20_config_guard():
    section("20. Config validation")
    for bad, why in [
        ({"upstreams": []}, "empty upstreams"),
        ({"upstreams": [{"name": "x"}]}, "missing base_url"),
        ({"upstreams": [{"name": "x", "base_url": "ftp://h"}]}, "unsupported scheme"),
        ({"upstreams": [{"name": "x", "base_url": "http://h"}], "stream": {"mode": "wat"}},
         "invalid stream.mode"),
        ({"upstreams": [{"name": "x", "base_url": "http://h", "enabled": False}]}, "all disabled"),
    ]:
        try:
            airelay.Config(bad)
            check(why + " must raise", False)
        except airelay.ConfigError:
            check(why + " must raise", True)
    cfg = airelay.Config({"upstreams": [{"name": "x", "base_url": "relay.example.com/v1"}]})
    check("a bare hostname gets https", cfg.upstreams[0].scheme == "https", cfg.upstreams[0].scheme)
    check("port defaults to 443", cfg.upstreams[0].port == 443, cfg.upstreams[0].port)
    check("prefix preserved", cfg.upstreams[0].prefix == "/v1", cfg.upstreams[0].prefix)


def t21_stats_and_snapshot():
    section("21. Stats and snapshots (what backs /__airelay/stats)")
    h = Harness()
    h.call("ok_stream")
    h.call("fail2_then_ok")
    h.call("bad_request")
    snap = h.stats.snapshot()
    check("requests=3", snap["requests"] == 3, snap)
    check("succeeded=2", snap["succeeded"] == 2, snap)
    check("failed=1", snap["failed"] == 1, snap)
    check("retries=2", snap["retries"] == 2, snap)
    check("invisible_recoveries=1", snap["invisible_recoveries"] == 1, snap)
    check("has an uptime field", "uptime_seconds" in snap, snap)
    us = h.pool.snapshot()[0]
    check("channel snapshot has name", us.get("name") == "flaky", us)
    check("channel snapshot holds no plaintext key", "key-one" not in json.dumps(us, ensure_ascii=False), us)
    for field in ("healthy", "cooldown_remaining_s", "consecutive_failures", "ok", "failed",
                  "avg_latency_ms", "last_error"):
        check("channel snapshot has %s" % field, field in us, list(us))
    check("the key in the snapshot is redacted", "…" in str(us.get("auth")) or
          us.get("auth") in ("<passthrough>", "<empty>"), us.get("auth"))


def t22_example_config():
    section("22. The bundled config.example.json actually works")
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.example.json")
    check("template exists", os.path.exists(path))
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    for spec in raw.get("upstreams", []):
        auth = str(spec.get("auth", ""))
        check("channel %s holds no plaintext key" % spec.get("name"),
              auth in ("passthrough", "") or auth.startswith(("${", "file:")), auth)
    cfg = airelay.Config(raw, source=path)
    check("the template loads", len(cfg.upstreams) >= 1, len(cfg.upstreams))
    check("disabled channels do not block startup (placeholder URLs / unset env vars)",
          all(u.name == "agentrouter" for u in cfg.upstreams),
          [u.name for u in cfg.upstreams])
    check("gated by default", cfg.stream_mode == "gated", cfg.stream_mode)
    check("enough retries by default", cfg.max_attempts >= 10, cfg.max_attempts)
    check("a long enough total budget by default", cfg.total_timeout >= 600, cfg.total_timeout)

    # Regression: an unset env var inside a disabled channel must not break startup
    raw2 = {"upstreams": [
        {"name": "live", "base_url": "http://x.invalid", "auth": "passthrough"},
        {"name": "dead", "base_url": "https://placeholder.invalid", "auth": "${NOPE_NOT_SET_XYZ}",
         "enabled": False},
    ]}
    try:
        cfg2 = airelay.Config(raw2)
        check("disabled channels skipped entirely", [u.name for u in cfg2.upstreams] == ["live"],
              [u.name for u in cfg2.upstreams])
    except airelay.ConfigError as exc:
        check("disabled channels skipped entirely", False, str(exc))


def t23_http_layer():
    section("23. The real HTTP layer: request line, chunked body, admin endpoints, self-loop guard")
    h = Harness()

    r = raw_call(h, "POST", "/v1/messages",
                 body=json.dumps({"model": "claude-opus-5", "max_tokens": 64, "stream": True,
                                  "messages": [{"role": "user", "content": "hi"}]}).encode(),
                 headers={"X-Scenario": "fail2_then_ok", "X-Case": "http-1",
                          "Content-Type": "application/json"})
    check("HTTP 200", r.status == 200, r.status)
    check("response is chunked SSE", r.chunked and "text/event-stream" in r.ctype, (r.chunked, r.ctype))
    check("client parsed the whole body", r.text == FULL_TEXT, repr(r.text))
    check("zero error events", r.error_events == [], r.error_events)
    check("upstream really was hit 3 times", BACKEND.count("http-1") == 3, BACKEND.count("http-1"))

    # the client sends its body chunked (some SDKs do this)
    payload = json.dumps({"model": "claude-opus-5", "max_tokens": 64, "stream": False,
                          "messages": [{"role": "user", "content": "hi"}]}).encode()
    chunked_body = b"%x\r\n%s\r\n0\r\n\r\n" % (len(payload), payload)
    r = raw_call(h, "POST", "/v1/messages", body=chunked_body,
                 headers={"X-Scenario": "ok_json", "X-Case": "http-2",
                          "Transfer-Encoding": "chunked", "Content-Type": "application/json"},
                 no_content_length=True)
    check("chunked request body reassembled", r.status == 200 and r.body == MSG_JSON, r.status)
    check("body forwarded byte for byte", BACKEND.last()[4] == payload, BACKEND.last()[4][:80])

    # non-streaming requests must use Content-Length, not chunked
    check("non-streaming response carries Content-Length", r.headers.get("content-length") == str(len(MSG_JSON)),
          r.headers.get("content-length"))

    r = raw_call(h, "GET", "/__airelay/health")
    check("/__airelay/health works", r.status == 200 and r.json().get("status") == "ok", r.body)

    r = raw_call(h, "GET", "/__airelay/stats")
    js = r.json() or {}
    check("/__airelay/stats works", r.status == 200 and "totals" in js and "upstreams" in js,
          list(js))
    check("stats counted the silent recovery", js.get("totals", {}).get("invisible_recoveries") == 1,
          js.get("totals"))
    check("no plaintext key in stats", "key-one" not in r.body.decode("utf-8", "replace"), "")

    r = raw_call(h, "GET", "/__airelay/nope")
    check("unknown admin endpoint returns 404", r.status == 404, r.status)

    # self-loop: an upstream base_url pointing back at the proxy
    r = raw_call(h, "POST", "/v1/messages", body=b'{"model":"x","max_tokens":1}',
                 headers={"X-Airelay": airelay.VERSION})
    check("self-loop detected, 508 returned", r.status == 508, r.status)
    check("the self-loop message is readable",
          "self-loop" in (r.json() or {}).get("error", {}).get("message", ""), r.body[:160])

    # malformed request body
    r = raw_call(h, "POST", "/v1/messages", body=b"not json at all",
                 headers={"X-Scenario": "ok_json", "X-Case": "http-3",
                          "Content-Type": "application/json"})
    check("a non-JSON body is still forwarded (the upstream decides)", r.status == 200, r.status)
    check("non-JSON is treated as non-streaming", not r.chunked, r.chunked)


def t24_is_streaming():
    section("24. Streaming detection")
    f = airelay.Handler._is_streaming
    check("stream:true -> streaming", f(b'{"stream": true, "model": "x"}') is True)
    check("stream:false -> not streaming", f(b'{"stream": false, "model": "x"}') is False)
    check("no stream field -> not streaming", f(b'{"model": "x"}') is False)
    check("empty body -> not streaming", f(b"") is False)
    check("invalid JSON containing stream:true -> still streaming",
          f(b'{"stream":true, "model": broken') is True)
    check("the string 'true' does not count", f(b'{"stream": "true"}') is False)
    big = b'{"messages":[' + b'{"role":"user","content":"' + b"x" * 20000 + b'"},' \
          b'{"role":"user","content":"y"}], "stream": true}'
    check("stream at the end of a huge body is still found", f(big) is True)


def t25_cli():
    section("25. CLI: doctor / stats / arguments and error messages")
    import contextlib
    import argparse

    tmp = os.environ.get("TMPDIR", ".")
    cfg_path = os.path.join(tmp, "airelay_cli_test.json")
    with open(cfg_path, "w", encoding="utf-8") as fh:
        json.dump({"listen": {"port": 8799},
                   "upstreams": [
                       {"name": "relay", "base_url": "http://cli1.invalid", "auth": "sk-relay"},
                       {"name": "pt", "base_url": "http://cli2.invalid", "auth": "passthrough"}],
                   "log": {"file": None, "echo": False}}, fh)
    try:
        def run_doctor(**kw):
            args = argparse.Namespace(config=cfg_path, model=None, models=None, key=None)
            for k, v in kw.items():
                setattr(args, k, v)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = airelay.cmd_doctor(args)
            return rc, out.getvalue()

        BACKEND.pin["relay"] = "count_tokens"
        BACKEND.pin["pt"] = "count_tokens"
        rc, out = run_doctor()
        check("doctor runs", rc == 0, out[-200:])
        check("doctor reported the healthy channel", out.count("✅") == 1, out)
        check("doctor skips passthrough channels by default", "skipped" in out, out)
        check("doctor explains how to check passthrough", "--key" in out, out)
        check("doctor prints no plaintext key", "sk-relay" not in out, out)

        rc, out = run_doctor(key="sk-borrowed-123")
        check("with --key, passthrough is checked too", out.count("✅") == 2, out)
        check("the borrowed credential is redacted", "sk-borrowed-123" not in out, out)

        BACKEND.pin["relay"] = "always_401"
        rc, out = run_doctor(key="sk-borrowed-123")
        check("doctor returns non-zero for a broken channel", rc == 1, rc)
        check("doctor flags rotate-class failures", "rotate" in out, out)
        check("doctor reassures: one working channel is enough", "will not stall" in out, out[-300:])

        BACKEND.pin.clear()
        BACKEND.pin["relay"] = "always_500"
        BACKEND.pin["pt"] = "always_500"
        rc, out = run_doctor(key="k")
        check("doctor says 5xx is retried automatically", "retried automatically" in out, out)

        # stats: when it cannot connect, say so in words instead of raising
        args = argparse.Namespace(config=cfg_path, host="127.0.0.1", port=59999)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = airelay.cmd_stats(args)
        check("stats fails gracefully when unreachable", rc == 1 and "cannot reach" in out.getvalue(), out.getvalue())

        # argument parsing
        out = io.StringIO()
        err = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = airelay.main([])
        check("no subcommand prints help and returns 1", rc == 1 and "usage" in out.getvalue().lower(),
              out.getvalue()[:200])

        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = airelay.main(["serve", "--config", os.path.join(tmp, "no_such_airelay.json")])
        check("a missing config returns 2 instead of raising", rc == 2, rc)
        check("a missing config gives actionable advice", "config.example.json" in err.getvalue(),
              err.getvalue()[:300])
    finally:
        BACKEND.pin.clear()
        if os.path.exists(cfg_path):
            os.unlink(cfg_path)


# ------------------------------------------------- automatic model switching


TWO_UPS = [{"name": "one", "base_url": "http://up1.invalid", "auth": "k1"},
           {"name": "two", "base_url": "http://up2.invalid", "auth": "k2"}]


def _models_sent(case):
    """The models actually sent upstream for this case (BACKEND.requests is section-wide)."""
    return [_sent_model(row) for row in BACKEND.requests
            if (row[3] or {}).get("X-Case") == case]


def _latin1_clean(headers):
    """
    http.server encodes response headers with latin-1/strict, so a single non-ASCII
    character in a header value raises UnicodeEncodeError deep inside the real server.
    The in-process fake handler stores header tuples without encoding them, so this has
    to be asserted explicitly or the whole class of bug slips past the suite.
    """
    for key, val in headers.items():
        for part in (key, str(val)):
            try:
                part.encode("latin-1", "strict")
            except UnicodeEncodeError:
                return False
    return True


def t26_model_failure_classifier():
    section("26. Telling \"channel is broken\" apart from \"channel lacks this model\"")
    lmu = airelay.looks_model_unavailable
    check("404 + model not found -> model level",
          lmu(404, "model: claude-opus-5 not found", "claude-opus-5"))
    check("400 + invalid model -> model level", lmu(400, "invalid model: foo", "foo"))
    check("403 + Chinese \"model not supported\" -> model level", lmu(403, "不支持该模型", "claude-opus-5"))
    check("404 + Chinese \"no such model\" -> model level (a 404 mentioning a model is little else)",
          lmu(404, "无此模型 claude-opus-5", "claude-opus-5"))
    check("mentioning just the model name counts as mentioning the model",
          lmu(404, "claude-opus-5 is not available on this plan", "claude-opus-5"))
    # All of the following must be False, or ordinary errors get mistaken for a missing model
    check("a 400 about max_tokens is not a missing model",
          not lmu(400, "max_tokens: must be positive", "claude-opus-5"))
    check("a 404 that mentions no model is not a missing model (could be a bad path)",
          not lmu(404, "not found", None))
    check("5xx is not a missing model (that one is retried)",
          not lmu(500, "model not found", "claude-opus-5"))
    check("401 is not a missing model (that is credentials, rotate)",
          not lmu(401, "invalid x-api-key for model", "claude-opus-5"))
    check("an error event in the stream (no status) is still recognised",
          lmu(None, "model claude-opus-5 not found", "claude-opus-5"))
    check("no guessing when there is neither a status nor a model", not lmu(None, "internal server error", None))

    em = airelay.extract_model
    check("reads the model out", em(b'{"model":"claude-opus-5","stream":true}') == "claude-opus-5")
    check("no model field -> None", em(b'{"stream":true}') is None)
    check("not JSON -> None", em(b"not json at all") is None)
    check("empty body -> None", em(b"") is None)
    check("model is not a string -> None", em(b'{"model":123}') is None)

    rm = airelay.replace_model
    swapped = rm(b'{"model":"a","max_tokens":64}', "b")
    check("model swapped", json.loads(swapped)["model"] == "b", swapped)
    check("no other field touched", json.loads(swapped)["max_tokens"] == 64, swapped)
    zh = rm('{"model":"a","messages":[{"role":"user","content":"你好"}]}'.encode("utf-8"), "b")
    check("non-ASCII content is not \\u-escaped (saves tokens)", "你好".encode("utf-8") in zh, zh)
    check("still valid JSON after the swap", json.loads(zh)["messages"][0]["content"] == "你好")
    check("no model field means leave it alone (returns None)", rm(b'{"stream":true}', "b") is None)
    check("non-JSON is left alone (returns None)", rm(b"garbage", "b") is None)


def t27_model_chain_config():
    section("27. Fallback chain config: cleaning and lookup")
    cfg = Harness(model_fallback={"chains": {"a": ["a", "b", "b", "c"],
                                             "*": ["z", "b"]}}).cfg
    check("a model never appears in its own chain (that would spin in place)",
          cfg.model_chain("a") == ["b", "c"], cfg.model_chain("a"))
    check("duplicates removed", cfg.model_chain("a") == ["b", "c"])
    check("models with no chain of their own use the * default", cfg.model_chain("q") == ["z", "b"], cfg.model_chain("q"))
    check("the * chain also drops self-references", cfg.model_chain("z") == ["b"], cfg.model_chain("z"))
    check("a request with no model has no chain", cfg.model_chain(None) == [])
    check("all_models collects every model mentioned",
          sorted(cfg.all_models()) == ["a", "b", "c", "z"], cfg.all_models())
    check("all_models does not treat * as a model name", "*" not in cfg.all_models())

    off = Harness(model_fallback={"enabled": False, "chains": {"a": ["b"]}}).cfg
    check("disabled means no chain for any model (your model is never substituted)", off.model_chain("a") == [])

    one = Harness(model_fallback={"chains": {"a": "b"}}).cfg
    check("a chain written as a bare string is accepted", one.model_chain("a") == ["b"], one.model_chain("a"))

    clamp = Harness(model_fallback={"switch_after_attempts": 0}).cfg
    check("switch_after_attempts is at least 1", clamp.model_switch_after == 1)
    check("defaults to 4", Harness().cfg.model_switch_after == 4)
    check("enabled by default", Harness().cfg.model_fallback is True)
    check("no chains by default -> behaves exactly like 1.0.0", Harness().cfg.model_chain("claude-opus-5") == [])

    try:
        Harness(model_fallback={"chains": {"a": 5}})
        check("a chain that is not an array is a config error", False, "no error raised")
    except airelay.ConfigError as exc:
        check("a chain that is not an array is a config error", "chains" in str(exc), str(exc))


def t28_missing_model_rotates():
    section("28. Channel lacks the model: rotate for the same model, and never cool that channel")
    h = Harness(upstreams=TWO_UPS,
                model_fallback={"chains": {"claude-opus-5": ["claude-sonnet-5"]}})
    BACKEND.pin["one"] = "no_opus"
    BACKEND.pin["two"] = "ok_stream"
    r = h.call("no_opus", model="claude-opus-5")
    check("the client still gets a complete answer", r.status == 200 and r.text == FULL_TEXT, r.text)
    check("the channel without it was tried once", BACKEND.count(r.case, "one") == 1, BACKEND.count(r.case, "one"))
    check("rotated to the second channel", BACKEND.count(r.case, "two") == 1)
    check("rotating does not downgrade; still the model you asked for",
          _sent_model(BACKEND.last("two")) == "claude-opus-5", BACKEND.last("two")[4])
    check("a missing model does not cool the channel (it just lacks opus)",
          h.up("one").available() and h.up("one").consec_fail == 0,
          h.up("one").cooldown_until)
    check("a missing model is not counted as a channel failure", h.up("one").fail_count == 0, h.up("one").fail_count)
    check("recorded which model this channel lacks",
          h.up("one").snapshot()["missing_models"] == ["claude-opus-5"],
          h.up("one").snapshot()["missing_models"])
    check("counted one channel-lacks-model event", h.stats.model_unavailable == 1)
    check("no model switch happened", h.stats.model_switches == 0)
    check("not finished on a fallback model", h.stats.served_on_fallback == 0)
    check("no X-Airelay-Model header without a downgrade", r.headers.get("x-airelay-model") is None,
          r.headers)
    check("recovered silently, invisible to the client", h.stats.invisible_recoveries == 1)

    # once remembered, later requests for that model go straight to a channel that has it
    r2 = h.call("no_opus", model="claude-opus-5")
    check("remembered: later requests skip the channel without it",
          BACKEND.count(r2.case, "one") == 0 and BACKEND.count(r2.case, "two") == 1,
          BACKEND.count(r2.case, "one"))
    check("the second request went through first time", h.stats.invisible_recoveries == 1)
    BACKEND.pin.clear()


def t29_all_channels_missing_switches_model():
    section("29. No channel has the model: walk the fallback chain")
    h = Harness(upstreams=TWO_UPS,
                model_fallback={"chains": {"claude-opus-5": ["claude-sonnet-5"]}})
    BACKEND.pin["one"] = "no_opus"
    BACKEND.pin["two"] = "no_opus"
    r = h.call("no_opus", model="claude-opus-5")
    check("the task continues, the client gets a complete answer", r.status == 200 and r.text == FULL_TEXT, r.text)
    check("both channels tried the original model before downgrading", BACKEND.count(r.case) == 3, BACKEND.count(r.case))
    check("the last request carried the fallback model", _sent_model() == "claude-sonnet-5", BACKEND.last()[4])
    check("one model switch", h.stats.model_switches == 1)
    check("two missing-model detections", h.stats.model_unavailable == 2)
    check("recorded as finished on a fallback model", h.stats.served_on_fallback == 1)
    check("the response header names the model actually used",
          r.headers.get("x-airelay-model") == "claude-sonnet-5", r.headers)
    check("the response header carries the full downgrade trail",
          r.headers.get("x-airelay-model-trail") == "claude-opus-5 -> claude-sonnet-5",
          r.headers.get("x-airelay-model-trail"))
    check("every response header is latin-1 encodable (http.server would raise otherwise)",
          _latin1_clean(r.headers), r.headers)
    # header_safe is the guard: channel and model names come from config, and one non-ASCII
    # character in a header value would otherwise take the whole response down.
    check("header_safe leaves ASCII alone", airelay.header_safe("claude-opus-5") == "claude-opus-5")
    check("header_safe keeps latin-1 as it is", airelay.header_safe("café") == "café")
    check("header_safe transliterates what latin-1 cannot hold",
          _latin1_clean({"x": airelay.header_safe("relay-→")}),
          airelay.header_safe("relay-→"))
    check("neither channel was cooled down",
          h.up("one").available() and h.up("two").available())
    check("nothing counted as failed", h.stats.failed == 0)
    BACKEND.pin.clear()


def t30_switch_after_repeated_failures():
    section("30. Same model failing to the threshold: switch models (the way out of a site-wide overload)")
    h = Harness(model_fallback={"chains": {"claude-opus-5": ["claude-sonnet-5"]},
                                "switch_after_attempts": 2},
                retry={"max_attempts": 6, "initial_backoff": 0.02, "max_backoff": 0.05})
    BACKEND.pin["flaky"] = "opus_overloaded"
    r = h.call("opus_overloaded", model="claude-opus-5")
    check("after two stubborn 529s it switches and still completes",
          r.status == 200 and r.text == FULL_TEXT, r.text)
    check("switches after exactly the 2nd failure, not the 1st",
          BACKEND.count(r.case) == 3, BACKEND.count(r.case))
    check("the fallback model was used in the end", _sent_model() == "claude-sonnet-5", BACKEND.last()[4])
    check("one model switch", h.stats.model_switches == 1)
    check("two retries", h.stats.retries == 2, h.stats.retries)
    check("529 is not a missing model", h.stats.model_unavailable == 0)
    check("the channel is not cooled below its threshold", h.up("flaky").available())
    check("the response header carries the downgrade trail", r.headers.get("x-airelay-model-trail") ==
          "claude-opus-5 -> claude-sonnet-5", r.headers.get("x-airelay-model-trail"))

    # below the threshold it never switches — the model you asked for comes first
    h2 = Harness(model_fallback={"chains": {"claude-opus-5": ["claude-sonnet-5"]},
                                 "switch_after_attempts": 9},
                 retry={"max_attempts": 3, "initial_backoff": 0.02, "max_backoff": 0.05})
    BACKEND.pin["flaky"] = "opus_overloaded"
    r2 = h2.call("opus_overloaded", model="claude-opus-5")
    check("below the threshold it keeps hammering the original model", h2.stats.model_switches == 0)
    check("every attempt carried the original model",
          _models_sent(r2.case) == ["claude-opus-5"] * 3, _models_sent(r2.case))
    check("fails honestly once max_attempts is used up", r2.status == 503, r2.status)
    check("the failure text does not mention a downgrade (there was none)",
          "models tried" not in (r2.json() or {}).get("error", {}).get("message", ""),
          r2.body[:200])
    BACKEND.pin.clear()


def t31_fallback_rejected_keeps_going():
    section("31. The fallback model is rejected too: keep going, do not blame the client")
    h = Harness(model_fallback={"chains": {
        "claude-opus-5": ["claude-mid-9", "claude-sonnet-5"]}})
    BACKEND.pin["flaky"] = "mid_rejected"
    r = h.call("mid_rejected", model="claude-opus-5")
    check("walked down to a usable rung and completed", r.status == 200 and r.text == FULL_TEXT, r.status)
    check("two model switches", h.stats.model_switches == 2, h.stats.model_switches)
    check("the middle rung's 400 was not handed to the client", r.status != 400)
    check("the trail is complete", r.headers.get("x-airelay-model-trail") ==
          "claude-opus-5 -> claude-mid-9 -> claude-sonnet-5",
          r.headers.get("x-airelay-model-trail"))
    check("a three-model trail is still latin-1 encodable", _latin1_clean(r.headers), r.headers)
    check("nothing counted as failed", h.stats.failed == 0)
    BACKEND.pin.clear()

    # the reverse: a genuine client 400 still passes through, downgrades enabled or not
    h2 = Harness(model_fallback={"chains": {"claude-opus-5": ["claude-sonnet-5"]}})
    r2 = h2.call("bad_request", model="claude-opus-5")
    check("a real client 400 is still returned as 400", r2.status == 400, r2.status)
    check("400 does not trigger a model switch", h2.stats.model_switches == 0)
    check("400 is not mistaken for a missing model", h2.stats.model_unavailable == 0)
    check("upstream hit exactly once", BACKEND.count(r2.case) == 1, BACKEND.count(r2.case))
    check("error body verbatim",
          (r2.json() or {}).get("error", {}).get("type") == "invalid_request_error", r2.body)


def t32_chain_exhausted_and_switch_off():
    section("32. Chain exhausted: an actionable 404; with the switch off, exactly the old behaviour")
    h = Harness(upstreams=TWO_UPS,
                model_fallback={"chains": {"claude-opus-5": ["claude-sonnet-5"]}})
    BACKEND.pin["one"] = "no_model_at_all"
    BACKEND.pin["two"] = "no_model_at_all"
    r = h.call("no_model_at_all", model="claude-opus-5", streaming=False)
    check("404, not a 503 and not a hang", r.status == 404, r.status)
    body = r.json() or {}
    check("a valid Anthropic error body", body.get("type") == "error" and
          body.get("error", {}).get("type") == "not_found_error", r.body[:200])
    msg = body.get("error", {}).get("message", "")
    check("says clearly that airelay is speaking", "airelay" in msg, msg)
    check("lists every model tried (enough to fix the config)",
          "claude-opus-5" in msg and "claude-sonnet-5" in msg, msg)
    check("includes the last upstream message verbatim", "does not exist" in msg, msg)
    check("2 channels x 2 models = 4 attempts", BACKEND.count(r.case) == 4, BACKEND.count(r.case))
    check("even a total wipeout cools no channel (a missing model is never a channel fault)",
          h.up("one").available() and h.up("two").available() and
          h.up("one").fail_count == 0)
    check("one failure counted", h.stats.failed == 1)
    check("one model switch", h.stats.model_switches == 1)
    BACKEND.pin.clear()

    # switch off: 404 goes back to rotate semantics (cool the channel), and models are never substituted
    off = Harness(model_fallback={"enabled": False,
                                  "chains": {"claude-opus-5": ["claude-sonnet-5"]}},
                  retry={"max_attempts": 3, "initial_backoff": 0.02, "max_backoff": 0.05})
    BACKEND.pin["flaky"] = "no_opus"
    r2 = off.call("no_opus", model="claude-opus-5")
    check("with it off, your model is never substituted",
          _models_sent(r2.case) == ["claude-opus-5"] * 3, _models_sent(r2.case))
    check("with it off, no model-switch stats at all",
          off.stats.model_switches == 0 and off.stats.model_unavailable == 0)
    check("with it off, 404 still cools the channel like a credential problem (1.0.0 behaviour)",
          not off.up("flaky").available(), off.up("flaky").cooldown_until)
    check("with it off, the end result is 503 rather than 404", r2.status == 503, r2.status)
    BACKEND.pin.clear()


def t33_missing_model_bookkeeping():
    section("33. Lifecycle of a missing-model record, and routing preference")
    h = Harness(upstreams=TWO_UPS)
    u1, u2 = h.up("one"), h.up("two")

    u1.note_model_missing("m", 300, "no such model")
    check("after marking, it is assumed not to have the model", not u1.has_model("m"))
    check("other models unaffected", u1.has_model("other"))
    check("other channels unaffected", u2.has_model("m"))
    for _ in range(5):
        u1.note_model_missing("m", 300, "no such model")
    check("marking never trips the breaker", u1.available() and u1.consec_fail == 0)
    check("marking is never counted as a failure", u1.fail_count == 0)
    check("last_error shows which model is missing", "missing model m" in (u1.last_error or ""), u1.last_error)

    check("the pool knows who still has the model",
          [u.name for u in h.pool.channels_with_model("m")] == ["two"])
    check("requests without a model are unaffected",
          len(h.pool.channels_with_model(None)) == 2)
    check("routing sorts channels lacking the model last", h.pool.choose(set(), "m")[0].name == "two")
    check("with no model, sticky_primary applies as usual", h.pool.choose(set(), None)[0].name == "one")
    u2.note_model_missing("m", 300, "nope")
    check("all lacking -> empty list (time to switch models)", h.pool.channels_with_model("m") == [])
    check("still returns a channel when all lack it, instead of crashing", h.pool.choose(set(), "m")[0] is not None)

    u1.note_model_missing("m2", 5, "gone")
    check("no second chance before expiry", not u1.has_model("m2", now=airelay._now() + 4))
    check("a second chance after expiry (the channel may have added it since)",
          u1.has_model("m2", now=airelay._now() + 6))
    check("expired records are cleaned up", "m2" not in u1.snapshot()["missing_models"],
          u1.snapshot()["missing_models"])
    u1.note_model_missing("m3", 0.001, "x")
    check("the TTL has a floor, so it never degenerates to single-use",
          not u1.has_model("m3", now=airelay._now() + 0.5))
    u1.note_model_missing(None, 300, "x")
    check("a None model writes no junk record",
          None not in u1.snapshot()["missing_models"], u1.snapshot()["missing_models"])

    # a model chosen by downgrade still goes through the channel's own model_map
    h3 = Harness(upstreams=[{"name": "mm", "base_url": "http://up1.invalid", "auth": "k",
                             "model_map": {"claude-sonnet-5": "sonnet-latest"}}])
    ex = airelay.Exchange("rid", "POST", "/v1/messages", {},
                          b'{"model":"claude-opus-5","max_tokens":8}', False, None)
    out = h3.router._body_for(ex, h3.up("mm"), "claude-sonnet-5")
    check("the downgraded model also goes through model_map", json.loads(out)["model"] == "sonnet-latest", out)
    same = h3.router._body_for(ex, h3.up("mm"), "claude-opus-5")
    check("body untouched when no model changed (no pointless re-serialising)", same is ex.body)

    # the startup banner has to lay out the chain (README quotes this verbatim)
    on = "\n".join(airelay.build_banner(
        Harness(model_fallback={"chains": {"claude-opus-5": ["claude-sonnet-5"], "*": []}}).cfg))
    check("the banner lists the chain", "claude-opus-5 -> claude-sonnet-5" in on, on)
    check("the banner prints no plaintext key", "key-one" not in on, on)
    off_banner = "\n".join(airelay.build_banner(
        Harness(model_fallback={"enabled": False}).cfg))
    check("with it off, the banner says so outright", "disabled" in off_banner, off_banner)
    plain = "\n".join(airelay.build_banner(Harness().cfg))
    check("with no chains the banner stays quiet about fallback", "model fallback" not in plain, plain)


TESTS = [t01_baseline, t02_status_retry, t03_sse_error_event, t04_truncate_pre_content,
         t05_truncate_mid_gated_vs_buffered, t06_fake_200, t07_network_flap, t08_retry_after,
         t09_rotation_and_breaker, t10_sticky_primary, t11_fatal_passthrough,
         t12_exhausted_paths, t13_invisible_long_retry, t14_client_disconnect,
         t15_headers_and_auth, t16_model_map_and_prefix, t17_other_paths, t18_classifiers,
         t19_secrets, t20_config_guard, t21_stats_and_snapshot, t22_example_config,
         t23_http_layer, t24_is_streaming, t25_cli,
         t26_model_failure_classifier, t27_model_chain_config, t28_missing_model_rotates,
         t29_all_channels_missing_switches_model, t30_switch_after_repeated_failures,
         t31_fallback_rejected_keeps_going, t32_chain_exhausted_and_switch_off,
         t33_missing_model_bookkeeping]


def main():
    print("airelay %s logic self-test (no network, no listening socket)" % airelay.VERSION)
    t0 = time.monotonic()
    for fn in TESTS:
        BACKEND.reset()
        try:
            fn()
        except Exception as exc:
            import traceback
            FAILS.append("%s raised: %s" % (fn.__name__, exc))
            print("  ✗ %s raised" % fn.__name__)
            traceback.print_exc()
    dt = time.monotonic() - t0
    print("\n" + "-" * 60)
    if FAILS:
        print("%d of %d assertions failed in %.1fs" % (len(FAILS), CHECKS, dt))
        for f in FAILS:
            print("  - " + f)
        return 1
    print("all green: %d assertions in %.1fs" % (CHECKS, dt))
    print("end-to-end (real sockets): python3 selftest_e2e.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
