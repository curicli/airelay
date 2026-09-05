#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
airelay — a local Anthropic-compatible retry / failover reverse proxy.

Point any Anthropic-compatible client (Claude Code, an SDK, your own scripts) at
this proxy through ANTHROPIC_BASE_URL. Relay flakiness — 5xx, timeouts, streams that
die mid-flight, "200 OK" bodies that are really errors — is swallowed here and
retried automatically. The client never sees it, and never needs a manual try again.

Usage:
    python3 airelay.py serve  [--config config.json] [--port 8787]
    python3 airelay.py doctor [--config config.json]   # health-check every upstream
    python3 airelay.py stats  [--port 8787]            # health of a running proxy

Zero third-party dependencies, stdlib only. Design tradeoffs live in README.md.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import random
import re
import socket
import ssl
import sys
import threading
import time
import uuid
from datetime import timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

VERSION = "1.1.0"
DEFAULT_ANTHROPIC_VERSION = "2023-06-01"

# ---------------------------------------------------------------- constants / classification

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade",
}
STRIP_REQUEST_HEADERS = HOP_BY_HOP | {"host", "content-length", "accept-encoding", "expect"}
AUTH_HEADERS = {"x-api-key", "authorization"}
SKIP_RESPONSE_HEADERS = HOP_BY_HOP | {"content-length"}

# Status codes worth retrying in place (Cloudflare's and relays' own 5xx included).
RETRYABLE_STATUS = {
    408, 409, 425, 429,
    500, 502, 503, 504, 507, 508,
    520, 521, 522, 523, 524, 525, 526, 527, 529, 530,
}
# The upstream itself is the problem (credential / model / balance). Retrying in
# place is pointless; rotate to another channel.
ROTATE_STATUS = {401, 402, 403, 404, 407, 418}
# The request itself is invalid. Rotating cannot help; hand it back to the client.
FATAL_STATUS = {400, 405, 406, 411, 412, 413, 414, 415, 416, 422, 431}

# Anthropic error type -> is it worth retrying?
RETRYABLE_ERROR_TYPES = {
    "overloaded_error", "api_error", "rate_limit_error", "timeout_error",
    "upstream_error", "internal_server_error", "service_unavailable",
}
ROTATE_ERROR_TYPES = {
    "authentication_error", "permission_error", "not_found_error",
    "billing_error", "insufficient_quota",
}

# Every relay words "this channel does not have that model" differently. A hint match
# plus a mention of the model means model-level failure, not channel-level failure.
# The CJK entries below are deliberate: Chinese relays answer in Chinese, and these
# are match patterns against upstream wire text, never user-facing strings.
MODEL_MISSING_HINTS = (
    "not found", "not_found", "notfound", "does not exist", "not exist",
    "no such", "not support", "unsupported", "not available", "unavailable",
    "unknown model", "invalid model", "model_not_found", "no access",
    "not allowed", "no permission", "deprecated", "decommissioned", "retired",
    "不存在", "不支持", "无权", "未找到", "没有找到", "没有权限", "已下线", "已废弃",
)

# Same idea for the bare word "model": a Chinese error message counts as mentioning
# the model. Wire-format data, not UI text.
MODEL_WORD_CJK = "模型"

# SSE: once one of these arrives the model really is producing output — open the gate.
GATE_OPEN_EVENTS = {"content_block_delta", "message_delta", "message_stop"}
PING_EVENT = b'event: ping\ndata: {"type": "ping"}\n\n'

PASSTHROUGH = object()  # auth sentinel: forward whatever credential the client sent


class ConfigError(Exception):
    pass


class ClientGone(Exception):
    """The client hung up. Stop retrying and pack up."""


class UpstreamFailure(Exception):
    """
    kind:
      retry         — transient; back off and retry (same channel or another)
      rotate        — this channel itself is broken; cool it down and move on
      model         — this channel does not have this model. The channel itself is
                      fine, so never trip its breaker: look for the same model on
                      another channel first, then walk the model_fallback chain
      fatal         — the request is invalid; hand it back to the client as-is
      unrecoverable — content already reached the client; no silent retry possible
    """

    def __init__(self, kind, detail, *, status=None, headers=None, body=None, retry_after=None):
        super().__init__(detail)
        self.kind = kind
        self.detail = detail
        self.status = status
        self.headers = headers or []
        self.body = body or b""
        self.retry_after = retry_after

    def __str__(self):
        s = f"[{self.kind}] {self.detail}"
        if self.status:
            s = f"[{self.kind}] HTTP {self.status}: {self.detail}"
        return s


# ---------------------------------------------------------------- small helpers


def _now() -> float:
    return time.monotonic()


def resolve_secret(raw):
    """Accepts "passthrough" / "${ENV_VAR}" / "file:/path/to/key" / a literal secret."""
    if raw is None:
        return PASSTHROUGH
    if not isinstance(raw, str):
        raise ConfigError(f"auth must be a string, got {type(raw).__name__}")
    raw = raw.strip()
    if raw == "" or raw.lower() == "passthrough":
        return PASSTHROUGH
    if raw.startswith("file:"):
        path = os.path.expanduser(raw[5:])
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError as exc:
            raise ConfigError(f"cannot read key file {path}: {exc}") from exc

    def sub(match):
        name = match.group(1)
        val = os.environ.get(name)
        if val is None:
            raise ConfigError(f"environment variable {name} is not set (config says ${{{name}}})")
        return val

    return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", sub, raw)


def redact(secret) -> str:
    if secret is PASSTHROUGH:
        return "<passthrough>"
    if not secret:
        return "<empty>"
    return secret[:6] + "…" + secret[-4:] if len(secret) > 12 else "…"


def header_safe(value) -> str:
    """
    http.server encodes response headers with latin-1/strict, so one non-ASCII character
    in a header value raises UnicodeEncodeError and kills the response. Channel names and
    model names come from config, so they are not ours to trust: transliterate anything
    latin-1 cannot hold rather than crash the request over a decorative character.
    """
    text = str(value)
    try:
        text.encode("latin-1", "strict")
        return text
    except UnicodeEncodeError:
        return text.encode("ascii", "replace").decode("ascii")


def parse_retry_after(value):
    """Retry-After is either seconds or an HTTP-date. Relays behind Cloudflare send both."""
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, when.timestamp() - time.time())


def split_sse_event(buf: bytes):
    """Cut one whole SSE event (blank line included) out of buf -> (event|None, rest)."""
    i = buf.find(b"\n\n")
    j = buf.find(b"\r\n\r\n")
    if i == -1 and j == -1:
        return None, buf
    if i == -1 or (j != -1 and j < i):
        return buf[: j + 4], buf[j + 4:]
    return buf[: i + 2], buf[i + 2:]


def sse_event_type(raw: bytes) -> str:
    for line in raw.split(b"\n"):
        line = line.strip()
        if line.lower().startswith(b"event:"):
            return line[6:].strip().decode("latin-1")
    return ""


def sse_event_data(raw: bytes) -> str:
    parts = []
    for line in raw.split(b"\n"):
        stripped = line.strip()
        if stripped.lower().startswith(b"data:"):
            parts.append(stripped[5:].strip().decode("utf-8", "replace"))
    return "\n".join(parts)


def classify_status(status: int) -> str:
    if status in RETRYABLE_STATUS:
        return "retry"
    if status in ROTATE_STATUS:
        return "rotate"
    if status in FATAL_STATUS:
        return "fatal"
    if status >= 500:
        return "retry"
    return "fatal"


def classify_error_payload(text: str, default: str = "retry") -> tuple[str, str]:
    """Work out the kind from an Anthropic-style error body. Returns (kind, message)."""
    etype, message = "", ""
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            err = obj.get("error")
            if isinstance(err, dict):
                etype = str(err.get("type") or "")
                message = str(err.get("message") or "")
            elif isinstance(err, str):
                message = err
            message = message or str(obj.get("message") or "")
    except (ValueError, TypeError):
        message = text[:400]
    if etype in RETRYABLE_ERROR_TYPES:
        return "retry", message or etype
    if etype in ROTATE_ERROR_TYPES:
        return "rotate", message or etype
    if etype == "invalid_request_error":
        return "fatal", message or etype
    return default, message or etype or text[:400]


def detect_json_error(body: bytes):
    """Some relays answer 200 + {"type":"error"}. Returns (kind, message) or None."""
    head = body[:2048].lstrip()
    if not head.startswith(b"{") or b'"error"' not in body[:4096]:
        return None
    try:
        obj = json.loads(body.decode("utf-8", "replace"))
    except (ValueError, UnicodeDecodeError):
        return None
    if isinstance(obj, dict) and obj.get("type") == "error":
        return classify_error_payload(body.decode("utf-8", "replace"))
    return None


def looks_model_unavailable(status, message: str, model=None) -> bool:
    """
    Is this error "this channel does not have the model you asked for" rather than
    "this channel is broken"?

    The distinction matters: a broken channel should be cooled down and rotated away
    from, a missing model should be swapped. Cooling down a healthy channel merely
    because it lacks one model throws away a channel that works.
    """
    if status is not None and status not in (400, 403, 404, 422, 501):
        return False
    text = message or ""
    low = text.lower()
    mentions = "model" in low or MODEL_WORD_CJK in text
    if model and model.lower() in low:
        mentions = True
    if not mentions:
        return False
    if any(hint in low for hint in MODEL_MISSING_HINTS):
        return True
    # 404 plus a mention of the model: realistically nothing else it could be
    return status == 404


def extract_model(body: bytes):
    """Read the model field out of a request body. None means the request has no model."""
    if not body:
        return None
    try:
        obj = json.loads(body)
    except (ValueError, UnicodeDecodeError, TypeError):
        return None
    if isinstance(obj, dict):
        model = obj.get("model")
        if isinstance(model, str) and model:
            return model
    return None


def replace_model(body: bytes, model: str):
    """Swap the model in a request body. None on failure (the caller keeps the original)."""
    try:
        obj = json.loads(body)
    except (ValueError, UnicodeDecodeError, TypeError):
        return None
    if not isinstance(obj, dict) or "model" not in obj:
        return None
    obj["model"] = model
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------- config


class Config:
    def __init__(self, raw: dict, source: str = "<defaults>"):
        self.source = source
        listen = raw.get("listen") or {}
        self.host = listen.get("host", "127.0.0.1")
        self.port = int(listen.get("port", 8787))

        retry = raw.get("retry") or {}
        self.max_attempts = int(retry.get("max_attempts", 30))
        self.initial_backoff = float(retry.get("initial_backoff", 1.0))
        self.max_backoff = float(retry.get("max_backoff", 20.0))
        self.backoff_multiplier = float(retry.get("backoff_multiplier", 2.0))
        self.jitter = float(retry.get("jitter", 0.5))
        self.respect_retry_after = bool(retry.get("respect_retry_after", True))
        self.max_retry_after = float(retry.get("max_retry_after", 120.0))

        t = raw.get("timeouts") or {}
        self.connect_timeout = float(t.get("connect", 15.0))
        self.read_timeout = float(t.get("read", 600.0))
        self.stream_idle_timeout = float(t.get("stream_idle", 120.0))
        self.total_timeout = float(t.get("total", 1800.0))

        s = raw.get("stream") or {}
        self.stream_mode = (s.get("mode") or raw.get("stream_mode") or "gated").lower()
        if self.stream_mode not in ("gated", "buffered"):
            raise ConfigError('stream.mode must be "gated" or "buffered"')
        self.keepalive_interval = float(s.get("keepalive_interval", 15.0))
        self.optimistic_after = float(s.get("optimistic_commit_after", 8.0))
        self.max_gate_wait = float(s.get("max_gate_wait", 300.0))
        self.max_buffer_bytes = int(s.get("max_buffer_bytes", 16 * 1024 * 1024))

        b = raw.get("breaker") or {}
        self.failure_threshold = int(b.get("failure_threshold", 3))
        self.cooldown_seconds = float(b.get("cooldown_seconds", 20.0))
        self.max_cooldown_seconds = float(b.get("max_cooldown_seconds", 300.0))
        self.rotate_cooldown_seconds = float(b.get("rotate_cooldown_seconds", 90.0))

        self.sticky_primary = bool(raw.get("sticky_primary", True))
        self.probe_model = raw.get("probe_model", "claude-opus-5")

        mf = raw.get("model_fallback") or {}
        self.model_fallback = bool(mf.get("enabled", True))
        self.model_switch_after = max(1, int(mf.get("switch_after_attempts", 4)))
        self.model_unavailable_ttl = float(mf.get("unavailable_ttl_seconds", 600.0))
        self.model_chains = {}
        for key, chain in (mf.get("chains") or {}).items():
            if isinstance(chain, str):
                chain = [chain]
            if not isinstance(chain, list):
                raise ConfigError(f"model_fallback.chains[{key}] must be an array")
            seen, clean = set(), []
            for name in chain:
                name = str(name).strip()
                # A model must never appear in its own chain, or it spins in place
                if not name or name == key or name in seen:
                    continue
                seen.add(name)
                clean.append(name)
            self.model_chains[str(key)] = clean

        log = raw.get("log") or {}
        self.log_file = log.get("file", "logs/airelay.jsonl")
        self.log_echo = bool(log.get("echo", True))
        self.log_bodies = bool(log.get("bodies", False))

        specs = raw.get("upstreams") or []
        if not specs:
            raise ConfigError("the config needs at least one upstreams entry")
        self.upstreams = []
        for idx, spec in enumerate(specs):
            # Skip enabled:false channels entirely: placeholder base_urls and unset
            # ${ENV_VARS} left over in the template must not stop the proxy booting.
            if isinstance(spec, dict) and not bool(spec.get("enabled", True)):
                continue
            self.upstreams.append(Upstream(spec, idx))
        if not self.upstreams:
            raise ConfigError("every upstreams entry is turned off with enabled:false")

    def model_chain(self, model):
        """Fallback chain for one model. Falls back to the "*" default chain."""
        if not self.model_fallback or not model:
            return []
        chain = self.model_chains.get(model)
        if chain is None:
            chain = [m for m in self.model_chains.get("*", []) if m != model]
        return chain

    def all_models(self):
        """Every model name the config mentions (doctor's model matrix needs it)."""
        out = []
        for key, chain in self.model_chains.items():
            for name in ([key] if key != "*" else []) + chain:
                if name not in out:
                    out.append(name)
        return out

    @classmethod
    def load(cls, path):
        path = os.path.abspath(os.path.expanduser(path))
        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except FileNotFoundError as exc:
            raise ConfigError(
                f"config not found: {path}\nCopy the template first: cp config.example.json config.json"
            ) from exc
        except ValueError as exc:
            raise ConfigError(f"{path} is not valid JSON: {exc}") from exc
        cfg = cls(raw, source=path)
        base = os.path.dirname(path)
        if cfg.log_file and not os.path.isabs(os.path.expanduser(cfg.log_file)):
            cfg.log_file = os.path.join(base, cfg.log_file)
        return cfg


class Upstream:
    def __init__(self, spec: dict, index: int):
        if not isinstance(spec, dict):
            raise ConfigError("every upstreams entry must be an object")
        self.index = index
        self.name = str(spec.get("name") or f"upstream{index + 1}")
        raw_url = str(spec.get("base_url") or "").strip()
        if not raw_url:
            raise ConfigError(f"upstream {self.name} is missing base_url")
        parts = urlsplit(raw_url if "://" in raw_url else "https://" + raw_url)
        if parts.scheme not in ("http", "https"):
            raise ConfigError(f"upstream {self.name}: unsupported base_url scheme: {parts.scheme}")
        if not parts.hostname:
            raise ConfigError(f"upstream {self.name}: cannot parse a hostname out of base_url")
        self.scheme = parts.scheme
        self.host = parts.hostname
        self.port = parts.port or (443 if parts.scheme == "https" else 80)
        self.prefix = parts.path.rstrip("/")
        self.auth = resolve_secret(spec.get("auth", "passthrough"))
        self.auth_style = str(spec.get("auth_style", "auto")).lower()
        self.verify_tls = bool(spec.get("verify_tls", True))
        self.enabled = bool(spec.get("enabled", True))
        self.extra_headers = dict(spec.get("headers") or {})
        self.model_map = dict(spec.get("model_map") or {})
        self.note = str(spec.get("note") or "")

        self.lock = threading.Lock()
        self.consec_fail = 0
        self.cooldown_until = 0.0
        self.ok_count = 0
        self.fail_count = 0
        self.ewma_ms = None
        self.last_error = ""
        self.last_error_at = 0.0
        self.last_ok_at = 0.0
        self.missing_models = {}   # model name -> when "this channel lacks it" expires

    # --- network ---

    def join(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return (self.prefix + path) if self.prefix else path

    def effective_auth_style(self) -> str:
        if self.auth_style in ("bearer", "x-api-key"):
            return self.auth_style
        key = self.auth if isinstance(self.auth, str) else ""
        if key.startswith(("sk-ant-oat", "sk-ant-ort")):
            return "bearer"
        if key.startswith("sk-ant-api"):
            return "x-api-key"
        if self.host.endswith("anthropic.com"):
            return "x-api-key"
        return "bearer"  # nearly every relay accepts Authorization: Bearer

    def connect(self, cfg: Config):
        try:
            if self.scheme == "https":
                ctx = ssl.create_default_context()
                if not self.verify_tls:
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                conn = http.client.HTTPSConnection(
                    self.host, self.port, timeout=cfg.connect_timeout, context=ctx
                )
            else:
                conn = http.client.HTTPConnection(
                    self.host, self.port, timeout=cfg.connect_timeout
                )
            conn.connect()
            return conn
        except (socket.timeout, TimeoutError) as exc:
            raise UpstreamFailure("retry", f"connect timed out: {exc}") from exc
        except (socket.gaierror, ssl.SSLError, OSError) as exc:
            raise UpstreamFailure("retry", f"connect failed: {exc}") from exc

    # --- health ---

    def available(self, now=None) -> bool:
        return (now or _now()) >= self.cooldown_until

    def has_model(self, model, now=None) -> bool:
        """Does this channel still believe it has this model? (records expire and retry)"""
        if not model:
            return True
        now = now or _now()
        with self.lock:
            until = self.missing_models.get(model)
            if until is None:
                return True
            if now >= until:
                del self.missing_models[model]
                return True
            return False

    def note_model_missing(self, model, ttl: float, detail: str):
        """
        "This channel does not have this model" is not a channel failure and must
        never trip the breaker: cooling down a channel that merely lacks opus throws
        away a channel that otherwise works perfectly.
        """
        if not model:
            return
        with self.lock:
            self.missing_models[model] = _now() + max(1.0, ttl)
            self.last_error = f"missing model {model}: {detail}"[:400]
            self.last_error_at = time.time()

    def note_success(self, elapsed_ms: float):
        with self.lock:
            self.consec_fail = 0
            self.cooldown_until = 0.0
            self.ok_count += 1
            self.last_ok_at = time.time()
            self.ewma_ms = elapsed_ms if self.ewma_ms is None else 0.7 * self.ewma_ms + 0.3 * elapsed_ms

    def note_failure(self, err: UpstreamFailure, cfg: Config):
        with self.lock:
            self.consec_fail += 1
            self.fail_count += 1
            self.last_error = str(err)
            self.last_error_at = time.time()
            if err.kind == "rotate":
                cool = max(cfg.cooldown_seconds, cfg.rotate_cooldown_seconds)
            elif self.consec_fail >= cfg.failure_threshold:
                steps = self.consec_fail - cfg.failure_threshold
                cool = min(cfg.max_cooldown_seconds, cfg.cooldown_seconds * (2 ** steps))
            else:
                cool = 0.0
            if cool > 0:
                self.cooldown_until = _now() + cool
            return cool

    def snapshot(self) -> dict:
        now = _now()
        with self.lock:
            missing = sorted(m for m, until in self.missing_models.items() if now < until)
            return {
                "name": self.name,
                "base_url": f"{self.scheme}://{self.host}:{self.port}{self.prefix}",
                "auth": redact(self.auth),
                "auth_style": self.effective_auth_style(),
                "healthy": now >= self.cooldown_until,
                "cooldown_remaining_s": round(max(0.0, self.cooldown_until - now), 1),
                "consecutive_failures": self.consec_fail,
                "ok": self.ok_count,
                "failed": self.fail_count,
                "avg_latency_ms": round(self.ewma_ms, 1) if self.ewma_ms else None,
                "missing_models": missing,
                "last_error": self.last_error,
                "last_error_at": _iso(self.last_error_at),
                "last_ok_at": _iso(self.last_ok_at),
            }


def _iso(ts: float):
    if not ts:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ts))


# ---------------------------------------------------------------- logging / stats


class Logger:
    def __init__(self, path, echo=True):
        self.echo = echo
        self.lock = threading.Lock()
        self.fh = None
        if path:
            path = os.path.abspath(os.path.expanduser(path))
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                self.fh = open(path, "a", encoding="utf-8")
                self.path = path
            except OSError as exc:
                sys.stderr.write(f"[airelay] cannot open the log file ({exc}); stderr only\n")
                self.path = None
        else:
            self.path = None

    def event(self, kind, msg="", **fields):
        rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "kind": kind, "msg": msg}
        rec.update(fields)
        line = json.dumps(rec, ensure_ascii=False)
        with self.lock:
            if self.fh:
                try:
                    self.fh.write(line + "\n")
                    self.fh.flush()
                except OSError:
                    pass
            if self.echo:
                sys.stderr.write(self._human(rec) + "\n")
                sys.stderr.flush()

    @staticmethod
    def _human(rec) -> str:
        ts = rec["ts"].split("T")[-1]
        rid = rec.get("rid", "-")
        bits = [f"[{ts}] {rid} {rec['kind']}"]
        for key in ("method", "path", "upstream", "attempt", "status", "latency_ms",
                    "sleep_s", "cooldown_s", "attempts", "recovered"):
            if rec.get(key) is not None:
                bits.append(f"{key}={rec[key]}")
        if rec.get("msg"):
            bits.append("· " + str(rec["msg"])[:300])
        return " ".join(bits)


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.started_at = time.time()
        self.requests = 0
        self.succeeded = 0
        self.failed = 0
        self.retries = 0
        self.rotations = 0
        self.invisible_recoveries = 0     # succeeded after retry, client saw no error
        self.unrecoverable_midstream = 0  # stream died after content shipped: unfixable
        self.client_disconnects = 0
        self.model_switches = 0           # how often we walked down a fallback chain
        self.model_unavailable = 0        # "this channel lacks this model" detections
        self.served_on_fallback = 0       # requests finished on a substituted model

    def bump(self, field, n=1):
        with self.lock:
            setattr(self, field, getattr(self, field) + n)

    def snapshot(self) -> dict:
        with self.lock:
            up = time.time() - self.started_at
            return {
                "uptime_seconds": int(up),
                "started_at": _iso(self.started_at),
                "requests": self.requests,
                "succeeded": self.succeeded,
                "failed": self.failed,
                "retries": self.retries,
                "channel_rotations": self.rotations,
                "invisible_recoveries": self.invisible_recoveries,
                "unrecoverable_midstream": self.unrecoverable_midstream,
                "client_disconnects": self.client_disconnects,
                "model_switches": self.model_switches,
                "model_unavailable": self.model_unavailable,
                "served_on_fallback": self.served_on_fallback,
            }


# ---------------------------------------------------------------- upstream pool


class Pool:
    def __init__(self, cfg: Config, log: Logger):
        self.cfg = cfg
        self.log = log
        self.upstreams = cfg.upstreams

    def choose(self, tried: set, model=None) -> tuple[Upstream, float]:
        """
        Returns (channel, seconds to wait first). Always returns a channel — when they
        are all cooling down, probe whichever recovers soonest.
        Channels known to lack the model sort last but are never excluded outright,
        because that record may already be stale.
        """
        now = _now()
        avail = [u for u in self.upstreams if u.available(now)]
        if avail and not self.cfg.sticky_primary:
            avail.sort(key=lambda u: (u.ewma_ms if u.ewma_ms is not None else 0.0, u.index))
        if model:
            avail.sort(key=lambda u: not u.has_model(model, now))
        fresh = [u for u in avail if u.name not in tried]
        if fresh:
            return fresh[0], 0.0
        if avail:
            return avail[0], 0.0
        soonest = min(self.upstreams, key=lambda u: u.cooldown_until)
        return soonest, max(0.0, soonest.cooldown_until - now)

    def channels_with_model(self, model):
        """Channels not flagged as lacking this model. Empty = time to switch models."""
        if not model:
            return list(self.upstreams)
        now = _now()
        return [u for u in self.upstreams if u.has_model(model, now)]

    def note_success(self, up: Upstream, elapsed_ms: float):
        up.note_success(elapsed_ms)

    def note_failure(self, up: Upstream, err: UpstreamFailure, rid: str) -> float:
        cool = up.note_failure(err, self.cfg)
        if cool:
            self.log.event("breaker_open", str(err), rid=rid, upstream=up.name, cooldown_s=round(cool, 1))
        return cool

    def snapshot(self):
        return [u.snapshot() for u in self.upstreams]


# ---------------------------------------------------------------- client writer


class ClientWriter:
    """The only way to the client socket. Every write is serialized (keepalive too)."""

    def __init__(self, handler: BaseHTTPRequestHandler):
        self.h = handler
        self.lock = threading.RLock()
        self.committed = False
        self.finished = False
        self.last_write = _now()
        self.bytes_out = 0

    # --- one-shot complete response (non-streaming / errors) ---

    def send_full(self, status: int, headers, body: bytes):
        with self.lock:
            if self.committed:
                raise RuntimeError("the response headers are already out; send_full is impossible")
            try:
                self.h.send_response(status)
                for key, val in headers:
                    if key.lower() in SKIP_RESPONSE_HEADERS:
                        continue
                    self.h.send_header(key, val)
                self.h.send_header("Content-Length", str(len(body)))
                self.h.end_headers()
                if body:
                    self.h.wfile.write(body)
                self.h.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError) as exc:
                raise ClientGone(str(exc)) from exc
            self.committed = True
            self.finished = True
            self.bytes_out += len(body)

    # --- streaming ---

    def commit_stream(self, status: int, headers):
        with self.lock:
            if self.committed:
                return
            try:
                self.h.send_response(status)
                sent = set()
                for key, val in headers:
                    lk = key.lower()
                    if lk in SKIP_RESPONSE_HEADERS or lk == "content-encoding":
                        continue
                    self.h.send_header(key, val)
                    sent.add(lk)
                if "content-type" not in sent:
                    self.h.send_header("Content-Type", "text/event-stream; charset=utf-8")
                if "cache-control" not in sent:
                    self.h.send_header("Cache-Control", "no-cache, no-store")
                self.h.send_header("X-Accel-Buffering", "no")
                self.h.send_header("Transfer-Encoding", "chunked")
                self.h.end_headers()
                self.h.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError) as exc:
                raise ClientGone(str(exc)) from exc
            self.committed = True
            self.last_write = _now()

    def write_stream(self, data: bytes):
        if not data:
            return
        with self.lock:
            if self.finished:
                return
            self._raw_chunk(data)

    def write_if_idle(self, data: bytes, idle: float) -> bool:
        with self.lock:
            if self.finished or not self.committed:
                return False
            if _now() - self.last_write < idle:
                return False
            self._raw_chunk(data)
            return True

    def end_stream(self):
        with self.lock:
            if self.finished or not self.committed:
                return
            try:
                self.h.wfile.write(b"0\r\n\r\n")
                self.h.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError) as exc:
                raise ClientGone(str(exc)) from exc
            finally:
                self.finished = True

    def _raw_chunk(self, data: bytes):
        try:
            self.h.wfile.write(b"%x\r\n" % len(data) + data + b"\r\n")
            self.h.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError) as exc:
            self.finished = True
            raise ClientGone(str(exc)) from exc
        self.bytes_out += len(data)
        self.last_write = _now()


# ---------------------------------------------------------------- SSE forwarder


class SSEForwarder:
    """
    The heart of streaming. Two tricks make retries invisible to the client:

    1) Pass pings straight through, and generate keepalive pings of our own.
       A ping event carries no content, so every client ignores it. That means we can
       commit the response headers the moment we see a 200 and start emitting pings;
       the client's idle timer then never fires. Even while we are on retry #5 in the
       background, all the client sees is "still working".

    2) The gate.
       message_start / content_block_start — everything that arrives before actual
       content — is held in a buffer until the first content_block_delta. Until the
       gate opens the upstream can blow up any way it likes and we retry silently,
       because the client has not received one byte of content.
       buffered mode goes further: hold everything back until message_stop, so a
       failure at any point is retryable. The cost is no token-by-token streaming.
    """

    def __init__(self, writer: ClientWriter, cfg: Config, stats: Stats, log: Logger, rid: str):
        self.writer = writer
        self.cfg = cfg
        self.stats = stats
        self.log = log
        self.rid = rid
        self.mode = cfg.stream_mode
        self.pending = []           # events received but not yet released to the client
        self.pending_bytes = 0
        self.gate_open = False
        self.content_flushed = False
        self.saw_message_stop = False
        self.attempt_started = _now()
        self._done = threading.Event()
        self._keeper = None

    # --- lifecycle ---

    def begin_attempt(self, status, headers):
        """Called at the start of every upstream attempt. Once content is out, no retry."""
        if self.content_flushed:
            raise UpstreamFailure("unrecoverable", "content already sent; cannot retry")
        self.pending = []
        self.pending_bytes = 0
        self.gate_open = False
        self.saw_message_stop = False
        self.attempt_started = _now()
        self._commit(status, headers)

    def commit_optimistic(self):
        """Nothing has succeeded yet but retries are dragging on: ship 200 + SSE headers
        so pings can start flowing."""
        if self.writer.committed:
            return
        self._commit(200, [("Content-Type", "text/event-stream; charset=utf-8")])
        self.log.event("optimistic_commit",
                       "committed the response headers early to keep the client alive",
                       rid=self.rid)

    def _commit(self, status, headers):
        if self.writer.committed:
            return
        self.writer.commit_stream(status, list(headers))
        self._start_keepalive()

    def _start_keepalive(self):
        if self._keeper is not None or self.cfg.keepalive_interval <= 0:
            return
        self._keeper = threading.Thread(target=self._keepalive_loop, name=f"ka-{self.rid}", daemon=True)
        self._keeper.start()

    def _keepalive_loop(self):
        interval = self.cfg.keepalive_interval
        while not self._done.wait(min(1.0, interval / 2)):
            try:
                self.writer.write_if_idle(PING_EVENT, interval)
            except ClientGone:
                return
            except Exception:
                return

    # --- event handling ---

    def _failure_kind(self, base: str) -> str:
        return "unrecoverable" if self.content_flushed else base

    def handle_event(self, raw: bytes):
        etype = sse_event_type(raw)

        if etype == "ping":
            self.writer.write_stream(raw)   # lifeline signal: always release immediately
            return

        if etype == "error":
            data = sse_event_data(raw)
            kind, message = classify_error_payload(data)
            if self.content_flushed:
                # content is already out; all we can do is pass the error through
                self.writer.write_stream(raw)
                raise UpstreamFailure("unrecoverable", f"error mid-stream: {message}")
            raise UpstreamFailure(kind, f"SSE error event: {message}")

        if etype == "message_stop":
            self.saw_message_stop = True

        if self.mode == "buffered":
            self._buffer(raw)
            return

        if not self.gate_open:
            if etype in GATE_OPEN_EVENTS or _now() - self.attempt_started > self.cfg.max_gate_wait:
                self.gate_open = True
                self._flush(extra=raw)
                return
            self._buffer(raw)
            return

        self.writer.write_stream(raw)
        self.content_flushed = True

    def _buffer(self, raw: bytes):
        self.pending.append(raw)
        self.pending_bytes += len(raw)
        if self.pending_bytes >= self.cfg.max_buffer_bytes:
            self.log.event("buffer_overflow",
                           f"buffer grew past {self.cfg.max_buffer_bytes} bytes; forcing release",
                           rid=self.rid)
            self._flush()

    def _flush(self, extra: bytes = b""):
        payload = b"".join(self.pending)
        self.pending = []
        self.pending_bytes = 0
        if extra:
            payload += extra
        if payload:
            self.writer.write_stream(payload)
            self.content_flushed = True

    # --- pumping ---

    def pump(self, resp, deadline: float):
        buf = b""
        while True:
            if _now() > deadline:
                raise UpstreamFailure(self._failure_kind("retry"), "per-request time budget exceeded")
            try:
                chunk = resp.read1(65536)
            except (socket.timeout, TimeoutError) as exc:
                raise UpstreamFailure(self._failure_kind("retry"),
                                      f"stream idle for over {self.cfg.stream_idle_timeout}s: {exc}") from exc
            except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
                raise UpstreamFailure(self._failure_kind("retry"), f"stream broke: {exc}") from exc

            if not chunk:
                if self.saw_message_stop:
                    return
                raise UpstreamFailure(self._failure_kind("retry"),
                                      "upstream closed the connection before message_stop (truncated response)")
            buf += chunk
            while True:
                event, buf = split_sse_event(buf)
                if event is None:
                    break
                self.handle_event(event)

    # --- wrap-up ---

    def finish(self):
        self._flush()
        self._done.set()
        self.writer.end_stream()

    def abort(self, err: UpstreamFailure):
        """Out of retries. If the headers are already committed, emit a valid error
        event rather than leaving the client hanging."""
        self._done.set()
        if not self.writer.committed:
            return False
        detail = err.detail if isinstance(err, UpstreamFailure) else str(err)
        payload = json.dumps(
            {"type": "error",
             "error": {"type": "api_error",
                       "message": f"airelay: still failing after retrying every channel — {detail}"}},
            ensure_ascii=False)
        try:
            self.writer.write_stream(b"event: error\ndata: " + payload.encode("utf-8") + b"\n\n")
            self.writer.end_stream()
        except ClientGone:
            pass
        return True


# ---------------------------------------------------------------- one exchange


class Exchange:
    def __init__(self, rid, method, path, headers, body, streaming, writer):
        self.rid = rid
        self.method = method
        self.path = path
        self.req_headers = headers
        self.body = body
        self.streaming = streaming
        self.writer = writer
        self.sse = None
        self.start = _now()
        self.model = extract_model(body)   # None = this request has no model field


class ModelPlan:
    """
    Tracks which model this request should be using right now.

    idx == -1 means we are still on the model the client asked for, which is where the
    vast majority of requests both start and end. We only walk down the chain when
    that model is genuinely unobtainable (no channel has it, or it keeps failing).
    """

    def __init__(self, cfg: Config, requested):
        self.requested = requested
        self.chain = cfg.model_chain(requested)
        self.idx = -1
        self.fails = 0
        self.history = []       # [(model name, why we switched to it)]

    @property
    def current(self):
        return self.requested if self.idx < 0 else self.chain[self.idx]

    @property
    def substituted(self) -> bool:
        return self.idx >= 0

    def has_next(self) -> bool:
        return self.idx + 1 < len(self.chain)

    def advance(self, reason: str) -> bool:
        if not self.has_next():
            return False
        self.idx += 1
        self.fails = 0
        self.history.append((self.current, reason))
        return True

    def trail(self) -> str:
        return " -> ".join([str(self.requested)] + [m for m, _ in self.history])


# ---------------------------------------------------------------- routing / retry


class Router:
    def __init__(self, cfg: Config, pool: Pool, log: Logger, stats: Stats):
        self.cfg = cfg
        self.pool = pool
        self.log = log
        self.stats = stats

    # --- main loop ---

    def execute(self, ex: Exchange):
        cfg = self.cfg
        self.stats.bump("requests")
        start = _now()
        ex.start = start
        deadline = start + cfg.total_timeout
        if ex.streaming:
            ex.sse = SSEForwarder(ex.writer, cfg, self.stats, self.log, ex.rid)

        tried, attempts, last_err = set(), 0, None
        backoff = cfg.initial_backoff
        plan = ModelPlan(cfg, ex.model)
        model_err = None          # first "this channel lacks the model" error, kept in
                                  # case the chain runs out and the client needs an answer

        while attempts < cfg.max_attempts:
            now = _now()
            if now >= deadline:
                last_err = last_err or UpstreamFailure("retry", "total time budget exhausted")
                break

            up, wait = self.pool.choose(tried, plan.current)
            if wait > 0:
                wait = min(wait, cfg.max_backoff, max(0.0, deadline - now))
                if wait > 0:
                    self.log.event("wait_cooldown", f"every channel is cooling down; waiting {wait:.1f}s then probing {up.name}",
                                   rid=ex.rid, upstream=up.name, sleep_s=round(wait, 1))
                    self._sleep(ex, wait, deadline)

            attempts += 1
            tried.add(up.name)
            if attempts > 1:
                self.stats.bump("retries")
            if len(tried) > 1 and attempts > 1:
                self.stats.bump("rotations")

            t0 = _now()
            try:
                self._attempt(ex, up, deadline, attempts, plan)
            except ClientGone as exc:
                self.stats.bump("client_disconnects")
                self.log.event("client_gone", str(exc), rid=ex.rid, upstream=up.name, attempt=attempts)
                return
            except UpstreamFailure as err:
                latency = int((_now() - t0) * 1000)

                # Channel-level vs model-level failure: when a 404/400 says "no such
                # model", cooling this channel down is a pure loss — its other models
                # are perfectly fine.
                if cfg.model_fallback and plan.current and err.kind in ("rotate", "fatal") \
                        and looks_model_unavailable(err.status, err.detail, plan.current):
                    err.kind = "model"

                if err.kind == "model":
                    up.note_model_missing(plan.current, cfg.model_unavailable_ttl, err.detail)
                    self.stats.bump("model_unavailable")
                    model_err = model_err or err
                    self.log.event("model_missing",
                                   f"{up.name} does not have {plan.current}: {err.detail}",
                                   rid=ex.rid, upstream=up.name, attempt=attempts,
                                   status=err.status, model=plan.current, latency_ms=latency)
                    left = self.pool.channels_with_model(plan.current)
                    if left:
                        # Another channel claims to have it: rotate for the same model,
                        # no backoff and no downgrade needed.
                        continue
                    if plan.advance(f"no channel has {plan.current}"):
                        tried.clear()
                        backoff = cfg.initial_backoff
                        self.stats.bump("model_switches")
                        self.log.event("model_switch", f"retrying with another model: {plan.trail()}",
                                       rid=ex.rid, attempt=attempts, model=plan.current,
                                       requested=plan.requested)
                        continue
                    self.log.event("model_exhausted",
                                   f"chain exhausted; no channel provides any of {plan.trail()}",
                                   rid=ex.rid, attempts=attempts, requested=plan.requested)
                    # Relay 404 bodies tend to be vague, so say something actionable here
                    self._deliver_fatal(ex, UpstreamFailure(
                        "fatal",
                        f"airelay: tried {plan.trail()} — no channel provides any of them. "
                        f"Last upstream error: {model_err.detail}",
                        status=model_err.status or 404, headers=model_err.headers))
                    self.stats.bump("failed")
                    return

                self.pool.note_failure(up, err, ex.rid)
                last_err = err
                self.log.event("attempt_failed", str(err), rid=ex.rid, upstream=up.name,
                               attempt=attempts, status=err.status, latency_ms=latency,
                               model=plan.current)

                if err.kind == "fatal":
                    # If we substituted the model, a 400 is not the client's fault
                    if plan.substituted and plan.advance("the fallback model was rejected"):
                        tried.clear()
                        backoff = cfg.initial_backoff
                        self.stats.bump("model_switches")
                        self.log.event("model_switch", f"retrying with another model: {plan.trail()}",
                                       rid=ex.rid, attempt=attempts, model=plan.current)
                        continue
                    self._deliver_fatal(ex, model_err if plan.substituted and model_err else err)
                    self.stats.bump("failed")
                    return
                if err.kind == "unrecoverable":
                    self.stats.bump("unrecoverable_midstream")
                    self.stats.bump("failed")
                    self.log.event("unrecoverable",
                                   "the upstream cut the stream after content had already "
                                   "reached the client, so it cannot be retried silently; "
                                   "stream.mode=buffered avoids this entirely",
                                   rid=ex.rid, upstream=up.name, attempt=attempts)
                    if ex.sse:
                        ex.sse.abort(err)
                    return

                # The same model failing over and over usually means it really is down
                # (a site-wide opus overload, say). Moving to the next model gets the
                # task further than waiting this one out.
                plan.fails += 1
                if plan.fails >= cfg.model_switch_after and plan.has_next():
                    plan.advance(f"the same model failed {plan.fails} times in a row")
                    tried.clear()
                    backoff = cfg.initial_backoff
                    self.stats.bump("model_switches")
                    self.log.event("model_switch", f"retrying with another model: {plan.trail()}",
                                   rid=ex.rid, attempt=attempts, model=plan.current,
                                   requested=plan.requested)

                sleep_for = self._next_sleep(err, backoff)
                backoff = min(cfg.max_backoff, backoff * cfg.backoff_multiplier)
                remaining = deadline - _now()
                if remaining <= 0 or attempts >= cfg.max_attempts:
                    break
                sleep_for = min(sleep_for, remaining)
                self.log.event("retrying", f"retrying in {sleep_for:.1f}s (attempt {attempts + 1})",
                               rid=ex.rid, upstream=up.name, attempt=attempts,
                               sleep_s=round(sleep_for, 1))
                self._sleep(ex, sleep_for, deadline)
                continue
            else:
                latency = int((_now() - t0) * 1000)
                self.pool.note_success(up, latency)
                self.stats.bump("succeeded")
                if attempts > 1:
                    self.stats.bump("invisible_recoveries")
                if plan.substituted:
                    self.stats.bump("served_on_fallback")
                note = "" if attempts == 1 else f"succeeded after {attempts} attempts; the client never noticed"
                if plan.substituted:
                    note = (note + "; " if note else "") + f"served by fallback model {plan.trail()}"
                self.log.event("ok", note, rid=ex.rid, upstream=up.name, attempt=attempts,
                               latency_ms=latency, recovered=attempts > 1,
                               model=plan.current, fallback=plan.substituted)
                return

        # every attempt used up
        self.stats.bump("failed")
        self.log.event("exhausted", str(last_err) if last_err else "unknown",
                       rid=ex.rid, attempts=attempts, model=plan.current,
                       tried_models=plan.trail())
        self._deliver_exhausted(ex, last_err, attempts, plan)

    def _sleep(self, ex: Exchange, seconds: float, deadline: float):
        """
        Streaming clients have to keep receiving pings during backoff, or their own
        idle timer fires first. If retries drag past optimistic_commit_after seconds
        with nothing having succeeded, commit 200 + SSE headers early so the pings
        have somewhere to go. This is what makes retries invisible.
        """
        if seconds <= 0:
            return
        end = _now() + seconds
        while True:
            if ex.sse and not ex.writer.committed and \
                    _now() - ex.start >= self.cfg.optimistic_after:
                ex.sse.commit_optimistic()
            if ex.sse and ex.writer.committed:
                ex.writer.write_if_idle(PING_EVENT, self.cfg.keepalive_interval)
            left = end - _now()
            if left <= 0:
                return
            time.sleep(min(0.5, left))

    # --- a single upstream attempt ---

    def _attempt(self, ex: Exchange, up: Upstream, deadline: float, attempt_no: int, plan):
        body = self._body_for(ex, up, plan.current)
        headers = self._headers_for(ex, up)
        target = up.join(ex.path)

        conn = up.connect(self.cfg)
        try:
            try:
                conn.sock.settimeout(self.cfg.read_timeout)
                conn.request(ex.method, target, body=body or None, headers=headers)
                resp = conn.getresponse()
            except (socket.timeout, TimeoutError) as exc:
                raise UpstreamFailure("retry", f"timed out waiting for response headers: {exc}") from exc
            except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
                raise UpstreamFailure("retry", f"sending the request failed: {exc}") from exc

            status = resp.status
            resp_headers = resp.getheaders()
            retry_after = parse_retry_after(resp.getheader("retry-after"))

            if status >= 400:
                raw = self._read_body(resp, cap=256 * 1024)
                kind = classify_status(status)
                text = raw.decode("utf-8", "replace")
                sub_kind, message = classify_error_payload(text, default=kind)
                # Status code wins: 5xx is always retryable, however odd the body's type
                if kind == "retry":
                    sub_kind = "retry"
                raise UpstreamFailure(sub_kind, message or f"HTTP {status}",
                                      status=status, headers=resp_headers,
                                      body=raw, retry_after=retry_after)

            ctype = (resp.getheader("content-type") or "").lower()
            if ex.streaming and "text/event-stream" in ctype:
                conn.sock.settimeout(self.cfg.stream_idle_timeout)
                ex.sse.begin_attempt(status, list(resp_headers)
                                     + self._trace_headers(up, attempt_no, plan))
                ex.sse.pump(resp, deadline)
                ex.sse.finish()
                return

            raw = self._read_body(resp)
            if not raw and ex.path.rstrip("/").endswith("/v1/messages"):
                raise UpstreamFailure("retry", f"HTTP {status} with an empty body")
            err = detect_json_error(raw)
            if err:
                kind, message = err
                raise UpstreamFailure(kind, f"HTTP 200 but the body is an error: {message}",
                                      status=status, headers=resp_headers, body=raw)
            out_headers = list(resp_headers) + self._trace_headers(up, attempt_no, plan)
            ex.writer.send_full(status, out_headers, raw)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _trace_headers(self, up: Upstream, attempt_no: int, plan) -> list:
        """Tag the response with who served it and on which model, so a downgrade shows."""
        out = [("X-Airelay-Upstream", header_safe(up.name)), ("X-Airelay-Attempt", str(attempt_no))]
        if plan and plan.substituted:
            out.append(("X-Airelay-Model", header_safe(plan.current)))
            out.append(("X-Airelay-Model-Trail", header_safe(plan.trail())))
        return out

    def _read_body(self, resp, cap=64 * 1024 * 1024) -> bytes:
        out, total = [], 0
        while True:
            try:
                chunk = resp.read1(65536)
            except (socket.timeout, TimeoutError) as exc:
                raise UpstreamFailure("retry", f"timed out reading the body: {exc}") from exc
            except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
                raise UpstreamFailure("retry", f"reading the body failed: {exc}") from exc
            if not chunk:
                break
            out.append(chunk)
            total += len(chunk)
            if total > cap:
                raise UpstreamFailure("fatal", f"the body exceeds the {cap} byte cap")
        return b"".join(out)

    # --- request rewriting ---

    def _body_for(self, ex: Exchange, up: Upstream, model=None) -> bytes:
        """Apply the chain's model first, then the channel's own model_map, then send."""
        if not ex.body or not ex.model:
            return ex.body
        target = up.model_map.get(model or ex.model, model or ex.model)
        if target == ex.model:
            return ex.body
        return replace_model(ex.body, target) or ex.body

    def _headers_for(self, ex: Exchange, up: Upstream) -> dict:
        out = {}
        for key, val in ex.req_headers.items():
            lk = key.lower()
            if lk in STRIP_REQUEST_HEADERS or lk in AUTH_HEADERS:
                continue
            out[key] = val
        out["Accept-Encoding"] = "identity"
        if not any(k.lower() == "anthropic-version" for k in out):
            out["anthropic-version"] = DEFAULT_ANTHROPIC_VERSION

        if up.auth is PASSTHROUGH:
            for key, val in ex.req_headers.items():
                if key.lower() in AUTH_HEADERS:
                    out[key] = val
        else:
            if up.effective_auth_style() == "bearer":
                out["Authorization"] = f"Bearer {up.auth}"
            else:
                out["x-api-key"] = up.auth
        out.update(up.extra_headers)
        out["X-Airelay"] = VERSION
        return out

    # --- failure delivery ---

    def _deliver_fatal(self, ex: Exchange, err: UpstreamFailure):
        if ex.sse and ex.writer.committed:
            ex.sse.abort(err)
            return
        body = err.body or json.dumps(
            {"type": "error",
             "error": {"type": "not_found_error" if err.status == 404 else "invalid_request_error",
                       "message": err.detail}},
            ensure_ascii=False).encode("utf-8")
        headers = [(k, v) for k, v in (err.headers or []) if k.lower() != "content-encoding"]
        if not headers:
            headers = [("Content-Type", "application/json")]
        try:
            ex.writer.send_full(err.status or 400, headers, body)
        except (ClientGone, RuntimeError):
            pass

    def _deliver_exhausted(self, ex: Exchange, err, attempts: int, plan=None):
        detail = str(err) if err else "unknown error"
        if plan is not None and plan.substituted:
            detail += f" (models tried: {plan.trail()})"
        if ex.sse and ex.writer.committed:
            ex.sse.abort(err if isinstance(err, UpstreamFailure) else UpstreamFailure("retry", detail))
            return
        payload = json.dumps({
            "type": "error",
            "error": {
                "type": "api_error",
                "message": f"airelay: all {attempts} attempts failed, rotations included — {detail}",
            },
        }, ensure_ascii=False).encode("utf-8")
        try:
            ex.writer.send_full(503, [("Content-Type", "application/json")], payload)
        except (ClientGone, RuntimeError):
            pass

    def _next_sleep(self, err: UpstreamFailure, backoff: float) -> float:
        cfg = self.cfg
        if cfg.respect_retry_after and err.retry_after is not None:
            return min(err.retry_after, cfg.max_retry_after)
        jitter = cfg.jitter * backoff
        return max(0.05, backoff - jitter + random.random() * 2 * jitter)


# ---------------------------------------------------------------- HTTP server


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = f"airelay/{VERSION}"
    sys_version = ""

    # route through our own logger
    def log_message(self, fmt, *args):
        pass

    def log_error(self, fmt, *args):
        pass

    # --- method dispatch ---

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_PUT(self):
        self._dispatch("PUT")

    def do_PATCH(self):
        self._dispatch("PATCH")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def do_OPTIONS(self):
        self._dispatch("OPTIONS")

    def _dispatch(self, method):
        srv = self.server
        path = self.path

        if path.startswith("/__airelay"):
            self._admin(path, srv)
            return

        rid = "r" + uuid.uuid4().hex[:8]
        try:
            body = self._read_request_body()
        except ClientGone:
            return
        except Exception as exc:
            self._json(400, {"type": "error",
                             "error": {"type": "invalid_request_error",
                                       "message": f"airelay cannot read the request body: {exc}"}})
            return

        if self.headers.get("X-Airelay"):
            self._json(508, {"type": "error",
                             "error": {"type": "api_error",
                                       "message": "airelay: self-loop detected — some "
                                                  "upstream base_url points back at the proxy"}})
            return

        streaming = self._is_streaming(body)
        writer = ClientWriter(self)
        ex = Exchange(rid, method, path, dict(self.headers), body, streaming, writer)
        srv.log.event("request", "", rid=rid, method=method, path=path.split("?")[0])
        try:
            srv.router.execute(ex)
        except ClientGone:
            srv.stats.bump("client_disconnects")
        except Exception as exc:  # last resort: never let one request kill the thread
            srv.log.event("internal_error", repr(exc), rid=rid)
            if not writer.committed:
                try:
                    self._json(500, {"type": "error",
                                     "error": {"type": "api_error",
                                               "message": f"airelay internal error: {exc}"}})
                except Exception:
                    pass
        finally:
            if writer.committed and not writer.finished:
                try:
                    writer.end_stream()
                except Exception:
                    pass
            if not writer.committed:
                self.close_connection = True

    # --- admin endpoints ---

    def _admin(self, path, srv):
        if path.startswith("/__airelay/health"):
            self._json(200, {"status": "ok", "version": VERSION})
            return
        if path.startswith("/__airelay/stats"):
            self._json(200, {
                "version": VERSION,
                "config": srv.cfg.source,
                "stream_mode": srv.cfg.stream_mode,
                "model_fallback": {
                    "enabled": srv.cfg.model_fallback,
                    "switch_after_attempts": srv.cfg.model_switch_after,
                    "chains": srv.cfg.model_chains,
                },
                "totals": srv.stats.snapshot(),
                "upstreams": srv.pool.snapshot(),
            })
            return
        self._json(404, {"error": "unknown airelay endpoint"})

    def _json(self, status, obj):
        body = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    # --- request body ---

    def _read_request_body(self) -> bytes:
        te = (self.headers.get("Transfer-Encoding") or "").lower()
        try:
            if "chunked" in te:
                chunks = []
                while True:
                    line = self.rfile.readline(65536).strip()
                    if not line:
                        break
                    try:
                        size = int(line.split(b";")[0], 16)
                    except ValueError:
                        break
                    if size == 0:
                        while True:
                            trailer = self.rfile.readline(65536)
                            if trailer in (b"\r\n", b"\n", b""):
                                break
                        break
                    chunks.append(self.rfile.read(size))
                    self.rfile.read(2)
                return b"".join(chunks)
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length > 0 else b""
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise ClientGone(str(exc)) from exc

    @staticmethod
    def _is_streaming(body: bytes) -> bool:
        if not body:
            return False
        if b'"stream"' not in body[:8192] and b'"stream"' not in body[-4096:]:
            return False
        try:
            obj = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            return b'"stream":true' in body.replace(b" ", b"")
        return isinstance(obj, dict) and obj.get("stream") is True


class ProxyServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128

    def __init__(self, cfg: Config, log: Logger):
        super().__init__((cfg.host, cfg.port), Handler)
        self.cfg = cfg
        self.log = log
        self.stats = Stats()
        self.pool = Pool(cfg, log)
        self.router = Router(cfg, self.pool, log, self.stats)

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (BrokenPipeError, ConnectionResetError, ClientGone)):
            return
        self.log.event("socket_error", repr(exc))


# ---------------------------------------------------------------- subcommands


def build_banner(cfg: Config, log_path=None) -> list:
    """The startup banner. A function so tests can assert on it and docs can quote it."""
    banner = [
        f"airelay {VERSION} started",
        f"  listen          http://{cfg.host}:{cfg.port}",
        f"  config          {cfg.source}",
        f"  stream mode     {cfg.stream_mode}"
        + (" (every failure before content is released is retried silently)" if cfg.stream_mode == "gated"
           else " (fully buffered: any failure is retryable, but no token-by-token stream)"),
        f"  log             {log_path or 'stderr only'}",
        f"  retry           up to {cfg.max_attempts} attempts / {int(cfg.total_timeout)}s budget per request",
        "  upstreams:",
    ]
    for up in cfg.upstreams:
        banner.append(
            f"    {up.index + 1}. {up.name:<14} {up.scheme}://{up.host}{up.prefix or ''}"
            f"  auth={redact(up.auth)} ({up.effective_auth_style()})"
            + (f"  # {up.note}" if up.note else "")
        )
    if cfg.model_fallback and any(cfg.model_chains.values()):
        banner.append(f"  model fallback  next model after {cfg.model_switch_after} same-model failures, or when no channel has it:")
        for key, chain in cfg.model_chains.items():
            if chain:
                label = "every other model" if key == "*" else key
                banner.append(f"    {label} -> {' -> '.join(chain)}")
    elif not cfg.model_fallback:
        banner.append("  model fallback  disabled (retry only; your model is never substituted)")
    banner += [
        "",
        "  point your client here:",
        f"    export ANTHROPIC_BASE_URL=http://{cfg.host}:{cfg.port}",
        f"  health:  curl -s http://{cfg.host}:{cfg.port}/__airelay/stats",
        "",
    ]
    return banner


def cmd_serve(args):
    cfg = Config.load(args.config)
    if args.port:
        cfg.port = args.port
    if args.mode:
        cfg.stream_mode = args.mode
    log = Logger(cfg.log_file, echo=not args.quiet)

    server = ProxyServer(cfg, log)
    sys.stderr.write("\n".join(build_banner(cfg, log.path)) + "\n")
    sys.stderr.flush()
    log.event("startup", f"listening on {cfg.host}:{cfg.port}",
              upstream=",".join(u.name for u in cfg.upstreams))
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        sys.stderr.write("\n[airelay] Ctrl-C received, shutting down\n")
    finally:
        log.event("shutdown", json.dumps(server.stats.snapshot(), ensure_ascii=False))
        server.server_close()
    return 0


def _probe_headers(up: Upstream, key, style):
    headers = {
        "Content-Type": "application/json",
        "anthropic-version": DEFAULT_ANTHROPIC_VERSION,
        "Accept-Encoding": "identity",
    }
    if style == "bearer":
        headers["Authorization"] = f"Bearer {key}"
    else:
        headers["x-api-key"] = key
    headers.update(up.extra_headers)
    return headers


def _probe_model(cfg: Config, up: Upstream, model, key, style):
    """
    Fire one free count_tokens probe at a (channel, model) pair.
    Returns (ok, kind, one-line note). kind == "model" means this channel lacks it.
    """
    body = json.dumps({"model": up.model_map.get(model, model),
                       "messages": [{"role": "user", "content": "ping"}]}).encode("utf-8")
    t0 = _now()
    conn = None
    try:
        conn = up.connect(cfg)
        conn.sock.settimeout(cfg.connect_timeout + 15)
        conn.request("POST", up.join("/v1/messages/count_tokens"),
                     body=body, headers=_probe_headers(up, key, style))
        resp = conn.getresponse()
        raw = resp.read()
        ms = int((_now() - t0) * 1000)
        if 200 <= resp.status < 300:
            try:
                tokens = json.loads(raw).get("input_tokens")
            except ValueError:
                tokens = "?"
            return True, "ok", f"{ms}ms  input_tokens={tokens}"
        text = raw.decode("utf-8", "replace")[:160].replace("\n", " ")
        _, message = classify_error_payload(text, default=classify_status(resp.status))
        kind = classify_status(resp.status)
        if looks_model_unavailable(resp.status, message or text, model):
            kind = "model"
        return False, kind, f"HTTP {resp.status} ({kind})  {ms}ms  {text}"
    except UpstreamFailure as exc:
        return False, "retry", str(exc)
    except Exception as exc:
        return False, "retry", f"{type(exc).__name__}: {exc}"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def cmd_doctor(args):
    cfg = Config.load(args.config)

    if args.models:
        models = [m.strip() for m in args.models.split(",") if m.strip()]
    elif args.model:
        models = [args.model]
    else:
        models = [cfg.probe_model] + [m for m in cfg.all_models() if m != cfg.probe_model]

    # passthrough channels have no credential of their own (at runtime they use the
    # client's). Borrow one: --key > ANTHROPIC_AUTH_TOKEN > ANTHROPIC_API_KEY.
    borrowed = args.key or os.environ.get("ANTHROPIC_AUTH_TOKEN") \
        or os.environ.get("ANTHROPIC_API_KEY")

    print(f"airelay doctor — config {cfg.source}")
    print("probe: POST /v1/messages/count_tokens (not billed)")
    print(f"models: {', '.join(models)}")
    if borrowed:
        src = "--key" if args.key else "the environment"
        print(f"passthrough channels borrow the credential {redact(borrowed)} from {src}")
    print()

    worst = 0
    reachable = {m: [] for m in models}   # model -> channels that have it
    hints = {"rotate": "credential/permission problem; at runtime this channel is cooled down and skipped",
             "retry": "transient; at runtime this is retried automatically",
             "model": "this channel lacks this model; at runtime we rotate, then walk the fallback chain",
             "fatal": "the request itself is the problem"}

    for up in cfg.upstreams:
        print(f"  {up.name:<14} {up.scheme}://{up.host}{up.prefix or ''}")
        key, style = up.auth, up.effective_auth_style()
        if key is PASSTHROUGH:
            if not borrowed:
                print("      skipped: auth=passthrough (the credential comes from the client). "
                      "To check it, add --key <your key> or set ANTHROPIC_AUTH_TOKEN")
                continue
            key = borrowed
            style = "bearer" if key.startswith(("sk-ant-oat", "sk-ant-ort")) else \
                    ("x-api-key" if key.startswith("sk-ant-api") or
                     up.host.endswith("anthropic.com") else "bearer")
        for model in models:
            ok, kind, note = _probe_model(cfg, up, model, key, style)
            shown = up.model_map.get(model, model)
            label = shown if shown == model else f"{model} -> {shown}"
            if ok:
                reachable[model].append(up.name)
                print(f"      ✅ {label:<22} {note}")
            else:
                worst = max(worst, 1)
                print(f"      ❌ {label:<22} {note}")
                if kind in hints:
                    print(f"         -> {hints[kind]}")

    print()
    missing = [m for m, got in reachable.items() if not got]
    if len(models) > 1:
        print("model availability:")
        for model in models:
            got = reachable[model]
            print(f"  {model:<24} {', '.join(got) if got else 'no channel provides it'}")
        print()
    if missing and cfg.model_fallback:
        print(f"note: {', '.join(missing)} is unavailable on every channel. "
              "Dropping it from model_fallback.chains saves wasted attempts.")
    if worst:
        print("some channel/model pairs are down. Note: as long as one channel provides "
              "one model from the chain, the running proxy will not stall.")
    return worst


def format_stats(data: dict) -> list:
    """Render the /__airelay/stats JSON as a few human lines. A function so tests can assert."""
    totals = data["totals"]
    out = [
        f"airelay {data['version']}  up {totals['uptime_seconds']}s  "
        f"stream mode={data['stream_mode']}",
        f"  requests {totals['requests']}  succeeded {totals['succeeded']}  failed {totals['failed']}",
        f"  retries {totals['retries']}  channel rotations {totals['channel_rotations']}",
        f"  silently recovered, invisible to the client: {totals['invisible_recoveries']}",
        f"  unrecoverable mid-stream truncations: {totals['unrecoverable_midstream']}",
    ]
    if totals.get("model_switches") or totals.get("served_on_fallback"):
        out.append(f"  model switches {totals['model_switches']}  "
                   f"finished on a fallback model {totals['served_on_fallback']}  "
                   f"channel lacked a model {totals['model_unavailable']}")
    out.append("  channels:")
    for up in data["upstreams"]:
        flag = "healthy" if up["healthy"] else f"cooling {up['cooldown_remaining_s']}s"
        lat = f"{up['avg_latency_ms']}ms" if up["avg_latency_ms"] else "-"
        out.append(f"    {up['name']:<14} {flag:<14} ok={up['ok']} fail={up['failed']} avg {lat}")
        if up.get("missing_models"):
            out.append(f"        missing models: {', '.join(up['missing_models'])}")
        if up["last_error"]:
            out.append(f"        last error [{up['last_error_at']}] {up['last_error'][:140]}")
    return out


def cmd_stats(args):
    host = args.host
    port = args.port
    if not port:
        try:
            port = Config.load(args.config).port
        except ConfigError:
            port = 8787
    try:
        conn = http.client.HTTPConnection(host, port, timeout=5)
        conn.request("GET", "/__airelay/stats")
        resp = conn.getresponse()
        data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        print(f"cannot reach a running airelay ({host}:{port}): {exc}")
        return 1
    print("\n".join(format_stats(data)))
    return 0


def main(argv=None):
    here = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(here, "config.json")

    parser = argparse.ArgumentParser(
        prog="airelay",
        description="local Anthropic-compatible retry / failover proxy",
    )
    sub = parser.add_subparsers(dest="cmd")

    p_serve = sub.add_parser("serve", help="start the proxy")
    p_serve.add_argument("--config", default=default_config)
    p_serve.add_argument("--port", type=int, default=None)
    p_serve.add_argument("--mode", choices=["gated", "buffered"], default=None,
                         help="override stream.mode from the config")
    p_serve.add_argument("--quiet", action="store_true", help="do not echo every event to stderr")
    p_serve.set_defaults(func=cmd_serve)

    p_doc = sub.add_parser("doctor", help="health-check every upstream with a free count_tokens probe")
    p_doc.add_argument("--config", default=default_config)
    p_doc.add_argument("--model", default=None)
    p_doc.add_argument("--models", default=None,
                       help="comma separated; check only these models (default: every model in the chains)")
    p_doc.add_argument("--key", default=None,
                       help="lend a key to auth=passthrough channels just for the check")
    p_doc.set_defaults(func=cmd_doctor)

    p_st = sub.add_parser("stats", help="show the health of a running proxy")
    p_st.add_argument("--config", default=default_config)
    p_st.add_argument("--host", default="127.0.0.1")
    p_st.add_argument("--port", type=int, default=None)
    p_st.set_defaults(func=cmd_stats)

    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 1
    try:
        return args.func(args)
    except ConfigError as exc:
        sys.stderr.write(f"[airelay] config error: {exc}\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
