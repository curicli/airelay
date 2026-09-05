# airelay — stop losing AI tasks to `Server error`

A local, Anthropic-compatible proxy. Point any client at it (Claude Code, an SDK, your
own scripts) and it fights the relay/API on your behalf: **retry on failure, rotate to
another channel when one is broken, walk a fallback chain when a model is unavailable,
hold the connection open with keepalives while it retries — until it succeeds.** The
client sees one clean successful response and never needs you to click "try again".

Zero dependencies. Python 3.9+ stdlib. One file: `airelay.py`.

---

## What it actually fixes

Before:

```
Claude Code ─────────────────► relay ──► model
                    ↑
              one hiccup here kills your task, and you retry by hand
```

After:

```
Claude Code ──► airelay ──┬──► relay A (down? retry / cool it down)
                          ├──► relay B (picked up automatically)
   ↑                      └──► official API (last resort)
   connection stays alive, └──► nobody has your model? walk the fallback chain
   you only see pings,
   then the full answer
```

The point is **the client never notices**. As long as the failure happens before any
content has been handed to the client, airelay holds the whole `message_start` /
`content_block_start` prefix back (the **gate**). If the retry succeeds, the client sees
a response that looks like it worked first time. If it fails, airelay rotates. In
between, all the client receives is `ping`.

---

## 60 seconds to running

```bash
cd airelay
./start.sh doctor      # health check first: which channels answer, which models exist
./start.sh             # start the proxy (foreground, Ctrl-C to stop)
```

The first run copies `config.example.json` to `config.json`. Only one channel is enabled
by default (`agentrouter` with `auth: passthrough`), and that alone already swallows the
large majority of `Server error`s.

On startup it prints:

```
airelay 1.1.0 started
  listen          http://127.0.0.1:8787
  config          config.json
  stream mode     gated (every failure before content is released is retried silently)
  log             logs/airelay.jsonl
  retry           up to 30 attempts / 1800s budget per request
  upstreams:
    1. agentrouter    https://agentrouter.org  auth=<passthrough> (bearer)  # primary; passthrough reuses the client's own token, so no key to fill in
  model fallback  next model after 4 same-model failures, or when no channel has it:
    claude-opus-5 -> claude-opus-4-8 -> claude-sonnet-5
    claude-sonnet-5 -> claude-sonnet-4-6
    claude-haiku-4-5-20251001 -> claude-sonnet-5

  point your client here:
    export ANTHROPIC_BASE_URL=http://127.0.0.1:8787
  health:  curl -s http://127.0.0.1:8787/__airelay/stats
```

---

## Pointing a client at it

### Claude Code desktop app

The desktop app manages its own base URL and token (that is what
`CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST=1` in your environment means), so **exporting a
variable in your shell does nothing**. Change it in the app:

> Open the app's API provider settings and change the Base URL field — the one currently
> holding your relay's URL — to `http://127.0.0.1:8787`. Leave the key field alone.

The key can stay because the default config uses `"auth": "passthrough"`: airelay
forwards the `Authorization: Bearer …` header the app sends, and stores nothing.

### Claude Code CLI

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8787
claude
```

Put it in `~/.zshrc` to make it permanent.

### SDK / your own scripts

```python
from anthropic import Anthropic
client = Anthropic(base_url="http://127.0.0.1:8787")
```

```bash
curl http://127.0.0.1:8787/v1/messages -H "x-api-key: $KEY" ...
```

`/v1/messages`, `/v1/messages/count_tokens` and every other `/v1/*` path are forwarded.

**The proxy has to be running**, otherwise the client gets connection refused. To keep it
up permanently, see "Start at login" below.

---

## Failures it swallows

Every row here is something relays really do, and every row has a self-test case:

| Failure | What airelay does | What the client sees |
|---|---|---|
| HTTP 500 / 502 / 503 / 504 | back off and retry | nothing happened |
| HTTP 529 overloaded | back off and retry | nothing happened |
| Cloudflare 520–530 | back off and retry | nothing happened |
| HTTP 429 | wait out `Retry-After` (seconds or HTTP-date), then retry | nothing happened |
| connection reset / read timeout | retry | nothing happened |
| `200 OK` then `event: error` mid-stream | retry (the nastiest relay trick) | nothing happened |
| `200 OK` then the connection dies before any content | the gate holds the half response back, retry | nothing happened |
| `200 OK` whose body is really `{"type":"error"}` | recognised as a fake success, retry | nothing happened |
| `200 OK` with an empty body | retry | nothing happened |
| 401 / 402 / 403 (credentials, balance, permission) | **no retry in place**: cool this channel down and move to the next | nothing happened |
| 404 / 400 "no such model" | look for the same model on another channel; if nobody has it, walk the fallback chain. **The channel is not cooled down** — it is only missing one model | nothing happened |
| 400 / 422 (the request really is invalid) | hand it back verbatim, no pointless retries | a real 400 |
| the connection dies after content was already released | `gated` cannot fix it and says so honestly; `buffered` can | see below |

Slow failures are covered too: while backing off, airelay commits the response headers
once it has been stuck for `optimistic_commit_after` (default 8s) and then sends a `ping`
every 15 seconds. The client's idle timeout never fires, so one request can keep retrying
for up to `timeouts.total` (default 30 minutes).

---

## `gated` or `buffered` (the one tradeoff you have to make)

| | `gated` (default) | `buffered` |
|---|---|---|
| token-by-token streaming | yes | no, the whole answer arrives at once |
| failures **before** content is released | recovered silently | recovered silently |
| failures **after** content is released | cannot be recovered | recovered silently |
| good for | interactive use (Claude Code) | unattended long runs, batch jobs |

```bash
./start.sh serve --mode buffered      # one-off
```

or set `stream.mode` in `config.json`.

Why `gated` cannot recover: once bytes have gone to the client, HTTP gives you no way to
take them back. Making the model "carry on from that half sentence" needs assistant
prefill continuation, and **that returns 400 on Opus 5 / Sonnet 5 / 4.6+**, so the road is
closed. Rather than fake a success, airelay emits an `error` event labelled `airelay` and
records an `unrecoverable_midstream` in `/__airelay/stats`. To eliminate the case
entirely, use `buffered`.

How much this matters in practice: it only applies to the window after the model has
already started emitting content. The `Server error`s you see day to day almost all happen
before that, and `gated` eats all of those.

---

## Multiple channels: one dies, the next takes over

A single channel can only retry. A second channel is what makes "this provider is entirely
down and my task still finishes" possible.

Edit the second and third entries under `upstreams` in `config.json` and set `enabled` to
`true`:

```bash
cp secrets.env.example secrets.env
chmod 600 secrets.env
# fill in BACKUP_RELAY_KEY / ANTHROPIC_API_KEY
./start.sh doctor
```

Three ways to give a key. **Never put one in `config.json` in plaintext**:

- `"${BACKUP_RELAY_KEY}"` — read an env var (`start.sh` loads `secrets.env` for you)
- `"file:/absolute/path"` — read the first line of a file
- `"passthrough"` — use whatever credential the client already sends

Breaker logic: `breaker.failure_threshold` consecutive failures (default 3) on one channel
puts it in cooldown, doubling each time up to 300 seconds; credential problems (401/403)
go straight to a 90-second cooldown. Half-open probes run during cooldown and the channel
comes back as soon as it recovers. **As long as one channel works, the request does not
fail.**

`auth_style` normally needs no attention (`auto`). airelay picks by credential prefix:
`sk-ant-api…` uses `x-api-key`, `sk-ant-oat…` / `sk-ant-ort…` use `Authorization: Bearer`.
It also guarantees **only one of the two headers is sent** — sending both is a guaranteed
401, and it is the most common trap when wiring up a relay yourself.

### About prompt caching

Rotating channels throws away the original channel's prompt cache, so the first request on
the new channel is slower and more expensive (very noticeable with Claude Code's long
context). That is why `sticky_primary: true` is the default: while channel 1 is healthy,
stay on channel 1 and do no load balancing at all. This is deliberate — you want
stability, not balance.

---

## Automatic model switching: the failures rotation cannot fix

Two kinds of failure survive channel rotation, because the channel is not the problem:

1. **This channel does not carry the model you asked for.** Relays enable different model
   sets, and `sonnet` present with `opus` missing is very common. It shows up as a 404 or
   400 whose body says something like `model xxx not found` or "unsupported model" —
   everyone words it differently, and airelay recognises a dozen or so common phrasings in
   both English and Chinese.
2. **The model is overloaded everywhere.** Every channel returns 529 and waiting longer
   returns 529 again.

In those cases airelay switches models along `model_fallback.chains`. The order is
**channels first, then models** — the model you asked for gets priority:

```
you asked for claude-opus-5
  ├─ channel A: 404 "no opus" -> remember "A lacks opus", do not cool A, rotate
  ├─ channel B: 404 "no opus" -> remember "B lacks opus"
  ├─ no channel has opus -> switch model: claude-opus-4-8
  └─ channel A: 200 done (the response headers tell you a fallback was used)
```

Two triggers:

- **No channel has this model** → move to the next rung immediately (no backoff, no wasted
  time).
- **The same model failed `switch_after_attempts` times in a row** (default 4, counting
  failures after rotation) → move to the next rung. This is the cure for a site-wide opus
  overload: after four stubborn failures, pushing the task forward on 4-8 beats waiting.

Configuration (`config.json`):

```json
"model_fallback": {
  "enabled": true,
  "switch_after_attempts": 4,
  "unavailable_ttl_seconds": 600,
  "chains": {
    "claude-opus-5": ["claude-opus-4-8", "claude-sonnet-5"],
    "claude-sonnet-5": ["claude-sonnet-4-6"],
    "claude-haiku-4-5-20251001": ["claude-sonnet-5"],
    "*": []
  }
}
```

- The key is the model the client asked for; the value is the models to try, **in order**.
  `"*"` is the default chain for any model without its own entry.
- `unavailable_ttl_seconds`: "A lacks opus" is remembered for 10 minutes, then A gets
  another chance — relays add models back at any time. Meanwhile **every other model** on
  that channel keeps being used; it is not in cooldown.
- **Set `enabled` to `false` if you want no automatic downgrades at all.** airelay then
  only retries and rotates, and always sends the model you asked for (exactly the 1.0.0
  behaviour).

When a downgrade happens, the response headers say so:

```
X-Airelay-Model:       claude-opus-4-8
X-Airelay-Model-Trail: claude-opus-5 -> claude-opus-4-8
```

Neither header appears when there was no downgrade — their presence *is* the signal. The
log also carries a `model_switch` event and `stats` counts model switches.

⚠️ **A fallback model does not write like the one you asked for.** This is the only place
airelay quietly changes your request, which is why it leaves a trace in three places:
response headers, the log, and the stats. For tight control, keep the chains short (one
rung, say) or turn the feature off.

⚠️ **Do not cross generations.** The default chains stay within 4.6+ on purpose:
`thinking: {type: "adaptive"}` is rejected with a 400 on pre-4.6 models (they want
`budget_tokens`). Put a 4.5-era model in a chain and any client using adaptive thinking
gets a 400 on that rung. airelay carries on down the chain, but that rung was wasted.

Models chosen by a downgrade still go through each channel's own `model_map` (chain first,
then per-channel rename). `./start.sh doctor` prints which channel has which model as a
matrix — run it once before writing chains and save yourself the guesswork.

---

## Seeing what it did

```bash
./start.sh stats
```

```
airelay 1.1.0  up 3612s  stream mode=gated
  requests 145  succeeded 145  failed 0
  retries 19  channel rotations 1
  silently recovered, invisible to the client: 12
  unrecoverable mid-stream truncations: 0
  model switches 1  finished on a fallback model 1  channel lacked a model 2
  channels:
    agentrouter    healthy        ok=143 fail=7 avg 1832ms
        missing models: claude-opus-5
    official       healthy        ok=2 fail=0 avg 2104ms
```

`silently recovered` is the count of "would have interrupted you, and you never noticed" —
the bigger it gets, the more manual retries it saved you. `failed 0` is the number that
actually matters. The model-switch line only appears once a downgrade has really happened.

```bash
curl -s http://127.0.0.1:8787/__airelay/stats | python3 -m json.tool
curl -s http://127.0.0.1:8787/__airelay/health          # for monitoring
tail -f logs/airelay.jsonl                              # one structured record per retry
```

`./start.sh doctor` fires a free `count_tokens` probe at every real channel × every model
in the chains and tells you which pairs work, how slow they are, and what class of problem
the broken ones have:

```
airelay doctor — config config.json
probe: POST /v1/messages/count_tokens (not billed)
models: claude-opus-5, claude-sonnet-5
passthrough channels borrow the credential sk-ant…-xyz from --key

  agentrouter    https://agentrouter.org
      ❌ claude-opus-5          HTTP 404 (model)  138ms  {"type": "error", "error": {"type": "not_found_error", "message": "model: claude-opus-5 not found"}}
         -> this channel lacks this model; at runtime we rotate, then walk the fallback chain
      ✅ claude-sonnet-5        184ms  input_tokens=12
  official       https://api.anthropic.com
      ✅ claude-opus-5          402ms  input_tokens=12
      ✅ claude-sonnet-5        377ms  input_tokens=12

model availability:
  claude-opus-5            official
  claude-sonnet-5          agentrouter, official

some channel/model pairs are down. Note: as long as one channel provides one model from the chain, the running proxy will not stall.
```

A `passthrough` channel has no credential of its own, so the health check borrows one:

```bash
./start.sh doctor --key sk-ant-oat...     # or set ANTHROPIC_AUTH_TOKEN beforehand
./start.sh doctor --models claude-opus-5  # check specific models only (comma separated)
```

---

## Start at login (the key to running unattended)

```bash
./start.sh install-agent      # register a launchd service, starts at login
./start.sh uninstall-agent    # remove it
```

`KeepAlive` is on: if the proxy process ever crashes, launchd brings it back within ten
seconds. Together with the proxy's own retry and rotation, that is two independent layers.

⚠️ If the directory lives in a cloud-synced folder (OneDrive, Dropbox, iCloud…), **copy the
whole thing to `~/airelay` and run `install-agent` there**, so sync cannot swap files under
a running process. `install-agent` prints this reminder when it detects such a path.

```bash
cp -R "$PWD" ~/airelay && cd ~/airelay && ./start.sh install-agent
```

---

## Honest limitations

1. **A truncation after content was released cannot be recovered in `gated` mode.** Reason
   and workaround above. This follows from HTTP plus the model API, not from a lazy
   implementation.
2. **airelay does not replay request content.** It retries the whole HTTP request, so it is
   only safe for idempotent inference requests (the Messages API is idempotent). It never
   tries to guess "that half-finished answer".
3. **A fallback model is not the model you asked for.** Automatic switching keeps the task
   moving, but that stretch of output was written by a different model. The trace lives in
   the response headers, the log and the stats; if that is not acceptable, set
   `model_fallback.enabled` to `false`.
4. **Rotating channels loses the prompt cache.** `sticky_primary` already minimises this.
5. **It only handles transport failures.** A model saying something wrong, or bad tool-call
   logic, is not its job.
6. **If the proxy is down, everything is down.** It is a single point of failure.
   `KeepAlive` plus two layers of retry is the mitigation; if it ever does break, point the
   client's base URL back at the relay and you are immediately where you started.

---

## Config reference

`config.json` (fully commented template in `config.example.json`):

| Key | Default | Meaning |
|---|---|---|
| `listen.port` | 8787 | port to listen on |
| `sticky_primary` | true | stay on channel 1 while it is healthy, to keep the prompt cache |
| `retry.max_attempts` | 30 | attempts per request |
| `retry.initial_backoff` / `max_backoff` | 1.0 / 20.0 | backoff floor and ceiling in seconds, jittered |
| `retry.respect_retry_after` | true | honour the upstream's `Retry-After` |
| `timeouts.total` | 1800 | total budget per request in seconds — the ceiling on retrying |
| `timeouts.stream_idle` | 120 | how long a silent stream counts as stalled |
| `stream.mode` | `gated` | `gated` / `buffered`, see above |
| `stream.keepalive_interval` | 15 | seconds between pings while retrying |
| `stream.optimistic_commit_after` | 8 | how long to be stuck before committing headers to hold the connection |
| `breaker.failure_threshold` | 3 | consecutive failures before a channel is cooled down |
| `model_fallback.enabled` | true | off = retry and rotate only, your model is never substituted |
| `model_fallback.chains` | see template | `{"model you asked for": ["models to fall back to, in order"]}`, `"*"` is the default chain |
| `model_fallback.switch_after_attempts` | 4 | same-model failures before the next rung |
| `model_fallback.unavailable_ttl_seconds` | 600 | how long "this channel lacks this model" is remembered |
| `log.file` | `logs/airelay.jsonl` | structured log; `null` disables it |

Command-line overrides: `./start.sh serve --port 9000 --mode buffered --quiet`

---

## Self-tests

```bash
./start.sh test         # logic layer: 335 assertions, no network, no listening socket, ~4s
./start.sh test-e2e     # end to end: fake upstreams + the real proxy + a real HTTP client
```

`test` covers every row of the failure table above, plus auth headers, `model_map`, every
branch of automatic model switching, config validation, how the stats are counted, the real
HTTP parsing layer, and the CLI. Current status: **335 of 335 passing.**

`test-e2e` needs to be able to listen on a local port. It was written in a restricted
sandbox where `bind` is denied; the script detects that and skips cleanly, so **run it once
from your own terminal** to cover the end-to-end layer.

---

## Files

| File | What it is |
|---|---|
| `airelay.py` | the proxy itself, single file, zero dependencies |
| `config.example.json` | config template (copied to `config.json` on first run) |
| `secrets.env.example` | key template (copy to `secrets.env`, `chmod 600`) |
| `start.sh` | start / health check / self-test / install as a login item |
| `com.airelay.proxy.plist.example` | launchd template (`install-agent` generates this for you) |
| `selftest.py` | logic-layer self-test, no sockets |
| `selftest_e2e.py` | end-to-end self-test, needs to listen on a port |
