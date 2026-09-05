#!/usr/bin/env bash
# airelay launcher
#   ./start.sh                 start the proxy (foreground, Ctrl-C to stop)
#   ./start.sh doctor          check every channel
#   ./start.sh stats           health of a running proxy
#   ./start.sh test            run the self-test
#   ./start.sh install-agent   register as a login-time background service (launchd)
#   ./start.sh uninstall-agent remove it again
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

LABEL="com.airelay.proxy"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

# --- find a new enough python3 ---
PY="${PYTHON:-}"
if [[ -z "$PY" ]]; then
  for c in python3 python3.14 python3.13 python3.12 python3.11 python3.10 python3.9; do
    command -v "$c" >/dev/null 2>&1 && { PY="$c"; break; }
  done
fi
if [[ -z "$PY" ]]; then
  echo "no python3 found. On macOS: brew install python3" >&2
  exit 1
fi
if ! "$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)'; then
  echo "$PY is too old (3.9+ required): $("$PY" -V 2>&1)" >&2
  exit 1
fi

# --- first run writes a config for you ---
if [[ ! -f config.json ]]; then
  cp config.example.json config.json
  echo "wrote config.json from the template."
  echo "One channel is enabled: agentrouter + auth=passthrough (reuses the client's own credential)."
  echo "For \"one dies, move to the next\", set enabled to true on the second and third"
  echo "channels in config.json and put the keys in secrets.env."
  echo
fi

# --- keys come from secrets.env, never from config.json ---
if [[ -f secrets.env ]]; then
  perm="$(stat -f '%A' secrets.env 2>/dev/null || echo '')"
  [[ -n "$perm" && "$perm" != "600" ]] && \
    echo "tip: chmod 600 secrets.env is safer (currently $perm)" >&2
  set -a
  # shellcheck disable=SC1091
  source ./secrets.env
  set +a
fi

CMD="${1:-serve}"
[[ $# -gt 0 ]] && shift || true

case "$CMD" in
  test)
    exec "$PY" selftest.py "$@"
    ;;

  test-e2e)
    exec "$PY" selftest_e2e.py "$@"
    ;;

  install-agent)
    port="$("$PY" - <<'EOF'
import json, sys
try:
    print(json.load(open("config.json")).get("listen", {}).get("port", 8787))
except Exception:
    print(8787)
EOF
)"
    mkdir -p "$HOME/Library/LaunchAgents" logs
    cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$HERE/start.sh</string>
    <string>serve</string>
    <string>--quiet</string>
  </array>
  <key>WorkingDirectory</key><string>$HERE</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>$HERE/logs/launchd.out.log</string>
  <key>StandardErrorPath</key><string>$HERE/logs/launchd.err.log</string>
</dict>
</plist>
EOF
    launchctl unload "$PLIST" 2>/dev/null || true
    launchctl load -w "$PLIST"
    sleep 1
    echo "registered to start at login: $PLIST"
    if curl -fsS --max-time 3 "http://127.0.0.1:$port/__airelay/health" >/dev/null 2>&1; then
      echo "the proxy is running in the background: http://127.0.0.1:$port"
    else
      echo "the proxy is not up yet, check the log: tail -f $HERE/logs/launchd.err.log" >&2
    fi
    case "$HERE" in
      *CloudStorage*|*OneDrive*|*Dropbox*|*"Google Drive"*|*iCloud*)
        echo
        echo "note: this directory lives in a cloud-synced folder. For a long-running"
        echo "      background service, copy the whole directory to ~/airelay and run"
        echo "      install-agent from there, so sync cannot swap files under the process."
        ;;
    esac
    exit 0
    ;;

  uninstall-agent)
    launchctl unload "$PLIST" 2>/dev/null || true
    rm -f "$PLIST"
    echo "removed from login items and stopped."
    exit 0
    ;;
esac

exec "$PY" airelay.py "$CMD" "$@"
