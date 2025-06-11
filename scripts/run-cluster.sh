#!/usr/bin/env bash
#
# Starts a three-node Cairn cluster on loopback and waits until a leader exists.
#
# Run it directly to get a cluster you can poke at with cairnctl; it stays in the
# foreground and tears the cluster down on Ctrl-C. demo.sh sources this file to
# reuse the helper functions below.

set -euo pipefail

CAIRN_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${CAIRN_RUN_DIR:-$CAIRN_ROOT/run}"
BIN_DIR="$RUN_DIR/bin"

NODE_IDS=(n1 n2 n3)
RAFT_PORTS=(7001 7002 7003)
HTTP_PORTS=(8001 8002 8003)

PEERS="n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003"
HTTP_ADDRS="127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"

# Shorter than the library defaults so an election is over in well under a
# second, which is what makes a leader failover watchable in a demo. The 3x
# heartbeat-to-election ratio the config validator insists on still holds.
HEARTBEAT=100ms
ELECTION_MIN=400ms
ELECTION_MAX=800ms

export PATH="/opt/homebrew/bin:$PATH"

say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
note() { printf '  %s\n' "$*"; }

cairn_build() {
  note "building cairn-node and cairnctl into ${BIN_DIR#"$CAIRN_ROOT"/}"
  mkdir -p "$BIN_DIR"
  (cd "$CAIRN_ROOT" && go build -o "$BIN_DIR/cairn-node" ./cmd/cairn-node)
  (cd "$CAIRN_ROOT" && go build -o "$BIN_DIR/cairnctl" ./cmd/cairnctl)
}

ctl() { "$BIN_DIR/cairnctl" -addr "$HTTP_ADDRS" -state "$RUN_DIR/client-state" "$@"; }

http_addr_of() {
  local want="$1" i
  for i in "${!NODE_IDS[@]}"; do
    if [[ "${NODE_IDS[$i]}" == "$want" ]]; then
      printf '127.0.0.1:%s\n' "${HTTP_PORTS[$i]}"
      return 0
    fi
  done
  return 1
}

cairn_start_node() {
  local i="$1"
  local id="${NODE_IDS[$i]}"
  local data="$RUN_DIR/$id"
  mkdir -p "$data"
  "$BIN_DIR/cairn-node" \
    -id "$id" \
    -peers "$PEERS" \
    -data "$data" \
    -http "127.0.0.1:${HTTP_PORTS[$i]}" \
    -heartbeat "$HEARTBEAT" \
    -election-min "$ELECTION_MIN" \
    -election-max "$ELECTION_MAX" \
    -snapshot-threshold 1024 \
    >>"$RUN_DIR/$id.log" 2>&1 &
  echo $! >"$RUN_DIR/$id.pid"
  # Detach from job control so that killing a node in a demo does not make the
  # shell print its own "Killed" notice over the demo's narration.
  disown %% 2>/dev/null || true
  note "$id: consensus 127.0.0.1:${RAFT_PORTS[$i]}  client http://127.0.0.1:${HTTP_PORTS[$i]}  pid $(cat "$RUN_DIR/$id.pid")"
}

cairn_start_all() {
  rm -rf "$RUN_DIR"
  mkdir -p "$RUN_DIR"
  cairn_build
  say "Starting a 3-node cluster"
  local i
  for i in "${!NODE_IDS[@]}"; do
    cairn_start_node "$i"
  done
}

cairn_stop_all() {
  local id pid
  for id in "${NODE_IDS[@]}"; do
    [[ -f "$RUN_DIR/$id.pid" ]] || continue
    pid="$(cat "$RUN_DIR/$id.pid")"
    # SIGTERM, not SIGKILL: the node's handler is what closes the log cleanly,
    # and a demo that killed -9 every node would never exercise that path.
    kill -TERM "$pid" 2>/dev/null || true
  done
  for id in "${NODE_IDS[@]}"; do
    [[ -f "$RUN_DIR/$id.pid" ]] || continue
    pid="$(cat "$RUN_DIR/$id.pid")"
    while kill -0 "$pid" 2>/dev/null; do sleep 0.05; done
    rm -f "$RUN_DIR/$id.pid"
  done
}

# cairn_leader prints the ID of a node that says it is the leader itself.
#
# Asking a follower who the leader is would be wrong here: a follower keeps its
# last known leader until its election timer fires, so right after a leader is
# killed every survivor still names the dead node. Only "role=leader" from the
# node's own mouth is evidence.
cairn_leader() {
  local i role id
  for i in "${!NODE_IDS[@]}"; do
    id="${NODE_IDS[$i]}"
    [[ -f "$RUN_DIR/$id.pid" ]] || continue
    role="$("$BIN_DIR/cairnctl" -addr "127.0.0.1:${HTTP_PORTS[$i]}" status 2>/dev/null |
      awk '$1 == "role" { print $2 }')" || true
    if [[ "${role:-}" == "leader" ]]; then
      printf '%s\n' "$id"
      return 0
    fi
  done
  return 1
}

cairn_wait_for_leader() {
  local deadline=$((SECONDS + ${1:-20})) leader
  while ((SECONDS < deadline)); do
    if leader="$(cairn_leader)"; then
      printf '%s\n' "$leader"
      return 0
    fi
    sleep 0.1
  done
  echo "no leader emerged within ${1:-20}s; see $RUN_DIR/*.log" >&2
  return 1
}

main() {
  cairn_start_all
  say "Waiting for a leader"
  local leader
  leader="$(cairn_wait_for_leader 20)"
  note "leader is $leader at http://$(http_addr_of "$leader")"

  say "Cluster is up"
  local i
  for i in "${!NODE_IDS[@]}"; do
    note "${NODE_IDS[$i]}  http://127.0.0.1:${HTTP_PORTS[$i]}"
  done
  printf '\n  try: %s -addr %s status\n' "${BIN_DIR#"$CAIRN_ROOT"/}/cairnctl" "$HTTP_ADDRS"
  printf '  logs: %s/n*.log\n' "${RUN_DIR#"$CAIRN_ROOT"/}"
  printf '\n  Ctrl-C to shut the cluster down.\n\n'

  trap 'say "Shutting down"; cairn_stop_all; exit 0' INT TERM
  while true; do sleep 1; done
}

# Only run the cluster when executed; demo.sh sources this file for the helpers.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
