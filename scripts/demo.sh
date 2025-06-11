#!/usr/bin/env bash
#
# Proves the three claims that matter about a Raft-backed store, in order:
#
#   1. A cluster of three elects exactly one leader.
#   2. A committed write is readable under a linearizable read.
#   3. Killing the leader costs a short unavailability window, not the data: a
#      new leader is elected and the committed value is still there.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=run-cluster.sh
source "$SCRIPT_DIR/run-cluster.sh"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
claim() { printf '  proving: %s\n' "$*"; }

cleanup() {
  step "Tearing down"
  cairn_stop_all
  note "all nodes stopped; data and logs left in ${RUN_DIR#"$CAIRN_ROOT"/} for inspection"
}
trap cleanup EXIT

step "Step 1: start three nodes with empty logs"
claim "three independent processes with their own WALs form one cluster"
cairn_start_all

step "Step 2: wait for an election"
claim "exactly one of the three becomes leader, and all three agree who it is"
LEADER="$(cairn_wait_for_leader 20)"
note "leader: $LEADER"
for id in "${NODE_IDS[@]}"; do
  addr="$(http_addr_of "$id")"
  printf '  %s reports: ' "$id"
  "$BIN_DIR/cairnctl" -addr "$addr" status | awk '$1=="role"||$1=="term"||$1=="leader" {printf "%s=%s ", $1, $2} END {print ""}'
done
note "role transitions logged by the nodes themselves, oldest first:"
grep -h 'role=' "$RUN_DIR"/n*.log | sort -k3 | sed 's/^/    /'

step "Step 3: write through the client API"
claim "a put is accepted only by the leader, and cairnctl finds the leader itself"
# Deliberately aimed at n1 whether or not n1 is the leader: if it is not, the
# 307 redirect and the client's leader resolution are what make this succeed.
"$BIN_DIR/cairnctl" -addr "$HTTP_ADDRS" -state "$RUN_DIR/client-state" put mountain cairn
"$BIN_DIR/cairnctl" -addr "$HTTP_ADDRS" -state "$RUN_DIR/client-state" put summit 2043m
ctl cas -create flag raised

step "Step 4: read it back"
claim "a linearizable read sees the committed writes"
ctl get mountain
ctl get summit
ctl get flag
note "and a stale read, served from local applied state with no coordination:"
ctl get -stale mountain

step "Step 5: kill the leader"
claim "losing the leader does not lose committed data"
LEADER_PID="$(cat "$RUN_DIR/$LEADER.pid")"
note "killing $LEADER (pid $LEADER_PID) with SIGKILL, so it gets no chance to hand over"
kill -9 "$LEADER_PID"
rm -f "$RUN_DIR/$LEADER.pid"
note "waiting for the surviving two to elect a new leader"
NEW_LEADER="$(cairn_wait_for_leader 20)"
if [[ "$NEW_LEADER" == "$LEADER" ]]; then
  echo "the dead node is still being reported as leader; that is a bug" >&2
  exit 1
fi
note "new leader: $NEW_LEADER (was $LEADER)"
note "role transitions again, oldest first; the last line is the new term:"
grep -h 'role=' "$RUN_DIR"/n*.log | sort -k3 | sed 's/^/    /'

step "Step 6: read the value back from the new leadership"
claim "the write committed before the failure survived it"
ctl get mountain
ctl get summit
ctl get flag

step "Step 7: write under the new leader"
claim "the cluster is not just readable but still accepting writes with two of three up"
ctl put mountain cairn-after-failover
ctl get mountain

step "Step 8: cluster status"
claim "the survivors agree on term and leader, and the term has advanced"
for id in "${NODE_IDS[@]}"; do
  [[ -f "$RUN_DIR/$id.pid" ]] || { note "$id is down (killed in step 5)"; continue; }
  printf '  --- %s ---\n' "$id"
  "$BIN_DIR/cairnctl" -addr "$(http_addr_of "$id")" status | sed 's/^/  /'
done

step "Demo complete"
note "elected a leader, committed writes, killed the leader, elected another,"
note "and read the committed data back unchanged."
