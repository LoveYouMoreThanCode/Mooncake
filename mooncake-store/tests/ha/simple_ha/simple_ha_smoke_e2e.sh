#!/usr/bin/env bash
# Simple HA L3 smoke:
#   1. kill -9 the serving leader; the standby must promote and the keys must
#      stay readable.
#   2. SIGSTOP the leader (frozen process: peer RPC and etcd keepalive both
#      stop); the standby promotes. SIGCONT the old leader; it must notice the
#      lost lease, drain, and merge from the new leader. Then kill the new
#      leader and verify the old leader re-promotes with the keys.
#
# Requires a STORE_USE_ETCD build (build-l2 by default):
#   cmake -S . -B build-l2 -DWITH_STORE=ON -DWITH_TE=OFF \
#     -DBUILD_UNIT_TESTS=ON -DSTORE_USE_ETCD=ON \
#     -DMOONCAKE_ENABLE_TEST_FAILPOINTS=ON
#   cmake --build build-l2 --target mooncake_master simple_ha_smoke_client -j4
#
# Also needs etcd/etcdctl in PATH. Set KEEP_WORK=1 to keep the temp logs.
set -euo pipefail
ulimit -c 0

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
MASTER_BIN="${MASTER_BIN:-${ROOT_DIR}/build-l2/mooncake-store/src/mooncake_master}"
CLIENT_BIN="${CLIENT_BIN:-${ROOT_DIR}/build-l2/mooncake-store/tests/ha/simple_ha/simple_ha_smoke_client}"
ETCD_BIN="${ETCD_BIN:-etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-etcdctl}"
BASE="${BASE:-$((24200 + ($$ % 200) * 10))}"
ETCD_CLIENT=$((BASE + 1))
ETCD_PEER=$((BASE + 2))

for bin in "$MASTER_BIN" "$CLIENT_BIN"; do
  if [[ ! -x "$bin" ]]; then
    echo "missing executable: $bin" >&2
    echo "build build-l2 with STORE_USE_ETCD=ON and targets" \
         "mooncake_master/simple_ha_smoke_client" >&2
    exit 1
  fi
done
command -v "$ETCD_BIN" >/dev/null 2>&1 || {
  echo "missing etcd binary" >&2
  exit 1
}

work="$(mktemp -d)"
etcd_pid=""
declare -A PIDS=()
declare -a ALL_PIDS=()
current_cluster=""

cleanup() {
  rc=$?
  set +e
  for pid in "${ALL_PIDS[@]}"; do
    kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
  done
  if [[ -n "$etcd_pid" ]]; then
    kill -9 "$etcd_pid" 2>/dev/null
    wait "$etcd_pid" 2>/dev/null
  fi
  if [[ "${KEEP_WORK:-0}" != 1 ]]; then
    rm -rf "$work"
  else
    echo "work kept: $work"
  fi
  exit "$rc"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for log in "$work"/*.log; do
    [[ -f "$log" ]] && {
      echo "--- $log" >&2
      tail -40 "$log" >&2
    }
  done
  exit 1
}

health() {
  curl -s --max-time 1 "http://127.0.0.1:$1/health" 2>/dev/null || true
}

is_serving() {
  local body
  body="$(health "$1")"
  [[ "$body" == *'"ha_state":"serving"'* &&
     "$body" == *'"service_ready":true'* ]]
}

is_state() {
  local body
  body="$(health "$1")"
  [[ "$body" == *"\"ha_state\":\"$2\""* ]]
}

wait_serving() {
  local admin=$1
  local timeout=$2
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    is_serving "$admin" && return 0
    sleep 0.1
  done
  return 1
}

wait_state() {
  local admin=$1
  local state=$2
  local timeout=$3
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    is_state "$admin" "$state" && return 0
    sleep 0.1
  done
  return 1
}

find_leader() {
  local a=$1
  local b=$2
  local timeout=$3
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    is_serving "$a" && {
      echo 1
      return 0
    }
    is_serving "$b" && {
      echo 2
      return 0
    }
    sleep 0.1
  done
  return 1
}

start_etcd() {
  "$ETCD_BIN" --name l3-etcd --data-dir "$work/etcd-data" \
    --listen-client-urls "http://127.0.0.1:${ETCD_CLIENT}" \
    --advertise-client-urls "http://127.0.0.1:${ETCD_CLIENT}" \
    --listen-peer-urls "http://127.0.0.1:${ETCD_PEER}" \
    --initial-advertise-peer-urls "http://127.0.0.1:${ETCD_PEER}" \
    --initial-cluster "l3-etcd=http://127.0.0.1:${ETCD_PEER}" \
    >"$work/etcd.log" 2>&1 &
  etcd_pid=$!
  for _ in $(seq 1 50); do
    "$ETCDCTL_BIN" --endpoints="127.0.0.1:${ETCD_CLIENT}" \
      --command-timeout=1s endpoint health >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  fail "etcd did not become healthy"
}

# start_master <name> <rpc> <admin> <sim> <peer_sim> <root>
start_master() {
  local name=$1 rpc=$2 admin=$3 sim=$4 peer_sim=$5 root=$6
  mkdir -p "$root"
  "$MASTER_BIN" \
    --enable_ha=true \
    --enable_simple_ha=true \
    --enable_oplog=false \
    --ha_backend_type=etcd \
    --ha_backend_connstring="127.0.0.1:${ETCD_CLIENT}" \
    --ha_leader_lease_ttl_sec=2 \
    --cluster_id="$current_cluster" \
    --rpc_address=127.0.0.1 \
    --rpc_port="$rpc" \
    --metrics_host=127.0.0.1 \
    --metrics_port="$admin" \
    --enable_metric_reporting=true \
    --root_fs_dir="$root" \
    --simple_ha_peer_address="127.0.0.1:${peer_sim}" \
    --simple_ha_rpc_address=127.0.0.1 \
    --simple_ha_rpc_port="$sim" \
    --simple_ha_pull_interval_ms=50 \
    --simple_ha_snapshot_chunk_entries=8 \
    --logtostderr=1 >"$work/$name.log" 2>&1 &
  PIDS[$name]=$!
  ALL_PIDS+=("$!")
}

stop_master() {
  local pid=${PIDS[$1]:-}
  [[ -n "$pid" ]] || return 0
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

assign_roles() {
  local leader=$1
  local rpc1=$2 admin1=$3 name1=$4 rpc2=$5 admin2=$6 name2=$7
  if [[ $leader == 1 ]]; then
    LEADER_RPC=$rpc1 LEADER_ADMIN=$admin1 LEADER_NAME=$name1
    STANDBY_RPC=$rpc2 STANDBY_ADMIN=$admin2 STANDBY_NAME=$name2
  else
    LEADER_RPC=$rpc2 LEADER_ADMIN=$admin2 LEADER_NAME=$name2
    STANDBY_RPC=$rpc1 STANDBY_ADMIN=$admin1 STANDBY_NAME=$name1
  fi
}

case_kill9() {
  current_cluster="smoke-kill-$$"
  local rpc1=$((BASE + 10)) admin1=$((BASE + 11)) sim1=$((BASE + 12))
  local rpc2=$((BASE + 20)) admin2=$((BASE + 21)) sim2=$((BASE + 22))
  start_master n1 "$rpc1" "$admin1" "$sim1" "$sim2" "$work/n1"
  start_master n2 "$rpc2" "$admin2" "$sim2" "$sim1" "$work/n2"
  local leader
  leader=$(find_leader "$admin1" "$admin2" 30) || fail "no leader"
  assign_roles "$leader" "$rpc1" "$admin1" n1 "$rpc2" "$admin2" n2

  "$CLIENT_BIN" put "127.0.0.1:${LEADER_RPC}" smoke-kill-a smoke-kill-b ||
    fail "put"
  sleep 1
  wait_state "$STANDBY_ADMIN" standby 20 || fail "standby not synced"

  local leader_pid=${PIDS[$LEADER_NAME]}
  kill -9 "$leader_pid" 2>/dev/null || true
  wait "$leader_pid" 2>/dev/null || true
  wait_serving "$STANDBY_ADMIN" 30 || fail "standby did not promote"
  "$CLIENT_BIN" exist "127.0.0.1:${STANDBY_RPC}" smoke-kill-a smoke-kill-b ||
    fail "keys lost after kill -9"
  stop_master "$STANDBY_NAME"
}

case_freeze() {
  current_cluster="smoke-freeze-$$"
  local rpc1=$((BASE + 110)) admin1=$((BASE + 111)) sim1=$((BASE + 112))
  local rpc2=$((BASE + 120)) admin2=$((BASE + 121)) sim2=$((BASE + 122))
  start_master n1 "$rpc1" "$admin1" "$sim1" "$sim2" "$work/n1"
  start_master n2 "$rpc2" "$admin2" "$sim2" "$sim1" "$work/n2"
  local leader
  leader=$(find_leader "$admin1" "$admin2" 30) || fail "no leader"
  assign_roles "$leader" "$rpc1" "$admin1" n1 "$rpc2" "$admin2" n2

  "$CLIENT_BIN" put "127.0.0.1:${LEADER_RPC}" smoke-freeze-a smoke-freeze-b ||
    fail "put"
  sleep 1
  wait_state "$STANDBY_ADMIN" standby 20 || fail "standby not synced"

  local leader_pid=${PIDS[$LEADER_NAME]}
  kill -STOP "$leader_pid"
  wait_serving "$STANDBY_ADMIN" 30 || fail "standby did not promote after freeze"
  "$CLIENT_BIN" exist "127.0.0.1:${STANDBY_RPC}" smoke-freeze-a smoke-freeze-b ||
    fail "keys lost while the old leader was frozen"

  kill -CONT "$leader_pid"
  wait_state "$LEADER_ADMIN" standby 30 ||
    fail "resumed leader did not return as standby"

  local standby_pid=${PIDS[$STANDBY_NAME]}
  kill -9 "$standby_pid" 2>/dev/null || true
  wait "$standby_pid" 2>/dev/null || true
  wait_serving "$LEADER_ADMIN" 30 ||
    fail "old leader did not re-promote with the merged state"
  "$CLIENT_BIN" exist "127.0.0.1:${LEADER_RPC}" smoke-freeze-a smoke-freeze-b ||
    fail "keys lost after freeze/resume"
  stop_master "$LEADER_NAME"
}

start_etcd
echo "case 1: kill -9 the leader"
case_kill9
echo "case 2: freeze/resume the leader"
case_freeze
echo "PASS: simple HA shell smoke"
