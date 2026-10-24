#!/usr/bin/env bash
# Build and run the Simple HA L1/L2/L3 suites end to end.
#
#   L1: in-process gtest harness (no etcd)
#   L2: supervisor e2e gtest (starts its own etcd; spawns two masters)
#   L3: shell smoke (kill -9 and freeze/resume; starts its own etcd)
#
# The build uses a dedicated directory because L2/L3 need STORE_USE_ETCD=ON
# (which builds the Go etcd wrapper) and failpoints for the L1 mid-merge test.
#
# Usage:
#   scripts/run_simple_ha_e2e.sh [options]
#
# Options:
#   --build-dir DIR   Build directory (default: build-ha-e2e)
#   --jobs N          Parallel build jobs (default: nproc)
#   --skip-build      Reuse an existing build directory
#   --skip-l3         Do not run the shell smoke
#   --keep-work       Keep the L3 temp directory and logs
#   --strict          Fail instead of skipping when prerequisites are missing
#   -h, --help        Show this help
#
# Environment:
#   BUILD_DIR, JOBS, GOOGLETEST_SOURCE_DIR, YALANTINGLIBS_SOURCE_DIR,
#   ETCD_BIN, ETCDCTL_BIN
#
# Prerequisites: cmake, a C++ toolchain, Go, etcd, etcdctl, curl. Go module
# access is needed the first time the etcd wrapper is built.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build-ha-e2e}"
JOBS="${JOBS:-$(nproc)}"
GOOGLETEST_SOURCE_DIR="${GOOGLETEST_SOURCE_DIR:-}"
STRICT="${STRICT:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_L3="${SKIP_L3:-0}"
KEEP_WORK="${KEEP_WORK:-0}"

usage() {
  sed -n '/^# Usage:/,/^# Prerequisites:/p' "${BASH_SOURCE[0]}" |
    sed 's/^# //; s/^#$//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      BUILD_DIR="$2"
      shift 2
      ;;
    --jobs)
      JOBS="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --skip-l3)
      SKIP_L3=1
      shift
      ;;
    --keep-work)
      KEEP_WORK=1
      shift
      ;;
    --strict)
      STRICT=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "${ROOT_DIR}/CMakeLists.txt" ]]; then
  echo "cannot find CMakeLists.txt under ${ROOT_DIR}" >&2
  exit 2
fi

LOG_ROOT="${ROOT_DIR}/local_test/run-simple-ha-e2e-logs/$(date +%Y%m%d-%H%M%S)"
mkdir -p "${LOG_ROOT}"

declare -a STAGES=()
record() { STAGES+=("$1|$2"); }

summary() {
  echo
  echo "Simple HA E2E summary"
  for entry in "${STAGES[@]}"; do
    printf '  %-12s %s\n' "${entry%%|*}" "${entry#*|}"
  done
  echo "logs: ${LOG_ROOT}"
}

run_stage() {
  local name=$1
  local log=$2
  shift 2
  echo "== ${name}"
  local rc=0
  "$@" >"${log}" 2>&1 || rc=$?
  if ((rc == 0)); then
    record "${name}" "PASS"
    return 0
  fi
  record "${name}" "FAIL"
  echo "--- ${log} (tail)" >&2
  tail -40 "${log}" >&2 || true
  return "${rc}"
}

# --- prerequisites ---------------------------------------------------------

missing=()
for cmd in cmake go etcd etcdctl curl timeout; do
  command -v "${cmd}" >/dev/null 2>&1 || missing+=("${cmd}")
done
if ((${#missing[@]})); then
  echo "SKIP: missing prerequisites: ${missing[*]}"
  echo "  Debian/Ubuntu: apt install etcd-server etcd-client; install Go and curl"
  if [[ "${STRICT}" == "1" ]]; then
    record prerequisites "FAIL (${missing[*]})"
    summary
    exit 1
  fi
  record prerequisites "SKIP (${missing[*]})"
  summary
  exit 0
fi
record prerequisites PASS

# --- configure/build -------------------------------------------------------

if [[ -z "${GOOGLETEST_SOURCE_DIR}" ]]; then
  for candidate in \
    "${ROOT_DIR}/build/_deps/googletest-src" \
    "/tmp/googletest-1.17.0"; do
    if [[ -f "${candidate}/CMakeLists.txt" ]]; then
      GOOGLETEST_SOURCE_DIR="${candidate}"
      break
    fi
  done
fi
if [[ -n "${GOOGLETEST_SOURCE_DIR}" ]]; then
  echo "gtest source: ${GOOGLETEST_SOURCE_DIR}"
else
  echo "gtest source: FetchContent (requires network)"
fi

YALANTINGLIBS_SOURCE_DIR="${YALANTINGLIBS_SOURCE_DIR:-}"
if [[ -z "${YALANTINGLIBS_SOURCE_DIR}" ]]; then
  for candidate in \
    "${ROOT_DIR}/build/_deps/yalantinglibs-src" \
    "${ROOT_DIR}/build-l2/_deps/yalantinglibs-src"; do
    if [[ -f "${candidate}/CMakeLists.txt" ]]; then
      YALANTINGLIBS_SOURCE_DIR="${candidate}"
      break
    fi
  done
fi
if [[ -n "${YALANTINGLIBS_SOURCE_DIR}" ]]; then
  echo "yalantinglibs source: ${YALANTINGLIBS_SOURCE_DIR}"
else
  echo "yalantinglibs source: FetchContent (requires network)"
fi

configure() {
  local args=(
    -S "${ROOT_DIR}"
    -B "${BUILD_DIR}"
    -DWITH_STORE=ON
    -DWITH_TE=OFF
    -DWITH_STORE_RUST=OFF
    -DWITH_CONDUCTOR_RUST=OFF
    -DBUILD_UNIT_TESTS=ON
    -DSTORE_USE_ETCD=ON
    -DMOONCAKE_ENABLE_TEST_FAILPOINTS=ON
    -DCMAKE_BUILD_TYPE=Release
  )
  if [[ -n "${GOOGLETEST_SOURCE_DIR}" ]]; then
    args+=("-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=${GOOGLETEST_SOURCE_DIR}")
  fi
  if [[ -n "${YALANTINGLIBS_SOURCE_DIR}" ]]; then
    args+=(
      "-DFETCHCONTENT_SOURCE_DIR_YALANTINGLIBS=${YALANTINGLIBS_SOURCE_DIR}")
  fi
  cmake "${args[@]}"
}

build_targets=(
  mooncake_master
  simple_ha_wal_test
  simple_ha_callback_barrier_test
  simple_ha_restore_lease_test
  simple_ha_snapshot_cursor_test
  simple_ha_drain_test
  simple_ha_l1_harness_test
  simple_ha_supervisor_e2e_test
  simple_ha_smoke_client
)

build() { cmake --build "${BUILD_DIR}" -j "${JOBS}" --target "${build_targets[@]}"; }

if [[ "${SKIP_BUILD}" == "1" ]]; then
  record configure "SKIP (--skip-build)"
  record build "SKIP (--skip-build)"
  if [[ ! -x "${BUILD_DIR}/mooncake-store/src/mooncake_master" ]]; then
    echo "no master binary under ${BUILD_DIR}; remove --skip-build" >&2
    summary
    exit 1
  fi
else
  run_stage configure "${LOG_ROOT}/configure.log" configure || {
    summary
    exit 1
  }
  run_stage build "${LOG_ROOT}/build.log" build || {
    summary
    exit 1
  }
fi

# --- L1 --------------------------------------------------------------------

l1_regex='^simple_ha_(wal|callback_barrier|restore_lease|snapshot_cursor|drain|l1_harness)_test$'
run_l1() {
  ctest --test-dir "${BUILD_DIR}" --output-on-failure --timeout 180 \
    --no-tests=error -R "${l1_regex}"
}
run_stage L1 "${LOG_ROOT}/l1.log" run_l1 || {
  summary
  exit 1
}

# --- L2 --------------------------------------------------------------------

run_l2() {
  ctest --test-dir "${BUILD_DIR}" --output-on-failure --timeout 180 \
    --no-tests=error -R '^simple_ha_supervisor_e2e_test$'
}
run_stage L2 "${LOG_ROOT}/l2.log" run_l2 || {
  summary
  exit 1
}

# --- L3 --------------------------------------------------------------------

if [[ "${SKIP_L3}" == "1" ]]; then
  record L3 "SKIP (--skip-l3)"
else
  run_l3() {
    MASTER_BIN="${BUILD_DIR}/mooncake-store/src/mooncake_master" \
      CLIENT_BIN="${BUILD_DIR}/mooncake-store/tests/simple_ha_smoke_client" \
      KEEP_WORK="${KEEP_WORK}" \
      timeout 300 bash "${ROOT_DIR}/mooncake-store/tests/ha/simple_ha/simple_ha_smoke_e2e.sh"
  }
  run_stage L3 "${LOG_ROOT}/l3.log" run_l3 || {
    summary
    exit 1
  }
fi

summary
