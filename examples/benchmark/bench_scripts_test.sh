#!/bin/bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

fail_test() {
	printf 'FAIL: %s\n' "$*" >&2
	exit 1
}

assert_equals() {
	local got=$1
	local want=$2
	local message=$3
	if [[ "$got" != "$want" ]]; then
		fail_test "$message (got=$got want=$want)"
	fi
}

assert_contains() {
	local haystack=$1
	local needle=$2
	local message=$3
	if [[ "$haystack" != *"$needle"* ]]; then
		fail_test "$message"
	fi
}

assert_not_contains() {
	local haystack=$1
	local needle=$2
	local message=$3
	if [[ "$haystack" == *"$needle"* ]]; then
		fail_test "$message"
	fi
}

new_sandbox() {
	local sandbox
	sandbox=$(mktemp -d)
	mkdir -p "$sandbox/examples/benchmark" "$sandbox/examples/common" "$sandbox/examples/local" "$sandbox/bin" "$sandbox/vtdataroot"
	cp "$REPO_ROOT/examples/benchmark/bench_run.sh" "$sandbox/examples/benchmark/bench_run.sh"
	cp "$REPO_ROOT/examples/benchmark/bench_compare.sh" "$sandbox/examples/benchmark/bench_compare.sh"
	cat > "$sandbox/examples/common/env.sh" <<'EOF'
#!/bin/bash

fail() {
	echo "$*" >&2
	exit 1
}

export VTDATAROOT="${VTDATAROOT:-$PWD/vtdataroot}"
mkdir -p "$VTDATAROOT"
EOF
	cat > "$sandbox/examples/benchmark/bench_generate_load.sh" <<'EOF'
#!/bin/bash
set -euo pipefail
printf '%s\n' "${LOAD_TYPE:-unset}:${ROW_COUNT:-unset}" >> "$BENCH_TEST_TMP/load_calls"
EOF
	cat > "$sandbox/examples/local/501_teardown.sh" <<'EOF'
#!/bin/bash
set -euo pipefail
printf 'teardown\n' >> "$BENCH_TEST_TMP/teardown_calls"
EOF
	cat > "$sandbox/bin/vtctldclient" <<'EOF'
#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> "$BENCH_TEST_TMP/vtctld_calls"
if [[ "${1:-}" == "GetTablets" ]]; then
	if [[ "$*" == *"--keyspace commerce"* ]]; then
		printf 'zone1-0000000100 primary\n'
	else
		printf 'zone1-0000000200 primary\n'
	fi
fi
EOF
	cat > "$sandbox/bin/mysql" <<'EOF'
#!/bin/bash
set -euo pipefail

query=""
while (($#)); do
	case "$1" in
		-e)
			query=$2
			shift 2
			;;
		--no-defaults|--binary-as-hex=false|-N)
			shift
			;;
		-h|-P|-u|-S)
			shift 2
			;;
		*)
			shift
			;;
	esac
done

state_calls_file="$BENCH_TEST_TMP/mysql_state_calls"
case "$query" in
	*"SELECT state FROM _vt.vreplication WHERE workflow='bench_move'"*)
		state_calls=0
		if [[ -f "$state_calls_file" ]]; then
			state_calls=$(cat "$state_calls_file")
		fi
		state_calls=$((state_calls + 1))
		printf '%s' "$state_calls" > "$state_calls_file"
		if [[ "$state_calls" -eq 1 ]]; then
			printf 'Running\n'
		else
			printf 'Stopped\n'
		fi
		;;
	*"ALTER TABLE bench_orders"*)
		if [[ "${BENCH_FAIL_FIRST_TARGET_ALTER:-0}" == "1" ]]; then
			echo 'simulated index build failure' >&2
			exit 1
		fi
		;;
	*"ALTER TABLE bench_events"*|*"ALTER TABLE bench_accounts"*|*"ALTER TABLE bench_logs"*)
		;;
	*"SELECT @@gtid_executed"*)
		printf 'uuid:1-100\n'
		;;
	*"SELECT pos FROM _vt.vreplication WHERE workflow='bench_move'"*)
		printf 'uuid:1-50\n'
		;;
	*"SELECT UNIX_TIMESTAMP() - FLOOR(time_updated) FROM _vt.vreplication WHERE workflow='bench_move'"*)
		printf '999\n'
		;;
	*"SELECT COUNT(*), COALESCE(BIT_XOR("*)
		printf '1 2\n'
		;;
	esac
EOF
	cat > "$sandbox/bin/date" <<'EOF'
#!/bin/bash
set -euo pipefail
if [[ "${1:-}" != "+%s" ]]; then
	/bin/date "$@"
	exit 0
fi

calls_file="$BENCH_TEST_TMP/date_calls"
calls=0
if [[ -f "$calls_file" ]]; then
	calls=$(cat "$calls_file")
fi
calls=$((calls + 1))
printf '%s' "$calls" > "$calls_file"

case "$calls" in
	1)
		printf '0\n'
		;;
	2)
		printf '%s\n' "${BENCH_TIMEOUT_ELAPSED:-7200}"
		;;
	*)
		printf '%s\n' "${BENCH_TIMEOUT_ELAPSED_END:-7201}"
		;;
esac
EOF
	chmod +x "$sandbox/examples/common/env.sh" "$sandbox/examples/benchmark/bench_generate_load.sh" "$sandbox/examples/local/501_teardown.sh" "$sandbox/bin/vtctldclient" "$sandbox/bin/mysql" "$sandbox/bin/date"
	printf '%s\n' "$sandbox"
}

test_bench_run_timeout_fails_without_results() {
	local sandbox output status
	sandbox=$(new_sandbox)
	trap 'rm -rf "$sandbox"' RETURN
	output=$(cd "$sandbox/examples/benchmark" && BENCH_TEST_TMP="$sandbox" VTDATAROOT="$sandbox/vtdataroot" PATH="$sandbox/bin:$PATH" bash ./bench_run.sh 2>&1) || status=$?
	status=${status:-0}

	if [[ "$status" -eq 0 ]]; then
		fail_test "bench_run timeout should fail"
	fi
	assert_contains "$output" "Timed out after" "bench_run should report the timeout"
	assert_not_contains "$output" "BENCHMARK RESULTS" "bench_run should not print results after a timeout"
	assert_contains "$(cat "$sandbox/vtctld_calls")" "MoveTables --workflow bench_move --target-keyspace customer cancel" "bench_run should clean up the workflow after a timeout"
	trap - RETURN
	rm -rf "$sandbox"
}

test_bench_run_index_failure_is_fatal() {
	local sandbox output status load_calls
	sandbox=$(new_sandbox)
	trap 'rm -rf "$sandbox"' RETURN
	output=$(cd "$sandbox/examples/benchmark" && BENCH_TEST_TMP="$sandbox" BENCH_FAIL_FIRST_TARGET_ALTER=1 VTDATAROOT="$sandbox/vtdataroot" PATH="$sandbox/bin:$PATH" bash ./bench_run.sh 2>&1) || status=$?
	status=${status:-0}

	if [[ "$status" -eq 0 ]]; then
		fail_test "bench_run should fail when target index creation fails"
	fi
	assert_not_contains "$output" "Target indexes added (~25 per table)." "bench_run should not report index success after an index build failure"
	load_calls=$(cat "$sandbox/load_calls")
	assert_equals "$load_calls" "seed:40000" "bench_run should stop before generating the mixed backlog when index creation fails"
	assert_contains "$(cat "$sandbox/vtctld_calls")" "MoveTables --workflow bench_move --target-keyspace customer cancel" "bench_run should clean up the workflow after an index build failure"
	trap - RETURN
	rm -rf "$sandbox"
}

test_bench_compare_can_run_parallel_first() {
	local sandbox output order
	sandbox=$(mktemp -d)
	trap 'rm -rf "$sandbox"; rm -f /tmp/bench_1_workers.log /tmp/bench_4_workers.log' RETURN
	mkdir -p "$sandbox/examples/benchmark" "$sandbox/examples/local"
	cp "$REPO_ROOT/examples/benchmark/bench_compare.sh" "$sandbox/examples/benchmark/bench_compare.sh"
	cat > "$sandbox/examples/benchmark/bench_setup.sh" <<'EOF'
#!/bin/bash
set -euo pipefail
printf '%s\n' "$PARALLEL_WORKERS" >> "$BENCH_TEST_TMP/setup_order"
EOF
	cat > "$sandbox/examples/benchmark/bench_run.sh" <<'EOF'
#!/bin/bash
set -euo pipefail
echo "  Backlog ops:    200000"
echo "  Seed rows:      40000"
echo "  Drain time:     10s"
echo "  Throughput:     20000 ops/sec"
EOF
	cat > "$sandbox/examples/local/501_teardown.sh" <<'EOF'
#!/bin/bash
set -euo pipefail
:
EOF
	chmod +x "$sandbox/examples/benchmark/bench_setup.sh" "$sandbox/examples/benchmark/bench_run.sh" "$sandbox/examples/local/501_teardown.sh"

	output=$(cd "$sandbox/examples/benchmark" && BENCH_TEST_TMP="$sandbox" RUN_ORDER=parallel-first bash ./bench_compare.sh 2>&1)
	order=$(paste -sd ',' "$sandbox/setup_order")
	assert_equals "$order" "4,1" "bench_compare should honor RUN_ORDER=parallel-first"
	assert_contains "$output" "Run order: parallel-first" "bench_compare should print the selected run order"
	trap - RETURN
	rm -rf "$sandbox"
	rm -f /tmp/bench_1_workers.log /tmp/bench_4_workers.log
}

test_bench_run_timeout_fails_without_results
test_bench_run_index_failure_is_fatal
test_bench_compare_can_run_parallel_first

echo "PASS: benchmark script regressions"
