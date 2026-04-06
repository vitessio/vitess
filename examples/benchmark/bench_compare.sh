#!/bin/bash

# A/B comparison: serial (workers=1) vs parallel (workers=4) VReplication applier
# with mixed write workload (INSERT/UPDATE/DELETE/bulk operations).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

ROW_COUNT=${ROW_COUNT:-200000}
SEED_ROWS=${SEED_ROWS:-10000}
export ROW_COUNT SEED_ROWS

echo "============================================"
echo "  VReplication Parallel Applier Benchmark"
echo "  ROW_COUNT=$ROW_COUNT  SEED_ROWS=$SEED_ROWS"
echo "============================================"
echo ""

run_bench() {
	local workers=$1
	local label=$2

	echo ">>> Run: $label (PARALLEL_WORKERS=$workers) <<<"
	echo ""

	# Teardown any previous state
	(cd "$SCRIPT_DIR/../local" && ./501_teardown.sh) 2>/dev/null

	# Setup cluster with specified worker count
	PARALLEL_WORKERS=$workers ./bench_setup.sh || { echo "FAILED: setup for $label"; return 1; }

	# Run benchmark. Use pipefail so a bench_run.sh validation failure is not
	# masked by tee's zero exit status.
	(
		set -o pipefail
		./bench_run.sh 2>&1 | tee "/tmp/bench_${workers}_workers.log"
	) || { echo "FAILED: bench_run for $label (validation or drain failure)"; return 1; }

	echo ""
	echo ">>> $label complete <<<"
	echo ""
}

# Run 1: Serial (1 worker)
run_bench 1 "Serial (1 worker)" || exit 1

# Teardown between runs
echo "Tearing down between runs..."
(cd "$SCRIPT_DIR/../local" && ./501_teardown.sh) 2>/dev/null
sleep 3

# Run 2: Parallel (4 workers)
run_bench 4 "Parallel (4 workers)" || exit 1

# Teardown after
echo "Tearing down after benchmark..."
(cd "$SCRIPT_DIR/../local" && ./501_teardown.sh) 2>/dev/null

# Compare results
echo ""
echo "============================================"
echo "  COMPARISON"
echo "============================================"

for workers in 1 4; do
	logfile="/tmp/bench_${workers}_workers.log"
	if [[ -f "$logfile" ]]; then
		echo ""
		echo "--- Workers=$workers ---"
		grep -E "(Drain time|Throughput|Backlog ops|Seed rows)" "$logfile"
	fi
done

# Calculate speedup if both logs exist
serial_log="/tmp/bench_1_workers.log"
parallel_log="/tmp/bench_4_workers.log"
if [[ -f "$serial_log" ]] && [[ -f "$parallel_log" ]]; then
	serial_time=$(grep "Drain time" "$serial_log" | grep -o '[0-9]*')
	parallel_time=$(grep "Drain time" "$parallel_log" | grep -o '[0-9]*')
	if [[ -n "$serial_time" ]] && [[ -n "$parallel_time" ]] && [[ "$parallel_time" -gt 0 ]]; then
		# Integer math: multiply by 100 for 2 decimal places
		speedup_x100=$((serial_time * 100 / parallel_time))
		speedup_whole=$((speedup_x100 / 100))
		speedup_frac=$((speedup_x100 % 100))
		printf -v speedup_str '%d.%02d' "$speedup_whole" "$speedup_frac"
		echo ""
		echo "--- Speedup ---"
		echo "  Serial:   ${serial_time}s"
		echo "  Parallel: ${parallel_time}s"
		echo "  Speedup:  ${speedup_str}x"
	fi
fi

echo ""
echo "============================================"
echo "Full logs: /tmp/bench_1_workers.log and /tmp/bench_4_workers.log"
echo "============================================"
