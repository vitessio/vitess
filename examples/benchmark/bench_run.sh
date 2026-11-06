#!/bin/bash

# Run the VReplication parallel applier benchmark with mixed write workload.
# Prerequisites: bench_setup.sh must have been run first.
#
# Flow:
#   1. Seed source tables with initial data (for UPDATE/DELETE targets)
#   2. Create MoveTables workflow, copy seed data, stop
#   3. Generate mixed backlog (INSERT/UPDATE/DELETE/bulk) while stopped
#   4. Start workflow, time drain until lag reaches 0
#
# Environment variables:
#   ROW_COUNT   — total backlog operations (default 200000)
#   SEED_ROWS   — seed rows per table for UPDATE/DELETE targets (default 10000)

source ../common/env.sh

TOTAL_OPS=${ROW_COUNT:-200000}
SEED_ROWS=${SEED_ROWS:-10000}
TOTAL_SEED=$((SEED_ROWS * 4))
BENCH_TABLES="bench_orders,bench_events,bench_accounts,bench_logs"

source_mysql() {
	command mysql --no-defaults -h 127.0.0.1 -P 15306 --binary-as-hex=false "$@"
}

# Find the primary tablet for a keyspace and return its MySQL socket path
detect_tablet_socket() {
	local ks=$1
	local primary_tablet
	primary_tablet=$(vtctldclient GetTablets --keyspace "$ks" --shard 0 2>/dev/null | grep -w primary | awk '{print $1}')
	if [[ -z "$primary_tablet" ]]; then
		fail "Could not find primary tablet for $ks keyspace"
	fi
	local uid
	uid=$(echo "$primary_tablet" | sed 's/.*-0*//')
	local sock="$VTDATAROOT/vt_$(printf '%010d' "$uid")/mysql.sock"
	echo "$sock"
}

detect_primaries() {
	SOURCE_SOCKET=$(detect_tablet_socket commerce)
	TARGET_SOCKET=$(detect_tablet_socket customer)
	echo "Source socket: $SOURCE_SOCKET"
	echo "Target socket: $TARGET_SOCKET"
}

source_direct_mysql() {
	command mysql --no-defaults -u vt_dba -S "$SOURCE_SOCKET" "$@"
}

target_mysql() {
	command mysql --no-defaults -u vt_dba -S "$TARGET_SOCKET" "$@"
}

# Extract the max GTID transaction sequence number from a GTID set string.
# Handles formats like "uuid:1-N" and "MySQL56/uuid:1-N".
max_gtid_seq() {
	echo "$1" | tr ',' '\n' | grep -oE ':[0-9]+-[0-9]+' | grep -oE '[0-9]+$' | sort -n | tail -1
}

# Get the target's current replication position from _vt.vreplication
target_pos() {
	target_mysql -N -e \
		"SELECT pos FROM _vt.vreplication WHERE workflow='bench_move'" 2>/dev/null
}

# Get replication lag in seconds (for display only, not reliable for drain detection)
replication_lag() {
	target_mysql -N -e \
		"SELECT UNIX_TIMESTAMP() - FLOOR(time_updated) FROM _vt.vreplication WHERE workflow='bench_move'" 2>/dev/null
}

echo "=== Bench Run (ROW_COUNT=$TOTAL_OPS, SEED_ROWS=$SEED_ROWS) ==="

detect_primaries

cleanup_workflow() {
	vtctldclient MoveTables --workflow bench_move --target-keyspace customer cancel 2>/dev/null
}

timeout_failed=0

add_target_indexes() {
	target_mysql vt_customer -e "
  ALTER TABLE bench_orders
    ADD INDEX idx_name_status (customer_name, status),
    ADD INDEX idx_name_region_qty (customer_name, region, quantity),
    ADD INDEX idx_sku_status_region (product_sku, status, region),
    ADD INDEX idx_region_status_price (region, status, total_price),
    ADD INDEX idx_notes_prefix (notes(255)),
    ADD INDEX idx_qty_price (quantity, total_price),
    ADD INDEX idx_status_qty_price (status, quantity, total_price),
    ADD INDEX idx_sku_qty (product_sku, quantity),
    ADD INDEX idx_name_price (customer_name, total_price),
    ADD INDEX idx_region_qty_price (region, quantity, total_price),
    ADD INDEX idx_status_name (status, customer_name),
    ADD INDEX idx_sku_region (product_sku, region),
    ADD INDEX idx_status_sku_price (status, product_sku, total_price),
    ADD INDEX idx_name_qty_status (customer_name, quantity, status),
    ADD INDEX idx_region_name (region, customer_name),
    ADD INDEX idx_sku_name_region (product_sku, customer_name, region),
    ADD INDEX idx_qty_status_region (quantity, status, region),
    ADD INDEX idx_price_status (total_price, status),
    ADD INDEX idx_price_region_name (total_price, region, customer_name),
    ADD INDEX idx_notes_prefix2 (notes(128));
" || return 1

	target_mysql vt_customer -e "
  ALTER TABLE bench_events
    ADD INDEX idx_source_type (source, event_type),
    ADD INDEX idx_type_category (event_type, category),
    ADD INDEX idx_category_severity (category, severity),
    ADD INDEX idx_created_severity (created_at, severity),
    ADD INDEX idx_source_category (source, category),
    ADD INDEX idx_payload_prefix (payload(255)),
    ADD INDEX idx_type_created_severity (event_type, created_at, severity),
    ADD INDEX idx_source_severity (source, severity),
    ADD INDEX idx_category_created (category, created_at),
    ADD INDEX idx_type_source_severity (event_type, source, severity),
    ADD INDEX idx_severity_category (severity, category),
    ADD INDEX idx_created_type (created_at, event_type),
    ADD INDEX idx_source_created_type (source, created_at, event_type),
    ADD INDEX idx_category_type_created (category, event_type, created_at),
    ADD INDEX idx_severity_source (severity, source),
    ADD INDEX idx_type_severity_created (event_type, severity, created_at),
    ADD INDEX idx_created_category_severity (created_at, category, severity),
    ADD INDEX idx_source_type_category (source, event_type, category),
    ADD INDEX idx_severity_type_source (severity, event_type, source),
    ADD INDEX idx_payload_prefix2 (payload(128));
" || return 1

	target_mysql vt_customer -e "
  ALTER TABLE bench_accounts
    ADD INDEX idx_username_tier (username, tier),
    ADD INDEX idx_email_region (email, region),
    ADD INDEX idx_tier_balance (tier, balance),
    ADD INDEX idx_region_tier (region, tier),
    ADD INDEX idx_bio_prefix (bio(255)),
    ADD INDEX idx_tier_region_balance (tier, region, balance),
    ADD INDEX idx_username_balance (username, balance),
    ADD INDEX idx_email_tier (email, tier),
    ADD INDEX idx_username_region (username, region),
    ADD INDEX idx_balance_tier (balance, tier),
    ADD INDEX idx_region_balance_tier (region, balance, tier),
    ADD INDEX idx_tier_username (tier, username),
    ADD INDEX idx_email_balance (email, balance),
    ADD INDEX idx_region_username (region, username),
    ADD INDEX idx_username_tier_balance (username, tier, balance),
    ADD INDEX idx_tier_email (tier, email),
    ADD INDEX idx_balance_region (balance, region),
    ADD INDEX idx_email_tier_region (email, tier, region),
    ADD INDEX idx_region_email_balance (region, email, balance),
    ADD INDEX idx_bio_prefix2 (bio(128));
" || return 1

	target_mysql vt_customer -e "
  ALTER TABLE bench_logs
    ADD INDEX idx_component_level (component, level),
    ADD INDEX idx_trace_span (trace_id, span_id),
    ADD INDEX idx_level_error (level, error_code),
    ADD INDEX idx_component_error (component, error_code),
    ADD INDEX idx_message_prefix (message(255)),
    ADD INDEX idx_span_level (span_id, level),
    ADD INDEX idx_error_component_level (error_code, component, level),
    ADD INDEX idx_level_component_error (level, component, error_code),
    ADD INDEX idx_trace_level (trace_id, level),
    ADD INDEX idx_component_trace (component, trace_id),
    ADD INDEX idx_error_level (error_code, level),
    ADD INDEX idx_span_component (span_id, component),
    ADD INDEX idx_level_trace (level, trace_id),
    ADD INDEX idx_trace_component_level (trace_id, component, level),
    ADD INDEX idx_error_span (error_code, span_id),
    ADD INDEX idx_component_span_level (component, span_id, level),
    ADD INDEX idx_level_span_error (level, span_id, error_code),
    ADD INDEX idx_span_error_component (span_id, error_code, component),
    ADD INDEX idx_trace_error (trace_id, error_code),
    ADD INDEX idx_message_prefix2 (message(128));
" || return 1
}

# Step 1: Seed source tables with initial data
# Retry the seed step: vtgate's connection pool to the primary tablet can be
# briefly unavailable right after cluster startup, surfacing as
# "connection pool is closed" when the seed script runs too soon.
echo ""
echo "Seeding source tables ($SEED_ROWS rows per table = $TOTAL_SEED total)..."
seed_attempts=0
seed_max_attempts=3
until LOAD_TYPE=seed ROW_COUNT=$TOTAL_SEED ./bench_generate_load.sh; do
	seed_attempts=$((seed_attempts+1))
	if [[ $seed_attempts -ge $seed_max_attempts ]]; then
		fail "Failed to seed data after $seed_max_attempts attempts"
	fi
	echo "Seed failed (attempt $seed_attempts); retrying in 10s..."
	sleep 10
done

# Step 2: Create MoveTables workflow (auto-start, copies seed data)
echo ""
echo "Creating MoveTables workflow..."
vtctldclient MoveTables --workflow bench_move --target-keyspace customer create \
	--source-keyspace commerce \
	--tables "$BENCH_TABLES" || fail "Failed to create MoveTables workflow"

# Step 3: Wait for copy phase to complete (state transitions to Running)
echo "Waiting for copy phase to complete..."
max_wait=600
for i in $(seq 1 $max_wait); do
	state=$(target_mysql -N -e \
		"SELECT state FROM _vt.vreplication WHERE workflow='bench_move'" 2>/dev/null | head -1)
	if [[ "$state" == "Running" ]]; then
		echo "Copy phase complete, workflow is running."
		break
	fi
	if [[ $((i % 10)) -eq 0 ]]; then
		echo "  ...still copying (state=$state, ${i}s elapsed)"
	fi
	sleep 1
done

if [[ "$state" != "Running" ]]; then
	fail "Timed out waiting for copy phase to complete (state=$state)"
fi

# Step 4: Stop the workflow so we can build a backlog
echo "Stopping workflow..."
vtctldclient MoveTables --workflow bench_move --target-keyspace customer stop || fail "Failed to stop workflow"

for i in $(seq 1 30); do
	state=$(target_mysql -N -e \
		"SELECT state FROM _vt.vreplication WHERE workflow='bench_move'" 2>/dev/null | head -1)
	if [[ "$state" == "Stopped" ]]; then
		break
	fi
	sleep 1
done

if [[ "$state" != "Stopped" ]]; then
	fail "Workflow did not stop (state=$state)"
fi
echo "Workflow stopped."

# Step 4b: Add extra indexes on the TARGET to increase per-statement MySQL cost.
# The source keeps lightweight indexes so the vstreamer produces events fast.
# Heavy target indexes make the applier the bottleneck, allowing parallel workers
# to demonstrate their advantage by overlapping expensive index maintenance.
# With ~25 indexes per table and an 8MB buffer pool, each INSERT/UPDATE/DELETE
# requires many random page reads that can be overlapped by parallel workers.
echo ""
echo "Adding extra indexes on target to increase applier workload..."
add_target_indexes || {
	echo "ERROR: failed to add target indexes"
	cleanup_workflow
	exit 1
}

echo "Target indexes added (~25 per table)."

# Step 5: Generate mixed backlog on source
echo ""
echo "Generating mixed backlog on source ($TOTAL_OPS operations)..."
LOAD_TYPE=mixed ROW_COUNT=$TOTAL_OPS SEED_ROWS=$SEED_ROWS ./bench_generate_load.sh || fail "Failed to generate backlog"

# Step 5b: Capture source GTID position after backlog generation.
# This is the definitive marker — when the target's pos reaches this point,
# all backlog events have been applied.
source_gtid=$(source_direct_mysql -N -e "SELECT @@gtid_executed" 2>/dev/null | tr -d '[:space:]')
source_seq=$(max_gtid_seq "$source_gtid")
echo "Source GTID seq after backlog: $source_seq"

if [[ -z "$source_seq" ]] || [[ "$source_seq" -eq 0 ]]; then
	fail "Could not capture source GTID position"
fi

# Step 6: Record start time and start the workflow
echo ""
echo "Starting workflow to drain backlog..."
start_time=$(date +%s)

vtctldclient MoveTables --workflow bench_move --target-keyspace customer start || fail "Failed to start workflow"

# Step 7: Poll until target GTID position catches up to source.
# We use GTID comparison instead of time_updated lag because:
# - time_updated is refreshed by the controller loop regardless of applier progress
# - With parallel workers, the controller doesn't block on the applier, so
#   time_updated stays near-current even while the backlog is being processed
# - GTID position accurately reflects committed progress
echo "Waiting for target to catch up (source_seq=$source_seq)..."
last_report=0
while true; do
	now=$(date +%s)
	elapsed=$((now - start_time))

	# Get target's current replicated position
	tpos=$(target_pos)
	target_seq=$(max_gtid_seq "$tpos")

	# Check if target has caught up to source
	if [[ -n "$target_seq" ]] && [[ "$target_seq" =~ ^[0-9]+$ ]] && [[ "$target_seq" -ge "$source_seq" ]]; then
		echo "Target caught up! (target_seq=$target_seq >= source_seq=$source_seq, ${elapsed}s elapsed)"
		break
	fi

	if [[ $((elapsed - last_report)) -ge 5 ]]; then
		lag=$(replication_lag)
		pct=""
		if [[ -n "$target_seq" ]] && [[ "$target_seq" =~ ^[0-9]+$ ]] && [[ "$source_seq" -gt 0 ]]; then
			pct=" $(( target_seq * 100 / source_seq ))%"
		fi
		echo "  ...draining (pos=${target_seq:-?}/${source_seq}${pct} lag=${lag:-?}s ${elapsed}s)"
		last_report=$elapsed
	fi

	if [[ "$elapsed" -ge 7200 ]]; then
		echo "ERROR: Timed out after ${elapsed}s (target_seq=${target_seq:-?})"
		timeout_failed=1
		break
	fi

	sleep 1
done

if [[ "$timeout_failed" -ne 0 ]]; then
	echo ""
	echo "ERROR: drain timed out before reaching source GTID position"
	cleanup_workflow
	exit 1
fi

end_time=$(date +%s)

# Step 8: Calculate and report results
elapsed_s=$((end_time - start_time))

echo ""
echo "============================================"
echo "  BENCHMARK RESULTS"
echo "============================================"
echo "  Backlog ops:    $TOTAL_OPS"
echo "  Seed rows:      $TOTAL_SEED"
echo "  Drain time:     ${elapsed_s}s"
if [ "$elapsed_s" -gt 0 ]; then
	echo "  Throughput:     $((TOTAL_OPS / elapsed_s)) ops/sec"
fi
echo "============================================"

# Step 9: Validate source and target are semantically equivalent (only after
# drain, when target is idle). COUNT(*) alone is too weak: reordering errors,
# wrong-row updates, or corrupted values can preserve cardinality while changing
# row content. We compute a content checksum per table that is order-independent
# (BIT_XOR of CRC32 over all column values) so parallel apply reordering is
# not flagged as divergence as long as the final state is equivalent.
echo ""
echo "Validating row counts and content checksums..."
validation_failed=0

# Returns the column list for the given table. These must match
# create_bench_schema.sql exactly — keep in sync. Using a function here
# instead of an associative array for bash 3.2 compatibility (macOS).
table_columns() {
	case "$1" in
		bench_orders)   echo "id,customer_name,product_sku,quantity,total_price,status,region,notes" ;;
		bench_events)   echo "id,event_type,source,payload,severity,created_at,category" ;;
		bench_accounts) echo "id,username,email,balance,region,bio,tier" ;;
		bench_logs)     echo "id,level,message,component,error_code,trace_id,span_id" ;;
	esac
}

for table in bench_orders bench_events bench_accounts bench_logs; do
	cols="$(table_columns "$table")"
	# Build CONCAT_WS over all columns with IFNULL so NULLs don't collapse the row.
	concat_expr="CONCAT_WS('|'"
	old_ifs="$IFS"
	IFS=','
	for col in $cols; do
		concat_expr="$concat_expr,IFNULL($col,'\\0')"
	done
	IFS="$old_ifs"
	concat_expr="$concat_expr)"
	checksum_sql="SELECT COUNT(*), COALESCE(BIT_XOR(CAST(CRC32($concat_expr) AS UNSIGNED)), 0) FROM"

	source_row=$(source_mysql -N -e "$checksum_sql commerce.$table" 2>/dev/null)
	target_row=$(target_mysql -N -e "$checksum_sql vt_customer.$table" 2>/dev/null)
	source_count=$(echo "$source_row" | awk '{print $1}')
	source_cksum=$(echo "$source_row" | awk '{print $2}')
	target_count=$(echo "$target_row" | awk '{print $1}')
	target_cksum=$(echo "$target_row" | awk '{print $2}')

	match="OK"
	if [[ "$source_count" != "$target_count" ]] || [[ "$source_cksum" != "$target_cksum" ]]; then
		match="MISMATCH"
		validation_failed=1
	fi
	echo "  $table: source=(count=$source_count, cksum=$source_cksum) target=(count=$target_count, cksum=$target_cksum) [$match]"
done

if [[ "$validation_failed" -ne 0 ]]; then
	echo ""
	echo "ERROR: validation FAILED — source and target diverged. See mismatches above."
	echo "=== Bench Run Failed ==="
	# Still attempt workflow cleanup before exiting.
	cleanup_workflow
	exit 1
fi

# Step 10: Cleanup workflow
echo ""
echo "Cleaning up workflow..."
cleanup_workflow

echo "=== Bench Run Complete ==="
