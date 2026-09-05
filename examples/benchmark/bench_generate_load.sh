#!/bin/bash

# Generate workload for VReplication benchmark. Supports two modes:
#   LOAD_TYPE=seed   — INSERT-only (builds base data for UPDATE/DELETE targets)
#   LOAD_TYPE=mixed  — Mixed INSERT/UPDATE/DELETE/bulk operations
#
# The random generator uses a FIXED SEED so output is deterministic and
# benchmark runs are repeatable for proper A/B comparisons.
#
# Environment variables:
#   ROW_COUNT    — total operations to generate (default 5000000)
#   LOAD_TYPE    — "seed" or "mixed" (default "mixed")
#   SEED_ROWS    — rows per table available for UPDATE/DELETE (used in mixed mode)

source ../common/env.sh

TOTAL_OPS=${ROW_COUNT:-200000}
OPS_PER_TABLE=$((TOTAL_OPS / 4))
LOAD_TYPE=${LOAD_TYPE:-mixed}
SEED_ROWS=${SEED_ROWS:-10000}

echo "=== Generating Load: $TOTAL_OPS total ops ($OPS_PER_TABLE per table, type=$LOAD_TYPE) ==="

TMPDIR="$VTDATAROOT/tmp/bench_load"
mkdir -p "$TMPDIR"

python3 -c "
import random
import string
import os

ops_per_table = $OPS_PER_TABLE
load_type = '$LOAD_TYPE'
seed_rows = $SEED_ROWS
tmpdir = '$TMPDIR'

# Fixed seed for deterministic, repeatable output.
random.seed(42)

# Pre-compute a pool of random strings to avoid per-row generation cost.
_pool_size = 10000
_str_pool = {}
def _init_pool(n):
    if n not in _str_pool:
        chars = string.ascii_letters + string.digits
        _str_pool[n] = [''.join(random.choices(chars, k=n)) for _ in range(_pool_size)]
def rand_str(n):
    pool = _str_pool.get(n)
    if pool is None:
        _init_pool(n)
        pool = _str_pool[n]
    return pool[random.randint(0, _pool_size - 1)]

def gen_insert_orders(f):
    name = rand_str(60)
    sku = rand_str(40)
    qty = random.randint(1, 100)
    price = random.randint(100, 100000)
    status = random.choice(['pending', 'shipped', 'delivered', 'cancelled', 'returned', 'processing'])
    region = random.choice(['us-east-1', 'us-west-2', 'eu-west-1', 'eu-central-1', 'ap-south-1', 'ap-east-1'])
    notes = rand_str(400)
    f.write(f\"INSERT INTO bench_orders (customer_name, product_sku, quantity, total_price, status, region, notes) VALUES ('{name}', '{sku}', {qty}, {price}, '{status}', '{region}', '{notes}');\n\")

def gen_insert_events(f):
    etype = random.choice(['click', 'purchase', 'view', 'signup', 'logout', 'error', 'timeout', 'retry'])
    source = rand_str(60)
    payload = rand_str(600)
    severity = random.randint(1, 10)
    created = random.randint(1700000000, 1800000000)
    category = rand_str(40)
    f.write(f\"INSERT INTO bench_events (event_type, source, payload, severity, created_at, category) VALUES ('{etype}', '{source}', '{payload}', {severity}, {created}, '{category}');\n\")

def gen_insert_accounts(f):
    username = rand_str(40)
    email = rand_str(30) + '@' + rand_str(20) + '.com'
    balance = random.randint(0, 1000000)
    region = random.choice(['us-east-1', 'us-west-2', 'eu-west-1', 'eu-central-1', 'ap-south-1', 'ap-east-1'])
    bio = rand_str(400)
    tier = random.choice(['free', 'basic', 'pro', 'enterprise', 'unlimited'])
    f.write(f\"INSERT INTO bench_accounts (username, email, balance, region, bio, tier) VALUES ('{username}', '{email}', {balance}, '{region}', '{bio}', '{tier}');\n\")

def gen_insert_logs(f):
    level = random.choice(['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'])
    message = rand_str(400)
    component = random.choice(['api', 'worker', 'scheduler', 'gateway', 'cache', 'auth', 'billing', 'storage'])
    error_code = random.randint(0, 9999)
    trace_id = rand_str(32)
    span_id = rand_str(16)
    f.write(f\"INSERT INTO bench_logs (level, message, component, error_code, trace_id, span_id) VALUES ('{level}', '{message}', '{component}', {error_code}, '{trace_id}', '{span_id}');\n\")

insert_fns = {
    'orders': gen_insert_orders,
    'events': gen_insert_events,
    'accounts': gen_insert_accounts,
    'logs': gen_insert_logs,
}

# UPDATE generators — modify multiple indexed columns to create significant MySQL work
def gen_update_orders(f, pk):
    name = rand_str(60)
    status = random.choice(['pending', 'shipped', 'delivered', 'cancelled', 'returned', 'processing'])
    region = random.choice(['us-east-1', 'us-west-2', 'eu-west-1', 'eu-central-1', 'ap-south-1', 'ap-east-1'])
    notes = rand_str(400)
    f.write(f\"UPDATE bench_orders SET customer_name='{name}', status='{status}', region='{region}', notes='{notes}' WHERE id={pk};\n\")

def gen_update_events(f, pk):
    etype = random.choice(['click', 'purchase', 'view', 'signup', 'logout', 'error', 'timeout', 'retry'])
    source = rand_str(60)
    payload = rand_str(600)
    category = rand_str(40)
    f.write(f\"UPDATE bench_events SET event_type='{etype}', source='{source}', payload='{payload}', category='{category}' WHERE id={pk};\n\")

def gen_update_accounts(f, pk):
    username = rand_str(40)
    email = rand_str(30) + '@' + rand_str(20) + '.com'
    balance = random.randint(0, 1000000)
    bio = rand_str(400)
    tier = random.choice(['free', 'basic', 'pro', 'enterprise', 'unlimited'])
    f.write(f\"UPDATE bench_accounts SET username='{username}', email='{email}', balance={balance}, bio='{bio}', tier='{tier}' WHERE id={pk};\n\")

def gen_update_logs(f, pk):
    level = random.choice(['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'])
    message = rand_str(400)
    component = random.choice(['api', 'worker', 'scheduler', 'gateway', 'cache', 'auth', 'billing', 'storage'])
    error_code = random.randint(0, 9999)
    f.write(f\"UPDATE bench_logs SET level='{level}', message='{message}', component='{component}', error_code={error_code} WHERE id={pk};\n\")

update_fns = {
    'orders': gen_update_orders,
    'events': gen_update_events,
    'accounts': gen_update_accounts,
    'logs': gen_update_logs,
}

# Bulk UPDATE generators — update N rows in one statement
def gen_bulk_update(table, f, pks):
    pk_list = ','.join(str(p) for p in pks)
    if table == 'orders':
        status = random.choice(['pending', 'shipped', 'delivered', 'cancelled', 'returned', 'processing'])
        region = random.choice(['us-east-1', 'us-west-2', 'eu-west-1', 'eu-central-1', 'ap-south-1', 'ap-east-1'])
        notes = rand_str(400)
        f.write(f\"UPDATE bench_orders SET status='{status}', region='{region}', notes='{notes}' WHERE id IN ({pk_list});\n\")
    elif table == 'events':
        etype = random.choice(['click', 'purchase', 'view', 'signup', 'logout', 'error', 'timeout', 'retry'])
        payload = rand_str(600)
        f.write(f\"UPDATE bench_events SET event_type='{etype}', payload='{payload}' WHERE id IN ({pk_list});\n\")
    elif table == 'accounts':
        balance = random.randint(0, 1000000)
        tier = random.choice(['free', 'basic', 'pro', 'enterprise', 'unlimited'])
        f.write(f\"UPDATE bench_accounts SET balance={balance}, tier='{tier}' WHERE id IN ({pk_list});\n\")
    elif table == 'logs':
        level = random.choice(['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'])
        message = rand_str(400)
        f.write(f\"UPDATE bench_logs SET level='{level}', message='{message}' WHERE id IN ({pk_list});\n\")

def gen_bulk_delete(table, f, pks):
    pk_list = ','.join(str(p) for p in pks)
    f.write(f\"DELETE FROM bench_{table} WHERE id IN ({pk_list});\n\")

tables = ['orders', 'events', 'accounts', 'logs']

if load_type == 'seed':
    # Seed mode: INSERT-only, one file per table
    for table in tables:
        fn = insert_fns[table]
        with open(os.path.join(tmpdir, f'{table}.sql'), 'w') as f:
            for _ in range(ops_per_table):
                fn(f)
    print('Seed SQL files generated.')
else:
    # Mixed mode: diverse write operations
    # Operation mix (as fractions of total per table):
    #   50% single-row INSERT  — light txns, good for serial batching
    #   20% single-row UPDATE  — medium txns, index maintenance
    #   5%  single-row DELETE  — light txns
    #   15% bulk UPDATE (5-15 rows) — heavy txns, lots of row events
    #   10% bulk DELETE (3-8 rows)  — medium-heavy txns
    for table in tables:
        insert_fn = insert_fns[table]
        update_fn = update_fns[table]
        with open(os.path.join(tmpdir, f'{table}.sql'), 'w') as f:
            for i in range(ops_per_table):
                r = random.random()
                if r < 0.50:
                    # Single-row INSERT
                    insert_fn(f)
                elif r < 0.70:
                    # Single-row UPDATE on existing seed row
                    pk = random.randint(1, seed_rows)
                    update_fn(f, pk)
                elif r < 0.75:
                    # Single-row DELETE
                    pk = random.randint(1, seed_rows)
                    f.write(f\"DELETE FROM bench_{table} WHERE id={pk};\n\")
                elif r < 0.90:
                    # Bulk UPDATE (5-15 rows)
                    n = random.randint(5, 15)
                    pks = [random.randint(1, seed_rows) for _ in range(n)]
                    gen_bulk_update(table, f, pks)
                else:
                    # Bulk DELETE (3-8 rows)
                    n = random.randint(3, 8)
                    pks = [random.randint(1, seed_rows) for _ in range(n)]
                    gen_bulk_delete(table, f, pks)
    print('Mixed SQL files generated.')
" || fail "Failed to generate SQL files"

echo "Loading data into commerce keyspace via vtgate (4 concurrent streams)..."

load_start=$(date +%s)

# Pipe all 4 SQL files concurrently through vtgate
load_pids=()
for table in orders events accounts logs; do
	command mysql --no-defaults -h 127.0.0.1 -P 15306 --binary-as-hex=false commerce < "$TMPDIR/${table}.sql" &
	load_pids+=("$!")
done

for pid in "${load_pids[@]}"; do
	wait "$pid" || fail "Failed to load one or more benchmark SQL streams"
done

load_end=$(date +%s)
load_elapsed=$((load_end - load_start))

echo "=== Load Generation Complete ==="
echo "Total operations: $TOTAL_OPS"
echo "Time: ${load_elapsed}s"
if [ "$load_elapsed" -gt 0 ]; then
	echo "Rate: $((TOTAL_OPS / load_elapsed)) ops/sec"
fi
