# Instructions

Detailed instructions for running this example can be found at https://vitess.io.
This document contains the summary of the commands to be run.


```
# Setup environment and aliases
source ../common/env.sh

# Bring up initial cluster and commerce keyspace
./101_initial_cluster.sh

# Insert and verify data
mysql < ../common/insert_commerce_data.sql
mysql --table < ../common/select_commerce_data.sql

# Bring up customer keyspace
./201_customer_tablets.sh

# Initiate move tables
vtctldclient MoveTables --workflow commerce2customer --target-keyspace customer create --source-keyspace commerce --tables "customer,corder"

# Validate
vtctldclient vdiff --workflow commerce2customer --target-keyspace customer create
vtctldclient vdiff --workflow commerce2customer --target-keyspace customer show last

# Cut-over
vtctldclient MoveTables --workflow commerce2customer --target-keyspace customer switchtraffic --tablet-types "rdonly,replica"
vtctldclient MoveTables --workflow commerce2customer --target-keyspace customer switchtraffic --tablet-types primary

# Clean-up
vtctldclient MoveTables --workflow commerce2customer --target-keyspace customer complete

# Prepare for resharding
./301_customer_sharded.sh
./302_new_shards.sh

# Reshard
vtctldclient Reshard --workflow cust2cust --target-keyspace customer create --source-shards '0' --target-shards '-80,80-'

# Validate
vtctldclient vdiff --workflow cust2cust --target-keyspace customer create
vtctldclient vdiff --workflow cust2cust --target-keyspace customer show last

# Cut-over
vtctldclient Reshard --workflow cust2cust --target-keyspace customer switchtraffic --tablet-types "rdonly,replica"
vtctldclient Reshard --workflow cust2cust --target-keyspace customer switchtraffic --tablet-types primary

# Down shard 0
vtctldclient Reshard --workflow cust2cust --target-keyspace customer complete
./306_down_shard_0.sh
vtctldclient DeleteShards --force --recursive customer/0

# Down cluster
./501_teardown.sh
```

## vtgate slow query observability

The local example enables vtgate-level slow-query detection and writes vtgate
query logs to `$VTDATAROOT/tmp/vtgate_querylog.txt`.

```
--slow-query-threshold 10ms
```

Override the threshold before starting the cluster:

```
export VTGATE_SLOW_QUERY_THRESHOLD=250ms
./101_initial_cluster.sh
```

### Generate slow query traffic

Start the cluster and insert sample data. Any query whose vtgate execution time
meets or exceeds the threshold is marked slow. The following command uses an
`UPDATE` with `sleep()` only to make the behavior easy to reproduce locally:

```
source ../common/env.sh
./101_initial_cluster.sh
mysql < ../common/insert_commerce_data.sql

mysql commerce <<'SQL'
set workload = olap;
update /* slow_docs */ customer set email = email where customer_id = 1 and sleep(0.05) = 0;
SQL
```

The `slow_docs` comment makes the query easy to find in logs.

### Check counters

`SlowQueries` is keyed by query type, plan type, and tablet type. The query
above increments `UPDATE.Passthrough.PRIMARY`:

```
curl -s http://localhost:15001/debug/vars | grep -o '"UPDATE.Passthrough.PRIMARY":[0-9]*'
```

Example output:

```
"UPDATE.Passthrough.PRIMARY":1
```

The same counter is exported to Prometheus as `vtgate_slow_queries`:

```
curl -s http://localhost:15001/metrics \
  | grep 'vtgate_slow_queries' \
  | grep 'query="UPDATE"' \
  | grep 'plan="Passthrough"' \
  | grep 'tablet="PRIMARY"'
```

Example output:

```
vtgate_slow_queries{plan="Passthrough",query="UPDATE",tablet="PRIMARY"} 1
```

### Alert on slow writes

For an application-facing signal, alert when more than 1% of primary writes are
slow over five minutes:

```
100 *
sum(rate(vtgate_slow_queries{query=~"INSERT|UPDATE|DELETE",tablet="PRIMARY"}[5m]))
/
sum(rate(vtgate_query_executions{query=~"INSERT|UPDATE|DELETE",tablet="PRIMARY"}[5m]))
> 1
```

This answers: "What percentage of writes routed through vtgate to PRIMARY were
slow from the client/vtgate point of view?"

For drilldown, group by query and plan:

```
sum by (query, plan, tablet) (
  rate(vtgate_slow_queries{tablet="PRIMARY"}[5m])
)
```

### Inspect slow queries

The vtgate query log includes `SlowQuery=true`:

```
grep 'slow_docs' "$VTDATAROOT/tmp/vtgate_querylog.txt" | grep $'\ttrue\t'
```

`/debug/querylogz` shows the same signal in the debug UI. Start the request in
one terminal:

```
curl 'http://localhost:15001/debug/querylogz?timeout=30&limit=1'
```

Then, in another terminal, run:

```
mysql commerce <<'SQL'
set workload = olap;
update /* slow_querylogz */ customer set email = email where customer_id = 1 and sleep(0.05) = 0;
SQL
```

The rendered table includes a `SlowQuery` column with `true`.

Clients that inspect MySQL protocol status flags should also see
`SERVER_QUERY_WAS_SLOW` set on the OK packet returned by the slow `UPDATE`.
