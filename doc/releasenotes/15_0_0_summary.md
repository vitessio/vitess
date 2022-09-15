## Major Changes

### Command-line syntax deprecations

#### vttablet startup flag --enable-query-plan-field-caching
This flag is now deprecated. It will be removed in v16.

#### vttablet startup flag deprecations
- --enable_semi_sync is now deprecated. It will be removed in v16. Instead, set the correct durability policy using `SetKeyspaceDurabilityPolicy`

### New Syntax

### VDiff2

We introduced the ability to resume a VDiff2 workflow:
```
$ vtctlclient --server=localhost:15999 VDiff -- --v2 customer.commerce2customer resume 4c664dc2-eba9-11ec-9ef7-920702940ee0
VDiff 4c664dc2-eba9-11ec-9ef7-920702940ee0 resumed on target shards, use show to view progress

$ vtctlclient --server=localhost:15999 VDiff -- --v2 customer.commerce2customer show last

VDiff Summary for customer.commerce2customer (4c664dc2-eba9-11ec-9ef7-920702940ee0)
State:        completed
RowsCompared: 196
HasMismatch:  false
StartedAt:    2022-06-26 22:44:29
CompletedAt:  2022-06-26 22:44:31

Use "--format=json" for more detailed output.

$ vtctlclient --server=localhost:15999 VDiff -- --v2 --format=json customer.commerce2customer show last
{
	"Workflow": "commerce2customer",
	"Keyspace": "customer",
	"State": "completed",
	"UUID": "4c664dc2-eba9-11ec-9ef7-920702940ee0",
	"RowsCompared": 196,
	"HasMismatch": false,
	"Shards": "0",
	"StartedAt": "2022-06-26 22:44:29",
	"CompletedAt": "2022-06-26 22:44:31"
}
```

Please see the VDiff2 [documentation](https://vitess.io/docs/15.0/reference/vreplication/vdiff2/) for additional information.

### New command line flags and behavior

#### Support for additional compressors and decompressors during backup & restore
Backup/Restore now allow you many more options for compression and decompression instead of relying on the default compressor(`pgzip`).
There are some built-in compressors which you can use out-of-the-box. Users will need to evaluate which option works best for their
use-case. Here are the flags that control this feature

- --compression-engine-name
- --external-compressor
- --external-decompressor
- --external-compressor-extension
- --compression-level

`--compression-engine-name` specifies the engine used for compression. It can have one of the following values

- pargzip (Default)
- pgzip
- lz4
- zstd
- external

where 'external' is set only when using a custom command or tool other than the ones that are already provided. 
If you want to use any of the built-in compressors, simply set one of the above values for `--compression-engine-name`. The value
specified in `--compression-engine-name` is saved in the backup MANIFEST, which is later read by the restore process to decide which
engine to use for decompression. Default value for engine is 'pgzip'.

If you would like to use a custom command or external tool for compression/decompression then you need to provide the full command with
arguments to the `--external-compressor` and `--external-decompressor` flags. `--external-compressor-extension` flag also needs to be provided
so that compressed files are created with the correct extension. If the external command is not using any of the built-in compression engines
(i-e pgzip, pargzip, lz4 or zstd) then you need to set `--compression-engine-name` to value 'external'.

Please note that if you want the current production behavior then you don't need to change any of these flags.
You can read more about backup & restore [here] (https://vitess.io/docs/15.0/user-guides/operating-vitess/backup-and-restore/).

If you decided to switch from an external compressor to one of the built-in supported compressors (i-e pgzip, pargzip, lz4 or zstd) at any point
in the future, you will need to do it in two steps.

- step #1, set `--external-compressor` and `--external-compressor-extension` flag values to empty and change `--compression-engine-name` to desired value.
- Step #2, after at least one cycle of backup with new configuration, you can set `--external-decompressor` flag value to empty.

The reason you cannot change all the values together is because the restore process will then have no way to find out which external decompressor
should be used to process the previous backup. Please make sure you have thought out all possible scenarios for restore before transitioning from one
compression engine to another.

#### Independent OLAP and OLTP transactional timeouts

`--queryserver-config-olap-transaction-timeout` specifies the timeout applied
to a transaction created within an OLAP workload. The default value is `30`
seconds, but this can be raised, lowered, or set to zero to disable the timeout
altogether.

Until now, while OLAP queries would bypass the query timeout, transactions
created within an OLAP session would be rolled back
`--queryserver-config-transaction-timeout` seconds after the transaction was
started.

As of now, OLTP and OLAP transaction timeouts can be configured independently of each
other.

The main use case is to run queries spanning a long period of time which
require transactional guarantees such as consistency or atomicity.

### Online DDL changes

#### Concurrent vitess migrations

All Online DDL migrations using the `vitess` strategy are now eligible to run concurrently, given `--allow-concurrent` DDL strategy flag. Until now, only `CREATE`, `DROP` and `REVERT` migrations were eligible, and now `ALTER` migrations are supported, as well. The terms for `ALTER` migrations concurrency:

- DDL strategy must be `vitess --allow-concurent ...`
- No two migrations can run concurrently on same table
- No two `ALTER`s will copy table data concurrently
- A concurrent `ALTER` migration will not start if another `ALTER` is running and is not `ready_to_complete`

The main use case is to run multiple concurrent migrations, all with `--postpone-completion`. All table-copy operations will run sequentially, but no migration will actually cut-over, and eventually all migration will be `ready_to_complete`, continuously tailing the binary logs and keeping up-to-date. A quick and iterative `ALTER VITESS_MIGRATION '...' COMPLETE` sequence of commands will cut-over all migrations _closely together_ (though not atomically together).

#### vtctl command changes. 
All `online DDL show` commands can now be run with a few additional parameters
- `--order` , order migrations in the output by either ascending or descending order of their `id` fields.
- `--skip`  , skip specified number of migrations in the output.
- `--limit` , limit results to a specified number of migrations in the output.

#### New syntax

The following is now supported:

```sql
ALTER VITESS_MIGRATION COMPLETE ALL
```

This works on all pending migrations (`queued`, `ready`, `running`) and internally issues a `ALTER VITESS_MIGRATION '<uuid>' COMPLETE` for each one. The command is useful for completing multiple concurrent migrations (see above) that are open ended (`--postpone-completion`).

### Tablet throttler

#### API changes

API endpoint `/debug/vars` now exposes throttler metrics, such as number of hits and errors per app per check type. Example:

```shell
$ curl -s http://127.0.0.1:15100/debug/vars | jq . | grep Throttler
  "ThrottlerAggregatedMysqlSelf": 0.191718,
  "ThrottlerAggregatedMysqlShard": 0.960054,
  "ThrottlerCheckAnyError": 27,
  "ThrottlerCheckAnyMysqlSelfError": 13,
  "ThrottlerCheckAnyMysqlSelfTotal": 38,
  "ThrottlerCheckAnyMysqlShardError": 14,
  "ThrottlerCheckAnyMysqlShardTotal": 42,
  "ThrottlerCheckAnyTotal": 80,
  "ThrottlerCheckMysqlSelfSecondsSinceHealthy": 0,
  "ThrottlerCheckMysqlShardSecondsSinceHealthy": 0,
  "ThrottlerProbesLatency": 355523,
  "ThrottlerProbesTotal": 74,
```

### Mysql Compatibility

#### Lookup Vindexes

Added new parameter `multi_shard_autocommit` to lookup vindex definition in vschema, if enabled will send lookup vindex dml query as autocommit to all shards
This is slighly different from `autocommit` parameter where the query is sent in its own transaction separate from the ongoing transaction if any i.e. begin -> lookup query execs -> commit/rollback

### Durability Policy

#### Cross Cell

A new durabilty policy `cross_cell` is now supported. `cross_cell` durability policy only allows replica tablets from a different cell than the current primary to
send semi sync ACKs. This ensures that any committed write exists in atleast 2 tablets belonging to different cells.