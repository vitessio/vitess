## Major Changes

### Breaking Change

#### Vindex Implementation

All the vindex interface methods are changed by adding context.Context as the input parameter.

E.g:
```go
Map(vcursor VCursor, .... ) .... 
	To
Map(ctx context.Context, vcursor VCursor, .... ) ....
```

This only impacts the users who have added their own vindex implementation. 
They would be required to change their implementation with these new interface method expectations.

### Command-line syntax deprecations

#### vttablet startup flag deletions
The following VTTablet flags were deprecated in 7.0. They have now been deleted
- --queryserver-config-message-conn-pool-size
- --queryserver-config-message-conn-pool-prefill-parallelism
- --client-found-rows-pool-size --queryserver-config-transaction-cap will be used instead
- --transaction_shutdown_grace_period Use --shutdown_grace_period instead
- --queryserver-config-max-dml-rows
- --queryserver-config-allowunsafe-dmls
- --pool-name-prefix
- --enable-autocommit Autocommit is always allowed

#### vttablet startup flag deprecations
- --enable-query-plan-field-caching is now deprecated. It will be removed in v16.

### New command line flags and behavior

#### vtctl GetSchema --table-schema-only

The new flag `--table-schema-only` skips columns introspection. `GetSchema` only returns general schema analysis, and specifically it includes the `CREATE TABLE|VIEW` statement in `schema` field.

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
Backup/Restore now allow you many more options for compression and decompression instead of relying on the default compressor(pgzip).
There are some built-in compressors which you can use out-of-the-box. Users will need to evaluate which option works best for their
use-case. Here are the flags that control this feature

- --builtin-compressor
- --builtin-decompressor
- --external-compressor
- --external-decompressor
- --external-compressor-extension
- --compression-level

builtin compressor as of today supports the following options
- pgzip
- pargzip
- lz4
- zstd

If you want to use any of the builtin compressors, simply set one of the above values for `--builtin-compressor`. You don't need to set
the `--builtin-decompressor` flag in this case as we infer it automatically from the MANIFEST file. The default value for
`--builtin-decompressor` is  `auto`.

If you would like to use a custom command or external tool for compression/decompression then you need to provide the full command with
arguments to the `--external-compressor` and `--external-decompressor` flags. `--external-compressor-extension` flag also needs to be provided
so that compressed files are created with the correct extension. There is no need to override `--builtin-compressor` and `--builtin-decompressor`
when using an external compressor/decompressor. Please note that if you want the current behavior then you don't need to change anything
in these flags. You can read more about backup & restore [here] (https://vitess.io/docs/15.0/user-guides/operating-vitess/backup-and-restore/).

### Online DDL changes

#### Concurrent vitess migrations

All Online DDL migrations using the `vitess` strategy are now eligible to run concurrently, given `--allow-concurrent` DDL strategy flag. Until now, only `CREATE`, `DROP` and `REVERT` migrations were eligible, and now `ALTER` migrations are supported, as well. The terms for `ALTER` migrations concurrency:

- DDL strategy must be `vitess --allow-concurent ...`
- No two migrations can run concurrently on same table
- No two `ALTER`s will copy table data concurrently
- A concurrent `ALTER` migration will not start if another `ALTER` is running and is not `ready_to_complete`

The main use case is to run multiple concurrent migrations, all with `--postpone-completion`. All table-copy operations will run sequentially, but no migration will actually cut-over, and eventually all migration will be `ready_to_complete`, continuously tailing the binary logs and keeping up-to-date. A quick and iterative `ALTER VITESS_MIGRATION '...' COMPLETE` sequence of commands will cut-over all migrations _closely together_ (though not atomically together).

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
