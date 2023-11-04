# Release of Vitess v14.0.0
## Summary

- [Gen4 is now the default planner](#gen4-is-now-the-default-planner)
- [New query support](#new-query-support)
- [Command-line syntax deprecations](#command-line-syntax-deprecations)
- [New command line flags and behavior](#new-command-line-flags-and-behavior)
- [Online DDL changes](#online-ddl-changes)
- [Table lifecycle](#table-lifecycle)
- [Tablet throttler](#tablet-throttler)
- [Heartbeat](#heartbeat)
- [VDiff2](#vdiff2)
- [Durability Policy](#durability-policy)
- [Deprecation of Durability Configuration](#deprecation-of-durability-configuration)
- [Advisory locking optimizations](#advisory-locking-optimizations)
- [Pre-Legacy Resharding is now deprecated](#pre-legacy-resharding-is-now-deprecated)

## Known Issues

- [VTOrc doesn't discover the tablets](https://github.com/vitessio/vitess/issues/10650) of a keyspace if the durability policy doesn't exist in the topo server when it comes up. This can be resolved by restarting VTOrc.
- [Corrupted results for non-full-group-by queries with JOINs](https://github.com/vitessio/vitess/issues/11625). This can be resolved by using full-group-by queries.

## Major Changes

### Gen4 is now the default planner

The new planner has been in the works since end of 2020, and it's finally grown enough to be able to become the default planner for Vitess.
This means that many more queries are supported on sharded keyspaces, and old queries might get planned better than before.
You can always roll back to the earlier planner, either by providing the flag `--planner-version=V3` to `vtgate`, or by adding a comment to individual queries, like so:

```sql
select /*vt+ PLANNER=V3 */ name, count(*) from users
```

### New query support

#### Support for aggregation across shards
Vitess can now plan and execute most aggregation queries across multiple shards and/or keyspaces.

#### INSERT from SELECT
Support has been added for inserting new data from SELECT queries.
Now you can insert data from a query into a table using a query like:

```sql
insert into tbl (col) select id from users 
```

#### UPDATE from SELECT
Similarly, we have added support for UPDATE with scalar sub-queries. This allows for queries where the updated value is fetched using a subquery, such as this example:

```sql
update tbl set foo = (select count(*) from otherTbl)
```

### Command-line syntax deprecations

Vitess has begun a transition to a new library for CLI flag parsing.
In order to facilitate a smooth transition, certain syntaxes that will not be supported in the future now issue deprecation warnings when used.

The messages you will likely see, along with explanations and migrations, are:

#### "Use of single-dash long flags is deprecated"

Single-dash usage will be only possible for short flags (e.g. `-v` is okay, but `-verbose` is not).

To migrate, update your CLI scripts from:

```
$ vttablet -tablet_alias zone1-100 -init_keyspace mykeyspace ... # old way
```

To:

```
$ vttablet --tablet_alias zone1-100 --init_keyspace mykeyspace ... # new way
```

#### "Detected a dashed argument after a position argument."

As the full deprecation text goes on to (attempt to) explain, mixing flags and positional arguments will change in a future version that will break scripts.

Currently, when invoking a binary like:

```
$ vtctl --topo_implementation etcd2 AddCellInfo --root "/vitess/global"
```

Everything after the `AddCellInfo` is treated by `package flag` as a positional argument, and we then use a sub FlagSet to parse flags specific to the subcommand.
So, at the top-level, `flag.Args()` returns `["AddCellInfo", "--root", "/vitess/global"]`.

The library we are transitioning to is more flexible, allowing flags and positional arguments to be interwoven on the command-line.
For the above example, this means that we would attempt to parse `--root` as a top-level flag for the `VTCtl` binary.
This will cause the program to exit on error, because that flag is only defined on the `AddCellInfo` subcommand.

In order to transition, a standalone double-dash (literally, `--`) will cause the new flag library to treat everything following that as a positional argument, and also works with the current flag parsing code we use.

So, to transition the above example without breakage, update the command to:

```shell
$ vtctl --topo_implementation etcd2 AddCellInfo -- --root "/vitess/global"
$ # the following will also work
$ vtctl --topo_implementation etcd2 -- AddCellInfo --root "/vitess/global"
$ # the following will NOT work, because --topo_implementation is a top-level flag, not a sub-command flag
$ vtctl -- --topo_implementation etcd2 AddCellInfo --root "/vitess/global"
```

### New command line flags and behavior

#### vttablet --heartbeat_on_demand_duration

`--heartbeat_on_demand_duration` joins the already existing heartbeat flags `--heartbeat_enable` and `--heartbeat_interval` and adds new behavior to heartbeat writes.

`--heartbeat_on_demand_duration` takes a duration value, such as `5s`.

The default value for `--heartbeat_on_demand_duration` is zero, which means the flag is not set and there is no change in behavior.

When `--heartbeat_on_demand_duration` has a positive value, then heartbeats are only injected on demand, based on internal requests. For example, when `--heartbeat_on_demand_duration=5s`, the tablet starts without injecting heartbeats.
An internal module, like the lag throttler, may request the heartbeat writer for heartbeats. Starting at that point in time, and for the duration (a lease) of `5s` in our example, the tablet will write heartbeats.
If no other requests come in during that time, the tablet then ceases to write heartbeats. If more requests for heartbeats come in, the tablet extends the lease for the next `5s` following each request.
It stops writing heartbeats `5s` after the last request is received.

The heartbeats are generated according to `--heartbeat_interval`.

#### Deprecation of --online_ddl_check_interval

The flag `--online_ddl_check_interval` is deprecated and will be removed in `v15`. It has been unused in `v13`.

#### Removal of --gateway_implementation

In previous releases, the `discoverygateway` was deprecated. In Vitess 14 it is now entirely removed, along with the VTGate flag that allowed us to choose a gateway.

#### Deprecation of --planner_version

The flag `--planner_version` is deprecated and will be removed in `v15`.
Some binaries used `--planner_version`, and some used `--planner-version`.
This has been made consistent - all binaries that allow you to configure the planner now take `--planner-version`.
All uses of the underscore form have been deprecated and will be removed in `v15`.

### Online DDL changes

#### Online DDL is generally available

Online DDL is no longer experimental (with the exception of `pt-osc` strategy). Specifically:

- Managed schema changes, the scheduler, the backing tables
- Supporting SQL syntax
- `vitess` strategy (online DDL via VReplication)
- `gh-ost` strategy (online DDL via 3rd party `gh-ost`)
- Recoverable migrations
- Revertible migrations
- Declarative migrations
- Postponed migrations
- And all other functionality

Are all considered production-ready.

`pt-osc` strategy (online DDL via 3rd party `pt-online-schema-change`) remains experimental.

#### ddl_strategy: 'vitess'

`ddl_strategy` now takes the value of `vitess` to indicate VReplication-based migrations. It is a synonym to `online` and uses the exact same functionality. The `online` term will be phased out in the future and `vitess` will remain the term of preference.

Example:

```shell
vtctlclient ApplySchema -skip_preflight -ddl_strategy='vitess' -sql "alter table my_table add column my_val int not null default 0" commerce
```

#### --singleton-context and REVERT migrations

It is now possible to submit a migration with `--singleton-context` strategy flag, while there's a pending (queued or running) `REVERT` migration that does not have a `--singleton-context` flag.

#### Support for CHECK constraints

Online DDL operations are more aware of `CHECK` constraints, and properly handle the limitation where a `CHECK`'s name has to be unique in the schema. As opposed to letting MySQL choose arbitrary names for shadow table's `CHECK` constraints, Online DDL now generates unique yet deterministic names, such that all shards converge onto the same names.

Online DDL attempts to preserve the original check's name as a suffix to the generated name, where possible (names are limited to `64` characters).

#### Behavior changes

- `vtctl ApplySchema --uuid_list='...'` now rejects a migration if an existing migration has the same UUID but with different `migration_context`.

### Table lifecycle

#### Views

Table lifecycle now supports views. It does not purge rows from views, and does not keep views in `EVAC` state (they are immediately transitioned to `DROP` state).

#### Fast drops

On Mysql `8.0.23` or later, the states `PURGE` and `EVAC` are automatically skipped, thanks to `8.0.23` improvements to `DROP TABLE` speed of operation.

### Tablet throttler

#### API changes

Added `/throttler/throttled-apps` endpoint, which reports back all current throttling instructions. Note, this only reports explicit throttling requests (such as ones submitted by `/throtler/throttle-app?app=...`). It does not list incidental rejections based on throttle thresholds.

API endpoint `/throttler/throttle-app` now accepts a `ratio` query argument, a floating point value in the range `[0..1]`, where:

- `0` means "do not throttle at all"
- `1` means "always throttle"
- Any number in between is allowed. For example, `0.3` means "throttle with 0.3 probability", i.e. for any given request there's a 30% chance that the request is denied. Overall we can expect about `30%` of requests to be denied. Example: `/throttler/throttle-app?app=vreplication&ratio=0.25`.

See new SQL syntax for controlling/viewing throttling, under [New Syntax](#new-syntax).

#### New Syntax

##### Control and view Online DDL throttling

We introduce the following syntax to:

- Start/stop throttling for all Online DDL migrations, in general
- Start/stop throttling for a particular Online DDL migration
- View throttler state


```sql
ALTER VITESS_MIGRATION '<uuid>' THROTTLE [EXPIRE '<duration>'] [RATIO <ratio>];
ALTER VITESS_MIGRATION THROTTLE ALL [EXPIRE '<duration>'] [RATIO <ratio>];
ALTER VITESS_MIGRATION '<uuid>' UNTHROTTLE;
ALTER VITESS_MIGRATION UNTHROTTLE ALL;
SHOW VITESS_THROTTLED_APPS;
```

The default `duration` is "infinite" (set as 100 years):
- Allowed units are (s)ec, (m)in, (h)our

The ratio is in the range `[0..1]`:
- `1` means throttle everything - the app will not make any progress
- `0` means no throttling at all
- `0.8` means on 8 out of 10 checks the app makes, it gets refused

The syntax `SHOW VITESS_THROTTLED_APPS` is a generic call to the throttler, and returns information about all throttled apps, not specific to migrations.

The output of `SHOW VITESS_MIGRATIONS ...` now includes `user_throttle_ratio`.

This column is updated "once in a while", while a migration is running. Normally this is once a minute, but can be more frequent. The migration reports back the throttling instruction set by the user while it was running.
This column does not indicate any lag-based throttling that might take place based on the throttler configuration. It only reports the explicit throttling value set by the user.

### Heartbeat

The throttler now checks in with the heartbeat writer to request heartbeats, any time it (the throttler) is asked for a check.

When `--heartbeat_on_demand_duration` is not set, there is no change in behavior.

When `--heartbeat_on_demand_duration` is set to a positive value, then the throttler ensures that the heartbeat writer generates heartbeats for at least the following duration.
This also means at the first throttler check, it's possible that heartbeats are idle, and so the first check will fail. As heartbeats start running, followup checks will get a more accurate lag evaluation and will respond accordingly.
In a sense, it's a "cold engine" scenario, where the engine takes time to start up, and then runs smoothly.

### VDiff2

We introduced a new version of VDiff -- currently marked as EXPERIMENTAL -- that executes the VDiff on vttablets rather than in vtctld.
While this is experimental we encourage you to try it out and provide feedback! This input will be invaluable as we improve the feature on the march toward [a production-ready version](https://github.com/vitessio/vitess/issues/10494).
You can try it out by adding the `--v2` flag to your VDiff command. Here's an example:
```
$ vtctlclient --server=localhost:15999 VDiff -- --v2 customer.commerce2customer
VDiff bf9dfc5f-e5e6-11ec-823d-0aa62e50dd24 scheduled on target shards, use show to view progress

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

> Even before it's marked as production-ready (feature complete and tested widely in 1+ releases), it should be safe to use and is likely to provide much better results for very large tables.

For additional details please see the [RFC](https://github.com/vitessio/vitess/issues/10134), the [README](https://github.com/vitessio/vitess/blob/release-14.0/go/vt/vttablet/tabletmanager/vdiff/README.md), and the VDiff2 [documentation](https://vitess.io/docs/14.0/reference/vreplication/vdiff2/).

### Durability Policy

#### Deprecation of durability_policy Flag
The durability policy for a keyspace is now stored in the keyspace record in the topology server.
The `durability_policy` flag used by VTCtl, VTCtld, and VTWorker binaries has been deprecated and will be removed in a future release.

#### New and Augmented Commands
The VTCtld command `CreateKeyspace` has been augmented to take in an additional argument `--durability-policy` which will
allow users to set the desired durability policy for a keyspace at creation time.

For existing keyspaces, a new command `SetKeyspaceDurabilityPolicy` has been added, which allows users to change the
durability policy of an existing keyspace.

If semi-sync is not being used then durability policy should be set to `none` for the keyspace. This is also the default option.

If semi-sync is being used then durability policy should be set to `semi_sync` for the keyspace and `--enable_semi_sync` should be set on vttablets.

### VTOrc - Deprecation of Durability Configuration
The `Durability` configuration is deprecated and removed from VTOrc. Instead VTOrc will find the durability policy of the keyspace from
the topology server. This allows VTOrc to monitor and repair multiple keyspaces which have different durability policies in use.

**VTOrc will ignore keyspaces which have no durability policy specified in the keyspace record. This is to avoid clobbering an existing
config from a previous release. So on upgrading to v14, users must run the command `SetKeyspaceDurabilityPolicy` specified above,
to ensure that VTOrc continues to work as desired. The recommended upgrade
path is to upgrade VTCtld, run `SetKeyspaceDurabilityPolicy` and then upgrade VTOrc.**

### Advisory locking optimizations
Work has gone into making the advisory locks (`get_lock()`, `release_lock()`, etc.) release reserved connections faster and in more situations than before.

### Pre-Legacy Resharding is now deprecated
A long time ago, the sharding column and type were specified at the keyspace level. This syntax is now deprecated and will be removed in v15.

------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/14.0/14.0.0/changelog.md).

The release includes 1101 commits (excluding merges)

Thanks to all our contributors: @FancyFane, @GuptaManan100, @Juneezee, @K-Kumar-01, @Phanatic, @ajm188, @akenneth, @aquarapid, @arthurschreiber, @brendar, @cuishuang, @dasl-, @dbussink, @deepthi, @dependabot[bot], @derekperkins, @doeg, @fatih, @frouioui, @harshit-gangal, @malpani, @matthiasr, @mattlord, @mattrobenolt, @notfelineit, @pjambet, @rohit-nayak-ps, @rsajwani, @shlomi-noach, @simon-engledew, @systay, @utk9, @vmg, @vmogilev, @y5w, @yields
