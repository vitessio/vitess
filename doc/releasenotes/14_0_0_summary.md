## Major Changes

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

Currently, when invoking a binary like

```
$ vtctl --topo_implementation etcd2 AddCellInfo --root "/vitess/global"
```

everything after the `AddCellInfo` is treated by `package flag` as a positional argument, and we then use a sub FlagSet to parse flags specific to the subcommand.
So, at the top-level, `flag.Args()` returns `["AddCellInfo", "--root", "/vitess/global"]`.

The library we are transitioning to is more flexible, allowing flags and positional arguments to be interwoven on the command-line.
For the above example, this means that we would attempt to parse `--root` as a top-level flag for the `vtctl` binary.
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

When `--heartbeat_on_demand_duration` has a positive value, then heartbeats are only injected on demand, per internal requests. For example, when `--heartbeat_on_demand_duration=5s`, the tablet starts without injecting heartbeats. An internal module, like the lag throttle, may request the heartbeat writer for heartbeats. Starting at that point in time, and for the duration (a lease) of `5s` in our example, the tablet will write heartbeats. If no other requests come in during that duration, then the tablet then ceases to write heartbeats. If more requests for heartbeats come while heartbeats are being written, then the tablet extends the lease for the next `5s` following up each request. Thus, it stops writing heartbeats `5s` after the last request is received.

The heartbeats are generated according to `--heartbeat_interval`.

#### Deprecation of --online_ddl_check_interval

The flag `--online_ddl_check_interval` is deprecated and will be removed in `v15`. It has been unused in `v13`.

#### Deprecation of --planner-version for vtexplain

The flag `--planner-version` is deprecated and will be removed in `v15`. Instead, please use `--planer_version`.

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
- and all other functionality

Are all considered production-ready.

`pt-osc` strategy (online DDL via 3rd party `pt-online-schema-change`) remains experimental.

#### Throttling

See new SQL syntax for controlling/viewing throttling for Online DDL, down below.

#### ddl_strategy: 'vitess'

`ddl_strategy` now takes the value of `vitess` to indicate VReplication-based migrations. It is a synonym to `online` and uses the exact same functionality. In the future, the `online` term will phase out, and `vitess` will remain the term of preference.

Example:

```shell
vtctlclient ApplySchema -skip_preflight -ddl_strategy='vitess' -sql "alter table my_table add column my_val int not null default 0" commerce
```

#### --singleton-context and REVERT migrations

It is now possible to submit a migration with `--singleton-context` strategy flag, while there's a pending (queued or running) `REVERT` migration that does not have a `--singleton-context` flag.

#### Support for CHECK constraints

Online DDL operations are more aware of `CHECK` constraints, and properly handle the limitation where a `CHECK`'s name has to be unique in the schema. As opposed to letting MySQL choose arbitrary names for shadow table's `CHECK` consraints, Online DDL now generates unique yet deterministic names, such that all shards converge onto same names.

Online DDL attempts to preserve the original check's name as a suffix to the generated name, where possible (names are limited to `64` characters).

#### Behavior changes

- `vtctl ApplySchema --uuid_list='...'` now rejects a migration if an existing migration has the same UUID but with different `migration_context`.

### Table lifecycle

#### Views

Table lifecycle now supports views. It ensures to not purge rows from views, and does not keep views in `EVAC` state (they are immediately transitioned to `DROP` state).

#### Fast drops

On Mysql `8.0.23` or later, the states `PURGE` and `EVAC` are automatically skipped, thanks to `8.0.23` improvement to `DROP TABLE` speed of operation.

### Tablet throttler

#### API changes

Added `/throttler/throttled-apps` endpoint, which reports back all current throttling instructions. Note, this only reports explicit throttling requests (sych as ones submitted by `/throtler/throttle-app?app=...`). It does not list incidental rejections based on throttle thresholds.

API endpoint `/throttler/throttle-app` now accepts a `ratio` query argument, a floating point in the range `[0..1]`, where:

- `0` means "do not throttle at all"
- `1` means "always throttle"
- any numbr in between is allowd. For example, `0.3` means "throttle in 0.3 probability", ie on a per request and based on a dice roll, there's a `30%` change a request is denied. Overall we can expect about `30%` of requests to be denied. Example: `/throttler/throttle-app?app=vreplication&ratio=0.25`

See new SQL syntax for controlling/viewing throttling, down below.

### New Syntax

#### Control and view Online DDL throttling

We introduce the following syntax, to:

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

default `duration` is "infinite" (set as 100 years)

- allowed units are (s)ec, (m)in, (h)our
ratio is in the range `[0..1]`.
- `1` means full throttle - the app will not make any progress
- `0` means no throttling at all
- `0.8` means on 8 out of 10 checks the app makes, it gets refused

The syntax `SHOW VITESS_THROTTLED_APPS` is a generic call to the throttler, and returns information about all throttled apps, not specific to migrations

`SHOW VITESS_MIGRATIONS ...` output now includes `user_throttle_ratio`

This column is updated "once in a while", while a migration is running. Normally this is once a minute, but can be more frequent. The migration reports back what was the throttling instruction set by the user while it was/is running.
This column does not indicate any actual lag-based throttling that takes place per production state. It only reports the explicit throttling value set by the user.

### Heartbeat

The throttler now checks in with the heartbeat writer to request heartbeats, any time it (the throttler) is asked for a check.

When `--heartbeat_on_demand_duration` is not set, there is no change in behavior.

When `--heartbeat_on_demand_duration` is set to a positive value, then the throttler ensures that the heartbeat writer generated heartbeats for at least the following duration. This also means at the first throttler check, it's possible that heartbeats are idle, and so the first check will fail. As heartbeats start running, followup checks will get a more accurate lag evaluation and will respond accordingly. In a sense, it's a "cold engine" scenario, where the engine takes time to start up, and then runs smoothly.

### VDiff2

We introduced a new version of VDiff -- currently marked as Experimental -- that executes the VDiff on tablets rather than in vtctld. While this is experimental we encourage you to try it out and provide feedback! This input will be invaluable as we improve this feature on the march toward a production-ready version. You can try it out by adding the `--v2` flag to your VDiff command. Here's an example:
```
$ vtctlclient --server=localhost:15999 VDiff -- --v2 customer.commerce2customer
VDiff bf9dfc5f-e5e6-11ec-823d-0aa62e50dd24 scheduled on target shards, use show to view progress

$ vtctlclient --server=localhost:15999 VDiff -- --v2 customer.commerce2customer show last

VDiff Summary for customer.commerce2customer (bf9dfc5f-e5e6-11ec-823d-0aa62e50dd24)
State: completed
HasMismatch: false

Use "--format=json" for more detailed output.

$ vtctlclient --server=localhost:15999 VDiff -- --v2 --format=json customer.commerce2customer show last
{
	"Workflow": "commerce2customer",
	"Keyspace": "customer",
	"State": "completed",
	"UUID": "bf9dfc5f-e5e6-11ec-823d-0aa62e50dd24",
	"HasMismatch": false,
	"Shards": "0"
}
```

:information_source:  NOTE: even before it's marked as production-ready (feature complete and tested widely in 1+ releases), it should be safe to use and is likely to provide much better results for very large tables.

For additional details, please see the [RFC](https://github.com/vitessio/vitess/issues/10134) and the [README](https://github.com/vitessio/vitess/tree/main/go/vt/vttablet/tabletmanager/vdiff/README.md).

### Durability Policy

#### Deprecation of durability_policy Flag
The durability policy for a keyspace is now stored in the keyspace record in the topo server.
The `durability_policy` flag used by vtctl, vtctld, and vtworker binaries has been deprecated.

#### New and Augmented Commands
The vtctld command `CreateKeyspace` has been augmented to take in an additional argument called `durability-policy` which will
allow users to set the desired durability policy for a keyspace at creation time.

For existing keyspaces, a new command `SetKeyspaceDurabilityPolicy` has been added, which allows users to change the
durability policy of an existing keyspace.

If semi-sync is not being used then durability policy should be set to `none` for the keyspace. This is also the default option.

If semi-sync is being used then durability policy should be set to `semi_sync` for the keyspace and `--enable_semi_sync` should be set on vttablets.

### Deprecation of Durability Configuration
The `Durability` configuration is deprecated and removed from VTOrc. Instead VTOrc will find the durability policy of the keyspace from
the topo server. This allows VTOrc to monitor and repair multiple keyspaces which have different durability policies in use.

**VTOrc will ignore the keyspaces which have no durability policy specified in the keyspace record. So on upgrading to v14, users must run
the command `SetKeyspaceDurabilityPolicy` specified above, to ensure VTOrc continues to work as desired. The recommended upgrade 
path is to upgrade vtctld, run `SetKeyspaceDurabilityPolicy` and then upgrade VTOrc.**
