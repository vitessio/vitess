## Major Changes

## Command-line syntax deprecations

Vitess has begun a transition to a new library for CLI flag parsing.
In order to facilitate a smooth transition, certain syntaxes that will not be supported in the future now issue deprecation warnings when used.

The messages you will likely see, along with explanations and migrations, are:

### "Use of single-dash long flags is deprecated"

Single-dash usage will be only possible for short flags (e.g. `-v` is okay, but `-verbose` is not).

To migrate, update your CLI scripts from:

```
$ vttablet -tablet_alias zone1-100 -init_keyspace mykeyspace ... # old way
```

To:

```
$ vttablet --tablet_alias zone1-100 --init_keyspace mykeyspace ... # new way
```

### "Detected a dashed argument after a position argument."

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

```
$ vtctl --topo_implementation etcd2 AddCellInfo -- --root "/vitess/global"
$ # the following will also work
$ vtctl --topo_implementation etcd2 -- AddCellInfo --root "/vitess/global"
$ # the following will NOT work, because --topo_implementation is a top-level flag, not a sub-command flag
$ vtctl -- --topo_implementation etcd2 AddCellInfo --root "/vitess/global"
```

### Online DDL changes

See new SQL syntax for controlling/viewing throttling fomr Online DDL, down below.

#### ddl_strategy: 'vitess'

`ddl_strategy` now takes the value of `vitess` to indicate VReplication-based migrations. It is a synonym to `online` and uses the exact same functionality. In the future, the `online` term will phase out, and `vitess` will remain the term of preference.

Example:

```shell
vtctlclient ApplySchema -skip_preflight -ddl_strategy='vitess' -sql "alter table my_table add column my_val int not null default 0" commerce
```

### --singleton-context and REVERT migrations

It is now possible to submit a migration with `--singleton-context` strategy flag, while there's a pending (queued or running) `REVERT` migration that does not have a `--singleton-context` flag.

#### Behavior changes

- `vtctl ApplySchema --uuid_list='...'` now rejects a migration if an existing migration has the same UUID but with different `migration_context`.

### Table lifecycle

#### Views

Table lifecycle now supports views. It ensures to not purge rows from views, and does not keep views in `EVAC` state (they are immediately transitioned to `DROP` state).

#### Fast drops

On Mysql `8.0.23` or later, the states `PURGE` and `EVAC` are automatically skipped, thanks to `8.0.23` improvement to `DROP TABLE` speed of operation.

### Tablet throttler

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


### Compatibility

#### Join with `USING`

In previous versions of Vitess our planners (v3 and Gen4) were rewriting the `USING` condition of a join to an `ON` condition.
This rewriting was causing an incompatible behavior with MySQL due to how MySQL handles queries with `ON` and `USING` differently.

Thanks to this rewriting we were previously able to plan sharded queries with an `USING` condition. This is no longer the case.
Queries with an `USING` condition that need to be sent to a sharded keyspace are no longer supported and will return an `unsupported` planner error.

This change was made through pull request [#9767](https://github.com/vitessio/vitess/pull/9767).
