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

### Online DDL changes

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

#### API changes

Added `/throttler/throttled-apps` endpoint, which reports back all current throttling instructions. Note, this only reports explicit throttling requests (sych as ones submitted by `/throtler/throttle-app?app=...`). It does not list incidental rejections based on throttle thresholds.

API endpoint `/throttler/throttle-app` now accepts a `ratio` query argument, a floating point in the range `[0..1]`, where:

- `0` means "do not throttle at all"
- `1` means "always throttle"
- any numbr in between is allowd. For example, `0.3` means "throttle in 0.3 probability", ie on a per request and based on a dice roll, there's a `30%` change a request is denied. Overall we can expect about `30%` of requests to be denied. Example: `/throttler/throttle-app?app=vreplication&ratio=0.25`

#### Heartbeat

The throttler now checks in with the heartbeat writer to request heartbeats, any time it (the throttler) is asked for a check.

When `--heartbeat_on_demand_duration` is not set, there is no change in behavior.

When `--heartbeat_on_demand_duration` is set to a positive value, then the throttler ensures that the heartbeat writer generated heartbeats for at least the following duration. This also means at the first throttler check, it's possible that heartbeats are idle, and so the first check will fail. As heartbeats start running, followup checks will get a more accurate lag evaluation and will respond accordingly. In a sense, it's a "cold engine" scenario, where the engine takes time to start up, and then runs smoothly.

### Compatibility

#### Join with `USING`

In previous versions of Vitess our planners (v3 and Gen4) were rewriting the `USING` condition of a join to an `ON` condition.
This rewriting was causing an incompatible behavior with MySQL due to how MySQL handles queries with `ON` and `USING` differently.

Thanks to this rewriting we were previously able to plan sharded queries with an `USING` condition. This is no longer the case.
Queries with an `USING` condition that need to be sent to a sharded keyspace are no longer supported and will return an `unsupported` planner error.

This change was made through pull request [#9767](https://github.com/vitessio/vitess/pull/9767).
