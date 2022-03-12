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

#### ddl_strategy: 'vitess'

`ddl_strategy` now takes the value of `vitess` to indicate VReplication-based migrations. It is a synonym to `online` and uses the exact same functionality. In the future, the `online` term will phase out, and `vitess` will remain the term of preference.

Example:

```shell
vtctlclient ApplySchema -skip_preflight -ddl_strategy='vitess' -sql "alter table my_table add column my_val int not null default 0" commerce
```

#### Behavior changes

- `vtctl ApplySchema -uuid_list='...'` now rejects a migration if an existing migration has the same UUID but with different `migration_context`.

### Table lifecycle

#### Views

Table lifecycle now supports views. It ensures to not purge rows from views, and does not keep views in `EVAC` state (they are immediately transitioned to `DROP` state).

#### Fast drops

On Mysql `8.0.23` or later, the states `PURGE` and `EVAC` are automatically skipped, thanks to `8.0.23` improvement to `DROP TABLE` speed of operation.

### Compatibility

#### Join with `USING`

In previous versions of Vitess our planners (v3 and Gen4) were rewriting the `USING` condition of a join to an `ON` condition.
This rewriting was causing an incompatible behavior with MySQL due to how MySQL handles queries with `ON` and `USING` differently.

Thanks to this rewriting we were previously able to plan sharded queries with an `USING` condition. This is no longer the case.
Queries with an `USING` condition that need to be sent to a sharded keyspace are no longer supported and will return an `unsupported` planner error.

This change was made through pull request [#9767](https://github.com/vitessio/vitess/pull/9767).
