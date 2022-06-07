## Major Changes

### Command-line syntax deprecations

### New command line flags and behavior

### Online DDL changes

#### Concurrent vitess migrations

All Online DDL migrations using the `vitess` strategy are now eligible to run concurrently, given `--allow-concurrent` DDL strategy flag. Until now, only `CREATE`, `DROP` and `REVERT` migrations were eligible, and now `ALTER` migrations are supported, as well. The terms for `ALTER` migrations concurrency:

- DDL strategy must be `vitess --allow-concurent ...`
- No two migrations can run concurrently on same table
- No two `ALTER`s will copy table data concurrently
- A concurrent `ALTER` migration will not start if another `ALTER` is running and is not `ready_to_complete`

The main use case is to run multiple concurrent migrations, all with `--postpone-completion`. All table-copy operations will run sequentially, but no migration will actually cut-over, and eventually all migration will be `ready_to_complete`, continuously tailing the binary logs and keeping up-to-date. A quick and iterative `ALTER VITESS_MIGRATION '...' COMPLETE` sequence of commands will cut-over all migrations _closely together_ (though not atomically together).
