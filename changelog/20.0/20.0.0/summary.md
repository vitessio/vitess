## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deletions](#deletions)** 
    - [MySQL binaries in the vitess/lite Docker images](#vitess-lite)
  - **[Breaking changes](#breaking-changes)**
    - [`shutdown_grace_period` Default Change](#shutdown-grace-period-default)
    - [New `unmanaged` Flag and `disable_active_reparents` deprecation](#unmanaged-flag)
    - [`mysqlctld` `onterm-timeout` Default Change](#mysqlctld-onterm-timeout)
    - [`Durabler` interface method renaming](#durabler-interface-method-renaming)
  - **[Query Compatibility](#query-compatibility)**
    - [Vindex Hints](#vindex-hints)
    - [Update with Limit Support](#update-limit)
    - [Update with Multi Table Support](#multi-table-update)
    - [Update with Multi Target Support](#update-multi-target)
    - [Delete with Subquery Support](#delete-subquery)
    - [Delete with Multi Target Support](#delete-multi-target)
  - **[Flag changes](#flag-changes)**
    - [`pprof-http` default change](#pprof-http-default)
    - [New `healthcheck-dial-concurrency` flag](#healthcheck-dial-concurrency-flag)
- **[Minor Changes](#minor-changes)**
  - **[New Stats](#new-stats)**
    - [VTTablet Query Cache Hits and Misses](#vttablet-query-cache-hits-and-misses)
  - **[`SIGHUP` reload of gRPC client static auth creds](#sighup-reload-of-grpc-client-auth-creds)**

## <a id="major-changes"/>Major Changes

### <a id="deletions"/>Deletion

#### <a id="vitess-lite"/>MySQL binaries in the `vitess/lite` Docker images

In `v19.0.0` we had deprecated the `mysqld` binary in the `vitess/lite` Docker image.
Making MySQL/Percona version specific image tags also deprecated.

Starting in `v20.0.0` we no longer build the MySQL/Percona version specific image tags.
Moreover, the `mysqld` binary is no longer present on the `vitess/lite` image.

Here are the images we will no longer build and push:

| Image                           | Available | 
|---------------------------------|-----------|
| `vitess/lite:v20.0.0`           | YES       |
| `vitess/lite:v20.0.0-mysql57`   | NO        |
| `vitess/lite:v20.0.0-mysql80`   | NO        |
| `vitess/lite:v20.0.0-percona57` | NO        |
| `vitess/lite:v20.0.0-percona80` | NO        |


If you have not done it yet, you can use an official MySQL Docker image for your `mysqld` container now such as: `mysql:8.0.30`.
Below is an example of a kubernetes yaml file before and after upgrading to an official MySQL image:

```yaml
# before:

# you are still on v19 and are looking to upgrade to v20
# the image used here includes MySQL 8.0.30 and its binaries

    mysqld:
      mysql80Compatible: vitess/lite:v19.0.0-mysql80
```
```yaml
# after:

# if we still want to use MySQL 8.0.30, we now have to use the
# official MySQL image with the 8.0.30 tag as shown below 

    mysqld:
      mysql80Compatible: mysql:8.0.30 # or even mysql:8.0.34 for instance
```

### <a id="breaking-changes"/>Breaking Changes

#### <a id="shutdown-grace-period-default"/>`shutdown_grace_period` Default Change

The `--shutdown_grace_period` flag, which was introduced in v2 with a default of `0 seconds`, has now been changed to default to `3 seconds`.
This makes reparenting in Vitess resilient to client errors, and prevents PlannedReparentShard from timing out.

In order to preserve the old behaviour, the users can set the flag back to `0 seconds` causing open transactions to never be shutdown, but in that case, they run the risk of PlannedReparentShard calls timing out.

#### <a id="unmanaged-tablet"/> New `unmanaged` Flag and `disable_active_reparents` deprecation

New flag `--unmanaged` has been introduced in this release to make it easier to flag unmanaged tablets. It also runs validations to make sure the unmanaged tablets are configured properly. `--disable_active_reparents` flag has been deprecated for `vttablet`, `vtcombo` and `vttestserver` binaries and will be removed in future releases. Specifying the `--unmanaged` flag will also block replication commands and replication repairs.

Starting this release, all unmanaged tablets should specify this flag.

#### <a id="mysqlctld-onterm-timeout"/>`mysqlctld` `onterm_timeout` Default Change

The `--onterm_timeout` flag default value has changed for `mysqlctld`. It now is by default long enough to be able to wait for the default `--shutdown-wait-time` when shutting down on a `TERM` signal. 

This is necessary since otherwise MySQL would never shut down cleanly with the old defaults, since `mysqlctld` would shut down already after 10 seconds by default.

#### <a id="durabler-interface-method-renaming"/>`Durabler` interface method renaming

The methods of [the `Durabler` interface](https://github.com/vitessio/vitess/blob/main/go/vt/vtctl/reparentutil/durability.go#L70-L79) in `go/vt/vtctl/reparentutil` were renamed to be public _(capitalized)_ methods to make it easier to integrate custom Durability Policies from external packages. See [RFC for details](https://github.com/vitessio/vitess/issues/15544).

Users of custom Durability Policies must rename private `Durabler` methods.

Changes:
- The `promotionRule` method was renamed to `PromotionRule`
- The `semiSyncAckers` method was renamed to `SemiSyncAckers`
- The `isReplicaSemiSync` method was renamed to `IsReplicaSemiSync`

### <a id="query-compatibility"/>Query Compatibility

#### <a id="vindex-hints"/> Vindex Hints

Vitess now supports Vindex hints that provide a way for users to influence the shard routing of queries in Vitess by specifying, which vindexes should be considered or ignored by the query planner. This feature enhances the control over query execution, allowing for potentially more efficient data access patterns in sharded databases.

Example:
 ```sql
    SELECT * FROM user USE VINDEX (hash_user_id, secondary_vindex) WHERE user_id = 123;
    SELECT * FROM order IGNORE VINDEX (range_order_id) WHERE order_date = '2021-01-01';
 ```

For more information about Vindex hints and its usage, please consult the documentation.

#### <a id="update-limit"/> Update with Limit Support

Support is added for sharded update with limit.

Example: `update t1 set t1.foo = 'abc', t1.bar = 23 where t1.baz > 5 limit 1`

More details about how it works is available in [MySQL Docs](https://dev.mysql.com/doc/refman/8.0/en/update.html)

#### <a id="multi-table-update"/> Update with Multi Table Support

Support is added for sharded multi-table update with column update on single target table using multiple table join.

Example: `update t1 join t2 on t1.id = t2.id join t3 on t1.col = t3.col set t1.baz = 'abc', t1.apa = 23 where t3.foo = 5 and t2.bar = 7`

More details about how it works is available in [MySQL Docs](https://dev.mysql.com/doc/refman/8.0/en/update.html)

#### <a id="update-multi-target"/> Update with Multi Target Support

Support is added for sharded multi table target update.

Example: `update t1 join t2 on t1.id = t2.id set t1.foo = 'abc', t2.bar = 23`

More details about how it works is available in [MySQL Docs](https://dev.mysql.com/doc/refman/8.0/en/update.html)

#### <a id="delete-subquery"/> Delete with Subquery Support

Support is added for sharded table delete with subquery

Example: `delete from t1 where id in (select col from t2 where foo = 32 and bar = 43)`

#### <a id="delete-multi-target"/> Delete with Multi Target Support

Support is added for sharded multi table target delete.

Example: `delete t1, t3 from t1 join t2 on t1.id = t2.id join t3 on t1.col = t3.col`

More details about how it works is available in [MySQL Docs](https://dev.mysql.com/doc/refman/8.0/en/delete.html)

### <a id="flag-changes"/>Flag Changes

#### <a id="pprof-http-default"/> `pprof-http` Default Change

The `--pprof-http` flag, which was introduced in v19 with a default of `true`, has now been changed to default to `false`.
This makes HTTP `pprof` endpoints now an *opt-in* feature, rather than opt-out.
To continue enabling these endpoints, explicitly set `--pprof-http` when starting up Vitess components.

#### <a id="healthcheck-dial-concurrency-flag"/>New `--healthcheck-dial-concurrency` flag

The new `--healthcheck-dial-concurrency` flag defines the maximum number of healthcheck connections that can open concurrently. This limit is to avoid hitting Go runtime panics on deployments watching enough tablets [to hit the runtime's maximum thread limit of `10000`](https://pkg.go.dev/runtime/debug#SetMaxThreads) due to blocking network syscalls. This flag applies to `vtcombo`, `vtctld` and `vtgate` only and a value less than the runtime max thread limit _(`10000`)_ is recommended.

## <a id="minor-changes"/>Minor Changes

### <a id="new-stats"/>New Stats

#### <a id="vttablet-query-cache-hits-and-misses"/>VTTablet Query Cache Hits and Misses

VTTablet exposes two new counter stats:

 * `QueryCacheHits`: Query engine query cache hits
 * `QueryCacheMisses`: Query engine query cache misses

### <a id="sighup-reload-of-grpc-client-auth-creds"/>`SIGHUP` reload of gRPC client static auth creds

The internal gRPC client now caches the static auth credentials and supports reloading via the `SIGHUP` signal. Previous to v20 the credentials were not cached. They were re-loaded from disk on every use.
