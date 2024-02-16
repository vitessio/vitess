## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Query Compatibility](#query-compatibility)**
    - [Vindex Hints](#vindex-hints)
    - [Update with Limit Support](#update-limit)
    - [Update with Multi Table Support](#multi-table-update)
    - [Delete with Subquery Support](#delete-subquery)
  - **[Flag changes](#flag-changes)**
    - [`pprof-http` default change](#pprof-http-default)
- **[Minor Changes](#minor-changes)**
  - **[New Stats](#new-stats)**
    - [VTTablet Query Cache Hits and Misses](#vttablet-query-cache-hits-and-misses)

## <a id="major-changes"/>Major Changes

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

#### <a id="delete-subquery"/> Delete with Subquery Support

Support is added for sharded table delete with subquery

Example: `delete from t1 where id in (select col from t2 where foo = 32 and bar = 43)`


### <a id="flag-changes"/>Flag Changes

#### <a id="pprof-http-default"/> `pprof-http` Default Change

The `--pprof-http` flag, which was introduced in v19 with a default of `true`, has now been changed to default to `false`.
This makes HTTP `pprof` endpoints now an *opt-in* feature, rather than opt-out.
To continue enabling these endpoints, explicitly set `--pprof-http` when starting up Vitess components.


## <a id="minor-changes"/>Minor Changes

### <a id="new-stats"/>New Stats

#### <a id="vttablet-query-cache-hits-and-misses"/>VTTablet Query Cache Hits and Misses

VTTablet exposes two new counter stats:

 * `QueryCacheHits`: Query engine query cache hits
 * `QueryCacheMisses`: Query engine query cache misses
