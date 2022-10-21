## Summary

- [New command line flags and behavior](#new-command-line-flags-and-behavior)

## Known Issues

## Major Changes

### New command line flags and behavior

#### VTGate: Support query timeout --query-timeout

`--query-timeout` allows you to specify a timeout for queries. This timeout is applied to all queries.
It can be overridden by setting the `query_timeout` session variable.
Setting it as command line directive with `QUERY_TIMEOUT_MS` will override other values.

### Mysql Compatibility

#### Support for views

Vitess now supports views in sharded keyspace. Views are not created on the underlying database but are logically stored
in vschema.
Any query using view will get re-rewritten as derived table during query planning.
VSchema Example

```json
{
  "sharded": true,
  "vindexes": {},
  "tables": {},
  "views": {
    "view1": "select * from t1",
    "view2": "select * from t2",
  }
}
```

This can also be modified using VSchema DDLs.
`alter vschema <create view definition> | <drop view definition>`
