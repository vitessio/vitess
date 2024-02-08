## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Query Serving](#query-serving)**
    - [Vindex Hints](#vindex-hints)
- **[Minor Changes](#minor-changes)**


## <a id="major-changes"/>Major Changes


### <a id="query-serving"/>Query Serving

#### <a id="vindex-hints"/> Vindex Hints

Vitess now supports Vindex hints that provide a way for users to influence the shard routing of queries in Vitess by specifying, which vindexes should be considered or ignored by the query planner. This feature enhances the control over query execution, allowing for potentially more efficient data access patterns in sharded databases.

Example:
 ```sql
    SELECT * FROM user USE VINDEX (hash_user_id, secondary_vindex) WHERE user_id = 123;
    SELECT * FROM order IGNORE VINDEX (range_order_id) WHERE order_date = '2021-01-01';
 ```

For more information about Vindex hints and its usage, please consult the documentation.

## <a id="minor-changes"/>Minor Changes

