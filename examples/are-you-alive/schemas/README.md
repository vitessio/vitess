# Test Schemas

MySQL schemas and Vitess schemas that must be applied to your cluster to use
this test helper.  They should be applied using `vtctlclient` like this:

```
vtctlclient --server "<vtctld_server>" ApplySchema -- --sql "$(cat create_test_table.sql)" <database_name>
vtctlclient --server "<vtctld_server>" ApplyVSchema -- --vschema "$(cat vschema.json)" <database_name>
```
