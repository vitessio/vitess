
### vtctldclient

* MoveTablesCreate: add TenantId. Accept string. Type should be in VSchema along with column name

### _vt.vreplication 
* Options column
  * `options` json NOT NULL,
  * binlog_player: CreateVReplicationState
  * Set options from CLI. 
    * Set keyspace routing rules option if target has multi-tenant mode. 
    * Also set a multi-tenant mode option. 
    * Additional-filter
  * Read options at required places
  

## VSchema
### Multi-Tenant mode at Keyspace Level
* column name
* column type

### Keyspace Routing Rules
* All from PR
* Both primary and replica/rdonly

### TestCases

* e2e
  go/test/endtoend/vreplication/multi_tenant_test.go in branch
* add to CI


Steps:
1. Add multi-tenant mode to VSchema
2. 