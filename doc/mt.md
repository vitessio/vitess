
### vtctldclient

* MoveTablesCreate: validate tenant id is present. Also on deployment, for all tables?

### _vt.vreplication 
* Options column
  * Set options from CLI. 
    * Set keyspace routing rules option if target has multi-tenant mode. 
    * Also set a multi-tenant mode option. 
    * Additional-filter

## VSchema

### Keyspace Routing Rules
* All from PR
* Both primary and replica/rdonly

### TestCases
* add to CI


