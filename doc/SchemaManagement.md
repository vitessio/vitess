# Schema Management

The schema is the list of tables and how to create them. It is managed by vtctl.

## Looking at the Schema

The following vtctl commands exist to look at the schema, and validate it's the same on all databases.

```
GetSchema <tablet alias>
```
where \<tablet alias\> is in the format of "\<cell name\>-\<uid\>"

Example:
```
$ vtctl -wait-time=30s GetSchema cell01-01234567
```
displays the full schema for a tablet

```
ValidateSchemaShard <keyspace/shard>
```
where \<keyspace/shard\> is the format of "\<keyspace\>/\<shard\>"

Example:
```
$ vtctl -wait-time=30s ValidateSchemaShard keyspace01/10-20
```
validate the master schema matches all the slaves.

```
ValidateSchemaKeyspace <keyspace>
```
validate the master schema from shard 0 matches all the other tablets in the keyspace.

Example:

```
$ vtctl -wait-time=30s ValidateSchemaKeyspace user
```

## Changing the Schema

Goals:

* simplify schema updates on the fleet
* minimize human actions / errors
* guarantee no or very little downtime for most schema updates
* do not store any permanent schema data in Topology Server, just use it for actions.
* only look at tables for now (not stored procedures or grants for instance, although they both could be added fairly easily in the same manner)

We’re trying to get reasonable confidence that a schema update is going to work before applying it. Since we cannot really apply a change to live tables without potentially causing trouble, we have implemented a Preflight operation: it copies the current schema into a temporary database, applies the change there to validate it, and gathers the resulting schema. After this Preflight, we have a good idea of what to expect, and we can apply the change to any database and make sure it worked.

The Preflight operation takes a sql string, and returns a SchemaChangeResult:

```go
type SchemaChangeResult struct {
 Error        string
 BeforeSchema *SchemaDefinition
 AfterSchema  *SchemaDefinition
}
```

The ApplySchema action applies a schema change to a specified keyspace, the performed steps are:

* It first finds shards belong to this keyspace, including newly added shards in the presence of [resharding event](Resharding.md).
* Validate the sql syntax and reject the schema change if the sql 1) Alter more then 100,000 rows, or 2) The targed table has more then 2,000,000 rows. The rational behind this is that ApplySchema simply applies schema changes to the masters; therefore, a big schema change that takes too much time slows down the replication and may reduce the availability of the overall system.
* Create a temporary database that has the same schema as the targeted table. Apply the sql to it and makes sure it changes table structure. 
* Apply the Sql command to the database.
* Read the schema again, make sure it is equal to AfterSchema.

```
ApplySchema {-sql=<sql> || -sql_file=<filename>} <keyspace>
```
