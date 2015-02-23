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
- simplify schema updates on the fleet
- minimize human actions / errors
- guarantee no or very little downtime for most schema updates
- do not store any permanent schema data in Topology Server, just use it for actions.
- only look at tables for now (not stored procedures or grants for instance, although they both could be added fairly easily in the same manner)

We’re trying to get reasonable confidence that a schema update is going to work before applying it. Since we cannot really apply a change to live tables without potentially causing trouble, we have implemented a Preflight operation: it copies the current schema into a temporary database, applies the change there to validate it, and gathers the resulting schema. After this Preflight, we have a good idea of what to expect, and we can apply the change to any database and make sure it worked.

The Preflight operation takes a sql string, and returns a SchemaChangeResult:
```go
type SchemaChangeResult struct {
 Error        string
 BeforeSchema *SchemaDefinition
 AfterSchema  *SchemaDefinition
}
```

The ApplySchema action applies a schema change. It is described by the following structure (also returns a SchemaChangeResult):
```go
type SchemaChange struct {
 Sql              string
 Force            bool
 AllowReplication bool
 BeforeSchema     *SchemaDefinition
 AfterSchema      *SchemaDefinition
}
```

And the associated ApplySchema remote action for a tablet. Then the performed steps are:
- The database to use is either derived from the tablet dbName if UseVt is false, or is the _vt database. A ‘use dbname’ is prepended to the Sql.
- (if BeforeSchema is not nil) read the schema, make sure it is equal to BeforeSchema. If not equal: if Force is not set, we will abort, if Force is set, we’ll issue a warning and keep going.
- if AllowReplication is false, we’ll disable replication (adding SET sql_log_bin=0 before the Sql).
- We will then apply the Sql command.
- (if AfterSchema is not nil) read the schema again, make sure it is equal to AfterSchema. If not equal: if Force is not set, we will issue an error, if Force is set, we’ll issue a warning.

We will return the following information:
- whether it worked or not (doh!)
- BeforeSchema
- AfterSchema

### Use case 1: Single tablet update:
- we first do a Preflight (to know what BeforeSchema and AfterSchema will be). This can be disabled, but is not recommended.
- we then do the schema upgrade. We will check BeforeSchema before the upgrade, and AfterSchema after the upgrade.

### Use case 2: Single Shard update:
- need to figure out (or be told) if it’s a simple or complex schema update (does it require the shell game?). For now we'll use a command line flag.
- in any case, do a Preflight on the master, to get the BeforeSchema and AfterSchema values.
- in any case, gather the schema on all databases, to see which ones have been upgraded already or not. This guarantees we can interrupt and restart a schema change. Also, this makes sure no action is currently running on the databases we're about to change.
- if simple:
 - nobody has it: apply to master, very similar to a single tablet update.
 - some tablets have it but not others: error out
- if complex: do the shell game while disabling replication. Skip the tablets that already have it. Have an option to re-parent at the end.
 - Note the Backup, and Lag servers won't apply a complex schema change. Only the servers actively in the replication graph will.
 - the process can be interrupted at any time, restarting it as a complex schema upgrade should just work.

### Use case 3: Keyspace update:
- Similar to Single Shard, but the BeforeSchema and AfterSchema values are taken from the first shard, and used in all shards after that.
- We don't know the new masters to use on each shard, so just skip re-parenting all together.

This translates into the following vtctl commands:

```
PreflightSchema {-sql=<sql> || -sql_file=<filename>} <tablet alias>
```
apply the schema change to a temporary database to gather before and after schema and validate the change. The sql can be inlined or read from a file.
This will create a temporary database, copy the existing keyspace schema into it, apply the schema change, and re-read the resulting schema.

```
$ echo "create table test_table(id int);" > change.sql
$ vtctl PreflightSchema -sql_file=change.sql nyc-0002009001
```

```
ApplySchema {-sql=<sql> || -sql_file=<filename>} [-skip_preflight] [-stop_replication] <tablet alias>
```
apply the schema change to the specific tablet (allowing replication by default). The sql can be inlined or read from a file.
a PreflightSchema operation will first be used to make sure the schema is OK (unless skip_preflight is specified).

```
ApplySchemaShard {-sql=<sql> || -sql_file=<filename>} [-simple] [-new_parent=<tablet alias>] <keyspace/shard>
```
apply the schema change to the specific shard. If simple is specified, we just apply on the live master. Otherwise, we do the shell game and will optionally re-parent.
if new_parent is set, we will also reparent (otherwise the master won't be touched at all). Using the force flag will cause a bunch of checks to be ignored, use with care.

```
$ vtctl ApplySchemaShard --sql-file=change.sql -simple vtx/0
$ vtctl ApplySchemaShard --sql-file=change.sql -new_parent=nyc-0002009002 vtx/0
```

```
ApplySchemaKeyspace {-sql=<sql> || -sql_file=<filename>} [-simple] <keyspace>
```
apply the schema change to the specified shard. If simple is specified, we just apply on the live master. Otherwise we will need to do the shell game. So we will apply the schema change to every single slave.
