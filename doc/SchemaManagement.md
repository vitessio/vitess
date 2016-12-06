Your MySQL database schema lists the tables in your database and
contains table definitions that explain how to create those tables.
Table definitions identify table names, column names, column types,
primary key information, and so forth.

This document describes the <code>[vtctl](/reference/vtctl.html)</code>
commands that you can use to [review](#reviewing-your-schema) or
[update](#changing-your-schema) your schema in Vitess.

Note that this functionality is not recommended for long-running schema changes. In such cases, we recommend to do a [schema swap](/user-guide/schema-swap.html) instead.

## Reviewing your schema

This section describes the following <code>vtctl</code> commands, which let you look at the schema and validate its consistency across tablets or shards:

* [GetSchema](#getschema)
* [ValidateSchemaShard](#validateschemashard)
* [ValidateSchemaKeyspace](#validateschemakeyspace)

### GetSchema

The <code>[GetSchema](/reference/vtctl.html#getschema)</code> command
displays the full schema for a tablet or a subset of the tablet's tables.
When you call <code>GetSchema</code>, you specify the tablet alias that
uniquely identifies the tablet. The <code>\<tablet alias\></code>
argument value has the format <code>\<cell name\>-\<uid\></code>.

**Note:** You can use the
<code>[vtctl ListAllTablets](/reference/vtctl.html#listalltablets)</code> 
command to retrieve a list of tablets in a cell and their unique IDs.

The following example retrieves the schema for the tablet with the
unique ID <code>test-000000100</code>:

```
GetSchema test-000000100
```

### ValidateSchemaShard

The
<code>[ValidateSchemaShard](/reference/vtctl.html#validateschemashard)</code>
command confirms that for a given keyspace, all of the slave tablets
in a specified shard have the same schema as the master tablet in that
shard. When you call <code>ValidateSchemaShard</code>, you specify both 
the keyspace and the shard that you are validating.

The following command confirms that the master and slave tablets in
shard <code>0</code> all have the same schema for the <code>user</code>
keyspace:

```
ValidateSchemaShard user/0
```

### ValidateSchemaKeyspace

The <code>[ValidateSchemaKeyspace](/reference/vtctl.html#validateschemakeyspace)</code>
command confirms that all of the tablets in a given keyspace have
the the same schema as the master tablet on shard <code>0</code>
in that keyspace. Thus, whereas the <code>ValidateSchemaShard</code>
command confirms the consistency of the schema on tablets within a shard
for a given keyspace, <code>ValidateSchemaKeyspace</code> confirms the
consistency across all tablets in all shards for that keyspace.

The following command confirms that all tablets in all shards have the
same schema as the master tablet in shard <code>0</code> for the
<code>user</code> keyspace:

```
ValidateSchemaKeyspace user
```

## Changing your schema

This section describes the <code>vtctl ApplySchema</code> command, which
supports schema modifications. Vitess' schema modification functionality
is designed the following goals in mind:

* Enable simple updates that propagate to your entire fleet of servers.
* Require minimal human interaction.
* Minimize errors by testing changes against a temporary database.
* Guarantee very little downtime (or no downtime) for most schema updates.
* Do not store permanent schema data in the topology server.

Note that, at this time, Vitess only supports
[data definition statements](https://dev.mysql.com/doc/refman/5.6/en/sql-syntax-data-definition.html)
that create, modify, or delete database tables.
For instance, <code>ApplySchema</code> does not affect stored procedures
or grants.

### ApplySchema

The <code>[ApplySchema](/reference/vtctl.html#applyschema)</code>
command applies a schema change to the specified keyspace on every
master tablet, running in parallel on all shards. Changes are then
propagated to slaves via replication. The command format is:
```
ApplySchema {-sql=<sql> || -sql_file=<filename>} <keyspace>
```

When the <code>ApplySchema</code> action actually applies a schema
change to the specified keyspace, it performs the following steps:

1. It finds shards that belong to the keyspace, including newly added
   shards if a [resharding event](/user-guide/sharding.html#resharding)
   has taken place.
1. It validates the SQL syntax and determines the impact of the schema
   change. If the scope of the change is too large, Vitess rejects it.
   See the [permitted schema changes](#permitted-schema-changes) section
   for more detail.
1. It employs a pre-flight check to ensure that a schema update will
   succeed before the change is actually applied to the live database.
   In this stage, Vitess copies the current schema into a temporary
   database, applies the change there to validate it, and retrieves
   the resulting schema. By doing so, Vitess verifies that the change
   succeeds without actually touching live database tables.
1. It applies the SQL command on the master tablet in each shard.

The following sample command applies the SQL in the **user_table.sql**
file to the **user** keyspace:

```
ApplySchema -sql_file=user_table.sql user
```

#### Permitted schema changes

The <code>ApplySchema</code> command supports a limited set of DDL
statements. In addition, Vitess rejects some schema changes because
large changes can slow replication and may reduce the availability
of your overall system.

The following list identifies types of DDL statements that Vitess
supports:

* <code>CREATE TABLE</code>
* <code>CREATE INDEX</code>
* <code>CREATE VIEW</code>
* <code>ALTER TABLE</code>
* <code>ALTER VIEW</code>
* <code>RENAME TABLE</code>
* <code>DROP TABLE</code>
* <code>DROP INDEX</code>
* <code>DROP VIEW</code>

In addition, Vitess applies the following rules when assessing the
impact of a potential change:

* <code>DROP</code> statements are always allowed, regardless of the
  table's size.
* <code>ALTER</code> statements are only allowed if the table on the
  shard's master tablet has 100,000 rows or less.
* For all other statements, the table on the shard's master tablet
  must have 2 million rows or less.

If a schema change gets rejected because it affects too many rows, you can specify the flag `-allow_long_unavailability` to tell `ApplySchema` to skip this check.
However, we do not recommend this. Instead, you should apply large schema changes by following the [schema swap process](/user-guide/schema-swap.html).
