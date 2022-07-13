# Online DDL during Resharding

#### (Work In Progress)

## Motivation

Currently, in Vitess, Online DDL and Sharding (MoveTables/Reshard) workflows don't coexist well. It is expected that if
you are resharding you should start an Online DDL only after the resharding cutover. There is no mechanism within Vitess
to prevent or warn against this.

With PSDB introducing Resharding this presents a problem. The only option right now is to either cancel the reshard to
run the ddl or wait till the reshard completes. The latter goes against the PSDB paradigm of easy database branching.
The former will be quite inefficient, could unnecessarily increase costs significantly and could also lead to starvation
where we can never actually complete the reshard.

Ideally we should be able to run Online DDLs on demand, let the Online DDL flow dictate when the queued DDLs run and
make the other workflows be aware of running Online DDL migrations and seamlessly handle the completion (or
cancellation) of these migrations.

In this document we attempt to come up with a design to support this requirement.

Keeping this private for now so we are free to discuss PSDB-specific requirements. Will create an open-source RFC once
we have a working design.

## Design Outline

In this design Online DDL does not need to be reshard-aware. It follows exactly the same flow it is doing today.

To start with, we consider the case where a Reshard is in progress but cannot be switched yet: either it is in the copy
phase or has too much replication lag. Now an Alter (using Online DDL) is run on a table in the source.

What we want to achieve, is to cutover the reshard only when there is no ongoing migration in the keyspace. (Yes, if
DDLs run all the time we will never finish a reshard, but that's to be expected. We can improve this by Online DDL
checking other workflows and if they are close to finishing queuing new DDLs until it is complete).

### The flow in a nutshell

* Online DDL creates the migration records in `_vt.schema_migrations`
* Each VReplication stream on the target interprets the corresponding binlog events
* The stream adds the migration to a one-to-many mapping table in _vt.
* When the shadow table is created, each stream will receive the binlog event. Each stream blocks attempting to create
  the table, first one succeeds, others will find the table already created. All will update their table plans with the
  new table.
*
* Online DDL will `rename` the table when migration completes. Each target stream will identify this when it receives
  the corresponding binlog event and start a synchronization loop:
    * all streams belonging to a workflow will wait until they have each received the rename event. The reason for this
      step is to handle merge workflows where a target stream will be sourcing data from multiple sources: a simple
      example is when a sharded keyspace is going to an unsharded one.
    * All streams will stop (go into Stopped state) when the rename event is received
    * The last stream will
        1. rename the shadow table to the original one
        2. drop the old table being operated on
        3. drop the shadow table from the table plans
        4. signal other streams to start
        5. Other streams will drop the shadow table from the table plans and start Running

* Any requests to cutover will be rejected if a stream noticing that there are ongoing migrations
* Cancelled migrations will be handled by removing the migration from the list when it seems the corresponding DML in
  `_vt.schema_migrations`, shadow table will be removed from the table plans

* *Optimization:* Since the shadow table is itself being created as part of the Online DDL's vreplication workflow it will be seeing
  bulk inserts during its copy phase. In the resharding workflow, unlike other tables, the entire shadow table will be
  replicated in the Replicate phase by processing binlog events. Because of RBR, each bulk insert will be seen by the
  target as a lot of single inserts. To use VReplication-speak they will be seen by the vplayer and not the vcopier. It
  seems easy enough to look-ahead in the vplayer event stream and convert consecutive inserts to the same table into a
  single bulk insert.

## Other Scenarios

We need to validate our design against several other flows, including:

* Create table instead of Alter table

Just need to add the new table to copy_state

`* Online DDL in progress when Reshard starts

Lock keyspace to register new workflows to avoid race conditions.

New reshard workflows can just ask Online DDL for existing migrations and use it as a dependent set for this migration
`

* MoveTables

Do we allow updating schema of tables being moved? With the current approach an Alter of a Table will cause a restart of
the entire MoveTables workflow. Maybe that is acceptable.

* Materialize

Initial thoughts: since we know the columns and table(s) involved in Materialize expressions we can identify if a
particular Materialize flow will be affected by an Online DDL. If the expression is incompatible with the new schema we
should either error out or stop the workflow when the Online DDL is finalized so that the user can fix it. It is
possible that we can auto-magically fix the expression (example, table/column rename or adding a new column to a
group-by if the previous aggregation was on all columns)

* CreateVLookupIndex

* Further downstream reshards of the current targets
