# RFC: Enhancements to Global Routing for Unsharded Tables

## Overview

This RFC proposes enhancements to the global routing mechanism in Vitess. The goal is to ensure
that unique tables from keyspaces without defined vschemas are globally routable. This document discusses the
current global
routing features, the proposed changes, and provides examples to illustrate the impact of these changes.

## Motivation

Vitess has two ways of addressing tables: using qualified names where the keyspace is specified or using unqualified
table names. Example: `keyspace1.table1` vs. `table1`. Tables are currently only globally routable if

* there is only one keyspace in the cluster, which is unsharded, or
* if there are multiple keyspaces and the unique tables are defined in the `vschema` for all keyspaces from which you
  want tables to be globally routable.

This has a catastrophic consequences of this logic. One example:

* User has a single unsharded keyspace `unsharded1` and are using unqualified table names, because their app had been 
  written 
  using unqualified names while targeting a regular MySQL db. `vtgate` correctly routes this because there is only 
  one unsharded keyspace: there is no vschema to consult because it was not necessary at this point to create one 
  for the unsharded keyspace.
* User wants to reshard some large tables. Say,  A `MoveTables` workflow is started into a sharded 
  keyspace, 
say, 
  `sharded1`, which, obviously, has a vschema with all tables defined. Say `table2` is moved in this workflow but 
  `table1` continues to live in `unsharded1`
  So tables with the same name `table2` exist in both the user's db as well as in `sharded1`. Routing rules have 
  already been setup so that the unqualified tables are routed to `unsharded1`. `sharded1` is still not-serving, so 
  the tables in `sharded1` are "invisible" to `vtgate`.
* When the `MoveTables`has caught up and the user does a `SwitchWrites` (i.e.`SwitchTraffic` for primary) `sharded1` 
  is now serving, *but* routing rules are updated to point to `sharded`, so the global routing _just_ works
* The problem comes when user does a `Complete` on the workflow to clean things up. 

A similar problem also holds if the user started with just one unsharded keyspace in Vitess and uses MoveTables to move
some of the tables into a sharded keyspace.

## Current Global Routing Features

### Global Routable Tables

In Vitess, a table is considered globally routable if it meets the following criteria:

- The table exists in a single unsharded keyspace.
- The table exists in multiple unsharded keyspaces but is identical in schema and vindex configuration.

### Ambiguous Tables

A table is considered ambiguous if:

- The table exists in multiple unsharded keyspaces with different schemas or vindex configurations.
- The table exists in both sharded and unsharded keyspaces.

### Example

Consider the following keyspaces and tables:

- `keyspace1` (unsharded): `table1`, `table2`
- `keyspace2` (unsharded): `table2`, `table3`
- `keyspace3` (sharded): `table4`

In this scenario:

- `table1` is globally routable.
- `table2` is ambiguous because it exists in multiple unsharded keyspaces with potentially different configurations.
- `table3` is globally routable.
- `table4` is globally routable because it exists in a sharded keyspace for which vschema is defined.

## Proposed Changes

The proposed changes aim to make tables from keyspaces without defined vschemas globally routable. Specifically, the
changes include:

1. **Automatic Inclusion of Tables from Keyspaces without Vschemas**: Tables from keyspaces that do not have vschemas
   defined will be automatically included in the global routing table.
2. **Conflict Resolution**: In case of conflicts (i.e., tables with the same name in multiple keyspaces), the table will
   be marked as ambiguous.

### Example

Consider the following keyspaces and tables after the proposed changes:

- `keyspace1` (unsharded, no vschema): `table1`, `table2`
- `keyspace2` (unsharded, no vschema): `table2`, `table3`
- `keyspace3` (sharded): `table4`

In this scenario:

- `table1` is globally routable.
- `table2` is ambiguous because it exists in multiple unsharded keyspaces.
- `table3` is globally routable.
- `table4` is not globally routable because it exists in a sharded keyspace.

## Benefits

- **Improved Global Routing**: Ensures that tables from keyspaces without vschemas are included in the global routing
  table, preventing hard-down situations as detailed above.
- **Simplified Configuration**: Reduces the need for explicit vschema definitions for unsharded keyspaces, simplifying
  the configuration process.
