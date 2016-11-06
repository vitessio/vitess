# VTGate V3 Vindex design

## Updates

Some changes have been made to the system since the last time the doc was written. This is the list:

* More flexibility has been added for using table names:
  * Allow applications to specify a keyspace at connection time. If so, an unqualified reference to a table is assumed to belong to the connection’s keyspace.
  * Support constructs like `keyspace.table`, which will force VSchema to only look inside that keyspace for that table.
  * Allow duplicate table names across keyspaces. If so, one of the above two methods can be used by the application for disambiguation.
  * If the application uses an anonymous connection, then we still allow queries to reference tables without qualification, as long as there are no duplicates.
* There's one VSchema per keyspace instead of a global one.
* The concept of Table Class has been eliminated.
* A new `sequence` table has been introduced, and sequences are orthogonal to vindexes. Any column can now be tied to a sequence.
* We've introduced the concept of a pinned table, which allows an unsharded table to be pinned to a keypsace id, allowing you to avoid creating a separate keyspace for tiny tables. This is yet to be implemented.
* Instead of a vschema editor, the DDL language will be extended to manage vindexes, and the DDL deployment tools will perform the necessary work.

## Objective

The goal of the V3 API is to simplify the application’s view of the physical databases served by VTGate. At a high level, this means that the application should not have to specify either the keyspace or the keyspace id while issuing queries to VTGate.

## Background

When vitess was originally conceived, our main goal was to make sure that we provided a way for the applications to continue to scale. However, we had presumed that a necessary cost was that the applications be rewritten to use a new API where they shared the burden of figuring out the routing for shards.

In the original implementation, the application needed to figure out the exact shard to which a query had to be sent. We later improved this situation with VTGate. We started off with the V1 API which required the application to specify the target shard for queries. This was replaced with V2 that required the app to specify the keyspace_id. The main advantage of V2 over V1 is that an application will survive a dynamic resharding event. It also simplified the application because it didn’t need to access the topology information any more.

However, we felt that vitess will appeal to a wider audience if we provided a standard db-compliant API. Apart from lowering the barrier to entry, it will encourage more people to try vitess because there's less fear of getting locked into a custom API. This means that VTGate should be able to figure out the target shard(s) of a query just from the info in the query itself.

## Overview

On top of addressing the problems expressed in the background, we have some additional requirements:

1. **Routing**: We must be able to figure out the routing based on the query and bind variables only. The target keyspace can be computed from the table name, and the shard(s) from the where clause (or insert values). However, the app still needs to specify the tablet type. The reason why this requirement is significant is because it will allow us to build database-compliant APIs. This will allow easier migration for apps. Additionally, we’ll automatically leverage all past tools like DAOs and OR mappers that were built to work with databases.
2. **Lookup**: Encapsulate and hide all lookup tables and keep them up-to-date.
3. **Pluggable sharding scheme**: Not all customers may want to use the sharding scheme provided by vitess. In fact, we ourselves are thinking of iterating on our current scheme. So, we need a way to support any sharding scheme as the system evolves.
4. **Routing schema**: There is config info for how queries have to be routed for each table. We need a mechanism to edit, store and retrieve this information. This should be both interactive and scriptable.
5. **Transactions**: Ideally, every transaction should be transparently ACID irrespective of whether it spans shards or keyspaces. However, we know that this is a far-fetched goal. We’ll start off with our current best-effort scheme and iterate towards better consistency.
6. **Performance**: Overall, V3 performance should be comparable to V2 for the common case scenarios. There will be situations where a query in V2 may outperform a V3 query. In a V2 API, the application can tell VTGate: “Trust me, I know this data is only here”, or “I know it’s safe to delete this”. But V3 cannot always assume such things. So, there will be situations where it needs to verify.

As simple as it may sound, some of these requirements are not trivial, especially #1. SQL allows a very wide range of queries. There are joins, unions, aggregations, order by, limit, etc. We can, however, handle the common cases. We must be able to tell the difference between what we can and cannot handle, and return an error when we cannot. We’re hoping that the number of such queries is low, and that the apps can rewrite them appropriately. After all, you can’t scale well if your queries are too complex.

At the end of the day, for those who want to try out vitess, they should be able to switch their app to use our client API, and everything should work mostly as is.

## Detailed design

### What is not changing

The new design is going for a pluggable sharding/routing scheme. However, there are parts of our system that are more or less cast in stone:

* We have a range-based sharding scheme.
* There is no overlap between shards.
* The full list of shards covers the entire range of sharding key values.
* The sharding key (or a computable version of it) must be stored on every table.
* The sharding key is a KeyspaceId (string), which can also be a uint64 binary-encoded as a KeyspaceId.

The above intangibles also establish the base contract between VTGate and its plugins: Given an input value, the plugin must be capable of returning the corresponding keyspace id(s). Beyond this, VTGate does not need to know anything about the details of this computation, because this info is sufficient to unambiguously route the query. Of course, there are more conversations that can be had. Those will be expanded in the details below.

### The vindex

If you look at this routing functionality from a database perspective, it’s essentially a cross-shard column index. An index gives you the assurance that you’ll get the same result if you just scanned a subset of rows instead of the entire table. In the case of a routing index, it tells you that you only need to look at a subset of all the shards to come to the same result.

As a matter of fact, if we made the effort, the user could issue a ‘create index’ DDL command that could transparently create such a vindex, populate it, and keep it up-to-date. However, there are practical considerations related to downtime, consistency and error recovery. But the analogy is important to keep in mind, as it will help us enrich the initial design with richer functionality in the future.

We introduce the new term ‘vindex’ so that we can differentiate from the traditional database index. Some definitions:

#### The vindex type

The vindex type is basically an indexing scheme represented by code. It can be a lookup index, or a hash, or anything else. A vindex type has a name that’s indicative of what kind of algorithm it implements to map a value to a keyspace id. A vindex type is not associated to any particular keyspace id or table. The list of vindex types is a very static list. New types have to be compiled into vitess.

For example, a “lookup\_hash\_unique\_autoinc” index is one that will use a lookup table to convert a value, and hash it to compute the keyspace\_id. Additionally, it ensures that the values are unique, and it’s also capable of generating new values if needed.

There is a hint here that there may be a “lookup\_hash\_autoinc”. Indeed there is. Just like database indexes, there are practical justifications for non-unique vindexes. In the future, we can also explore composite vindexes.

This is the currently supported list of vindex types:

* **numeric**: binpack a uint64 into a keyspace_id
* **hash**: hashes a uint64 into a keyspace_id
* **hash_autoinc**: Same as hash, but can generate autoincrement values
* **lookup\_hash**: Uses a lookup table, and hashes the result to generate a keyspace\_id. It’s non-unique.
* **lookup\_hash\_unique**: lookup\_hash, but unique
* **lookup\_hash\_autoinc**
* **lookup\_hash\_unique\_autoinc**

In the future, if we decide to go with our alternate sharding scheme where we require the main id to be stored with each table instead of the keyspace_id, the above list covers those needs also.

#### The vindex (itself)

When a vindex type is instantiated into a keyspace, then we call this a vindex. Vindexes may sometimes refer to a lookup table where they store and retrieve lookup info. Such tables usually live in an unsharded keyspace.

#### The ColVindex

When a table references a vindex and associates it to one of its columns, then it’s called a ColVindex. It’s basically a column name associated with a vindex.

#### The Table Class

In a well-designed schema, you’d use uniform column names to mean the same thing. This means that the list of ColVindexes used by various tables becomes repetitive. In order to handle this, we create a Table Class. This class combines a set of ColVindexes together. Then, all tables that have that same set can refer to that class instead of repeating the same list everywhere. If a table has a unique set of ColVindexes, the convention is to create a class of the same name as the table.

### The contract

The guiding principle behind a vindex is that it has to be invisible to the app, just like a database index. This means that new entries need to be transparently created for lookup indexes when new rows are inserted, and cleaned up accordingly when rows are deleted. If you keep this in mind, it will be easy to understand the motivation behind the contract.

#### The Vindex interface

Every vindex type is required to implement this interface (but it’s not sufficient). This interface requires every index to publish its Cost, which is currently just a number. VTGate will always prefer a lower cost vindex over higher ones.

The interface also requires you to define a Verify function. This function must return true if the given value has a valid mapping from the key to the specified KeyspaceId. The reason why every vindex must provide this verification function is for handling inserts. For every insert, VTGate will compute the associated keyspace id (explained later). It will then ask every ColVindex of that table to verify that the value supplied for that column can map to the computed keyspace id. If, for any reason, the verification fails, then the insert is failed.

#### The Unique and NonUnique interfaces

Every vindex must define a Map function. Both the interfaces require a Map function, but have different signatures. If the vindex yields unique values, then it must define the Map of the Unique interface. This automatically forces a vindex to make a decision whether it’s Unique or NonUnique. These also happen to be mutually exclusive properties.

The Map function, along with the Vindex interface is sufficient to define a valid vindex type. The rest of the interfaces are required for improved transparency of vindexes.

#### Functional and Lookup interfaces

Functional and Lookup interfaces are also mutually exclusive. A vindex type must choose one. The purpose of these interfaces is for managing the rows in a lookup database.

A functional vindex is one that can compute the keyspace id without looking up an external data source. This means that the Create function is only meant for reserving that id so that no one else can use it. The Create function does not contain the keyspace id as a parameter because that is computable. If you do not need to enforce uniqueness or require generating new values, then the ‘Functional’ interface of a functional vindex is unnecessary.

A lookup vindex is one that requires you to create an association between a value and keyspace id. Consequently, the Create function of a Lookup vindex takes the KeyspaceId as additional parameter. Unlike functional vindexes, lookup vindexes will work only if prior associations are created, either by the app, or by VTGate.

Both interfaces also require a Delete function which will be used to delete vindex entries when needed.

#### FunctionalGenerator and LookupGenerator interfaces

These are extensions of their counterparts. A Generator interface requires a Generate function that VTGate will use to generate new values for a vindex.

If a vindex type does not define a Generator interface, then inserts that have no value supplied for such columns will fail if they’re not otherwise computable. If values are supplied, then they will succeed as long as the Verify succeeds.

#### The Reversible interface

This is another optional interface. If a vindex defines it, then VTGate can use it to reverse-map the value from the keyspace id, and use it to populate a column on inserts. The purpose of this interface is to hide columns like keyspace_id that the app doesn’t care about.

#### The VCursor

The VCursor is an interface that VTGate has to create a variable for. This contains an Execute function that’s tied to the current session. Vindexes have the option of using this variable to execute DMLs that insert, update or delete rows in the lookup database. These will then be included as part of the current transaction that VTGate is managing.

This gives rise to distributed transaction consistency issues that are discussed later.

#### The VSchema

The full set of keyspaces, tables, their classes all the way to the vindexes constitutes the vschema. This is currently represented as a JSON file.

### The rules

By the fact that vindexes have unique properties, there are automatic restrictions on where they can or cannot be used. These rules can be logically derived, but it’s not obvious. So, it’s better if these are spelled out.

#### The vindex owner

A vindex can specify an owner table. This means that VTGate will automatically call the Create, Generate or Delete functions when rows in the owner table are created or deleted. This implies that vindexes can be shared by multiple tables. If vindexes are shared, a row must be created in the owner table first before other sub-table rows are created for that main table row. Conversely, an owner table row should be deleted only after all corresponding sub-table rows are deleted first. There is no efficient way to enforce this rule. So, we must rely on good behavior from the app.

#### The primary ColVindex

Every table needs a primary ColVindex. It’s always the first ColVindex of the table. VTGate will use this ColVindex to compute the keyspace id for the row on inserts. The value for a ColVindex must either be supplied by the app, or it should have a Generate function.

This means that an *owned primary ColVindex cannot be a Lookup*. If it was owned, then it would need to call Create, but the Create function needs the keyspace id, which is not computed yet.

A table *could have a Lookup ColVindex that is primary but not owned*. In that case, VTGate will just use the Map function of the Vindex to compute the keyspace_id.

*A primary vindex must also be unique.* Otherwise, VTGate cannot compute a unique keyspace id for the row being inserted.

#### The non-primary ColVindex

Any ColVindex that is not the first one is considered non-primary. For these, VTGate uses the computed keyspace id to validate or populate them:

* If the vindex is owned
  * If no value was supplied
    * If the vindex has a generator, then the generator is used to create an association between the newly generated value and the keyspace id.
    * *Functional vindexes are not allowed* here because there is no way to guarantee that a generated value that did not use keyspace id as input will map to a previously computed keyspace id.
  * If a value was specified
    * If the vindex is a lookup, then we use the Create function
    * Again, *functional vindexes are not allowed here*
* If the vindex is not owned
  * If no value was supplied and if the vindex has a ReverseMap, it’s used to compute and fill the value. Otherwise, the insert is failed
  * If a value was specified, then VTGate uses Verify to make sure that it agrees with the keyspace_id.

#### Unique table name

One of Vitess’ features is transparent vertical resharding. This means that a table can migrate from one keyspace to another. In order to support this, the application needs to be agnostic of the physical location of a table. This automatically means that a table has to be unique across the entire set of keyspaces.

Although this is currently not enforced by the rest of the vitess system, VTGate will start enforcing this as one of the vschema constraints.

### Plan building

When a query is received and parsed into an AST, the plan builder first analyzes the complexity of the query. If there are any constructs that it cannot handle, it returns a NoPlan and documents a reason code, which is essentially an error for the app.

Once the query passes the complexity check, it’s branched off into different analysis paths depending on the statement type. For example, VTGate will currently reject any queries that involve unions, joins or subqueries. It will be an ongoing project to support more and more such constructs.

#### selects

For selects, we try to look at the where clause and collect equality constraints that matched a ColVindex. Out of all those matches, we choose the one with the lowest cost.

In the case of a select, if no ColVindex is matched, the query is treated as a scatter.

One of the results of the initial analysis of a query is whether it requires post-processing. This basically means that the results cannot be returned as is to the client. For example, aggregations, order by, etc. are post-processing constructs. If the select had any such constructs, then the initial implementation of VTGate will fail queries that target more than one keyspace_id. Having VTGate handle post-processing constructs will be another ongoing project that will include more and more use cases as it evolves.

#### updates

The routing of updates is similar to select. We use the same strategy. However, multi-keyspace-id updates are not allowed because our resharding tools cannot handle such statements. Also, VTGate will currently not allow you to modify a ColVindex column. This is because such changes could effectively require us to migrate a row from one shard to another. However, this is definitely something we can look at supporting in the future.

#### inserts

inserts are slightly more involved because we have to guarantee data integrity. We compute the keyspace id using the primary vindex value. Then we verify or generate the rest of the ColVindex values and ensure that everything is consistent. The details of an insert action are already explained in the vindex section.

#### deletes

Deletes are a bigger challenge. If the app issues a delete for a table that has multiple ColVindexes, it would usually specify only one of them in the where clause. However, vitess is responsible for deleting lookup rows for all owned ColVindexes. Also, a delete that matches a ColVindex does not guarantee that such a row will be deleted if there are other constraints in the where clause.

For this reason, VTGate first issues a ‘select for update’ using the specified where clause. And then, issues Vindex deletes only based on the returned rows. Finally, it sends in the actual delete statement to the computed shards.

#### DDLs (not implemented yet)

Should VTGate support DDLs? This is a question that needs to be answered first. The main issue with DDLs is that they’re dangerous, and it may not be wise to allow the app to run them. Also, it’s difficult to repair DDLs that partially failed. So, it may be better to support these using workflows.

However, if VTGate were to support DDLs, this is how it would do it. All DDLs will be treated as full-scatter operations. The table addressed by the DDL must already be present in the vschema. In the case of a create table, a vschema entry must first be created for it, and then the DDL will be issued. The vschema entry will be used to figure out which keyspace the table will be created on.

#### Query splitting

For map-reducers, we need the ability to split queries into ones that target specific shards. The current v2 implementation uses keyranges for this purpose. However, this violates the V3 design constraint that only a tablet type can be specified along with the query and nothing else.

In order to support this, we introduce a new function called keyrange. This is a special construct that VTGate will use and treat it exactly like a V2 keyrange constraint. For example, a query like

`select * from user` will be split into queries like this:

`select * from user where keyrange(‘’, ‘\x80’)`, and

`select * from user where keyrange(‘\x80’, ‘’)`

VTGate will remove these keyrange constructs before passing the query on to the appropriate shard.

### Execution

Execution details were mostly covered in the previous sections.

The Execute function itself will support all plans. ExecuteStream will support only the ‘Select’ plans. ExecuteBatch is currently an open issue. The initial implementation could just loop through the list of requests and ‘Execute’ them, and then return the combined results. In a future optimization, it could analyze the routing upfront, and prepare separate ExecuteBatch calls to the vttablets, and then combine the results in the correct order.

Additionally, in the case of DMLs, the execution appends a keyspace_id comment that can be used to handle resharding.

### The schema editor

In order to make V3 user-friendly, we need the ability to easily maintain the vschema. The schema editor will run as a javascript app under vtctld.

In terms of frameworks, we’ll use AngularJS and twitter’s bootstrap. To the extent possible, we should minimize the amount of html and custom css we write ourselves. A secondary purpose of the schema editor is to prototype a framework for the Vitess dashboard, which will be used to manage all the other workflows.

#### The load workflow

When you fire up the schema editor, it should take you to the load workflow. The default implementation will allow you to load a schema from the topology. It will also allow you to upload a vschema file.

#### The editor

The schema picks up the loaded JSON, parse it and display the various components of the schema in a page where the relationships are easily visualized. The vschema has four main components: keyspaces, tables, table classes and vindexes.

Keyspaces can be on a left navbar. Once you select the keyspaces, it will display the the rest of the three components in one column each.

The schema editor will sanity check the JSON file for inconsistencies and flag them using various color codes:

* Red: This schema file will not compile until you fix the issue. If you hover the mouse over such items, the tooltip should tell you why. For example, a table referring to a class that doesn't exist is an incorrect vschema.
* Yellow: Even though the file will compile, things don’t look consistent. For example, if a table is not referring to a vindex it’s supposed to own, that’s a warning.

Once you’ve performed all the edits, the output will be a JSON, and it will be shown as a diff against the original one. Once you’re satisfied, you go into the ‘Save’ workflow.

Although mostly static, the list of vindex types should not be hardcoded. There is a data structure that generically describes each vindex type and its abilities. This should be supplied by vtctl to the schema editor.

#### The save workflow

The save workflow will show the new JSON file as a diff against the old one. At this point, you have the option of saving it to the topo, or downloading the file.  This is done through an ajax call to vtctld, which first tries to parse the schema. If the parse fails, the save is rejected. If the save is successful, then VTGate can load this schema and route queries accordingly.

There is also an opportunity to revisit this after the auto-schema feature is implemented; a schema change is likely to require a corresponding change in the vschema.

### Tying it all together

At a high level, VTGate will define new entry points to support the new API. These functions will have plain names, unlike the previous versions of the API. For example, instead of ExecuteShard, etc, it will just be Execute.

VTGate will load the vschema from the topo on startup. It can also load it from a file, but we don’t expect to use this feature. There is a separate project to change VTGate’s polling code to use notifications. When this is done, we’ll change VTGate to listen on changes to the vschema.

Once up and running, the same approach as VTTablet will be used to process a query. A brand new query will first be parsed, and the AST will be handed over to a planbuilder that returns a plan. This plan will then be cached for future reuse. Subsequent such queries will just reuse the originally computed plan.

### Monitoring and diagnostics

We have the opportunity to mimic many of vttablet’s features that have helped us in the past:

* /queryz (will display query plans and their stats)
* /schemaz (will display the vschema)
* /querylogz
* /debug/ urls that serve the above data in JSON format
* streamlog

## Future improvements

VTGate has a lot of room to evolve. Many of the features listed below can become their own independent long-running projects with their own design document:

### Transactions

#### 2PC

We know that we’re doing best-effort distributed transactions. This means that we don’t really support cross-shard or cross-keyspace consistency. If we can make 2PC work, then this story will be complete.

This becomes more relevant for the V3 project because VTGate takes over the maintenance of vindex (lookup) tables. What looks like a simple insert on the application side may actually be a multi-keyspace transaction under the covers, for which there is no guaranteed consistency. We’re still better off than V2 where this burden falls on the app, but it’s not good enough.

#### Savepoints

There is a more subtle failure scenario: If the app issues a DML that requires VTGate to also update a vindex. There is a possibility that the vindex update succeeds and the DML fails. Today, we just return an error, but the statement is partially complete. If the app retries that statement, it may fail due to the fact that the vindexes have already changed. Even worse, the app could later commit the transaction which would cause this partial work to be committed.

In order to be consistent, we have to make sure that we rollback all statements we executed to fulfil a request before we return an error.

The way to do this is by using savepoints. MySQL allows you to set savepoints and then partially rollback up to that save point. We need to investigate the viability of using this feature to make sure that a request is either fully applied or any partial work done up to a failure point is reverted.

### Resharding workflows

The current resharding features only support horizontal or vertical splitting. However, we may want to migrate a table out of an unsharded database to sharded one, or vice versa. When we get to this point, we can have tablet-type specific vschemas that route queries for a table as unsharded for one tablet-type, and as sharded for another.

Functionally, this will be a superset of the specific cases we handle today. This means that this scheme will work equally well for vertical sharding. This will also allow us to get rid of the keyspace alias hack.

### Post-processing

Ideally, we’d like to cover the entire SQL grammar and semantics. However, we know that it’s an endeavour with diminishing returns. On the other hand, there’s still a lot of ground to cover before we can reach that point. Here’s a proposed laundry list that we have to tick in order to reach the comfort zone:

* Joins
  * Identify joins that have trivial routing: These are joins where you’d get the same result back if you scattered the join statement to the various shards vs. if you sent the statement to a single database that contained all the data.
  * Cross-keyspace join: Simple join where you load data from one keyspace and use the results to look up another table.
* aggregations & group by: We need to support the most common such functions like count, sum, etc.
* order by: We should be able to support most order by queries using a merge-sort algorithm. Initially, we can support this for numeric and binary columns. We can consider adding unicode sorting if there is a demand for it.
* limit clauses.
* Subqueries

### Live vindex creation

As applications evolve, they may notice that one of their low qps scatter queries is beginning to slam the databases. At that point, they may want to create a new lookup Vindex to make it more efficient.

There is currently no process in vitess to do this. One way would be to create this table and have a special VTGate index that will update this table on DMLs, but will return a scatter plan for selects. After this is setup, we need a workflow that will backfill the vindex with the old data. Once the backfill is done, we should be able to turn on the full functionality of the lookup vindex.
 
## Project Information

A good chunk of the work described in the doc is already complete. The parts that are yet to be done are:

* Schema editor
  * Remove hard coding of vindex types
  * Upload/download file
  * JSON diff
  * Import keyspace & table info from topo & vttablet
  * Testing
  * Add label ACL restrictions
* VTGate
  * ExecuteBatch
  * DDL
  * Schema change notification
  * Audit of error messages
* More comprehensive integration tests
* Monitoring

## Caveats

The current json format for the vschema needs to be revisited. It’s a monolithic file that covers all keyspaces. If you perform keyspace maintenance, then you have to remember to make updates to vschema accordingly. If we instead split this into individual, per-keyspace vschemas, then those would live and die with the keyspaces. It will also make it easier to refresh only a keyspace that has changed. The downside, however, is that it increases the complexity of the system.

## Testing plan

Unit tests have been written to cover every code path that can be seen on production, and expected functionality has been verified.

There is also a vtgatev3_test.py that performs some end-to-end tests. This, coupled with the unit tests, should cover everything needed. However, it may be a good idea to add a few more cases to the integration test for good measure.

Once the new API is established as the only official API of Vitess, we’ll need to migrate our existing V2 tests to V3.
