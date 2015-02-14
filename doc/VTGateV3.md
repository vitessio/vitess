# VTGate V3 design

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
