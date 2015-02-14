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
