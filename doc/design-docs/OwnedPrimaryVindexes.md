# Owned Primary Vindexes

This document presents a way to allow for owned lookup vindexes to also be the Primary Vindex.

## Problem statement

An owned lookup vindex is a vindex for which a mapping is created from the input column value(s) to a keyspace id at the time of insert. This necessarily means that the keyspace id has to be known before this mapping can be created.

This also means that an owned vindex cannot be the primary vindex. This is because the primary vindex is the one that determines the keyspace id of a row based on the input value. The essential thinking is: if one can compute the keyspace id for a row, then there is no need to store a mapping for it.

There are, however, situations where these seemingly contradictory situations can co-exist. Here are two use cases:

1. I want to generate a random keyspace id at the time of row insertion, but I want to remember it for later.
2. I want to use multiple column values to compute a keyspace id, but would like to save a mapping from just one column to the computed keyspace id. This way, the lookup can be used in situations where only the mapped column was provided in the where clause. This is an upcoming use case for those who wish to geo-partition their data based on regions.

In the above cases, the vindex is capable of generating a keyspace id, and at the same time, it needs to save that data so that it can be used later when `Map` is called.

## Solution

In order to support the new use cases, the following changes can be made:

1. Extend the Vindex API where a vindex can export a `MapNew` function. This function will generate a keyspace ID.
2. Allow owned lookup vindexes that support the `MapNew`function to be the primary vindex.
3. If a `MapNew` function exists for the primary vindex, the insert will call it. Otherwise, it will use the regular `Map` function.
4. If the vindex is owned, then the regular code for an owned vindex is executed, even if it’s the primary vindex. This is because `MapNew` would have returned us the keyspace id for the row.

Why not a `MapAndCreate` function instead?
A `MapAndCreate` will end up duplicating the work performed by an owned Vindex for the Create part. Having `MapNew` only generate the keyspace id keeps the functionality more orthogonal and composable.

Why do we need a separate `MapNew` function instead of just reusing the `Map` function?
This is to address the first use case. In the first use case, the `MapNew` function will generate a random keyspace id, whereas the `Map` function will perform the lookup.

## Resharding

We currently don’t support resharding through a lookup vindex. This is because a vttablet is not able to read from a lookup table that may be distributed across different keyspaces and shards. Also, performing a lookup for each vreplication row may be a performance bottleneck.

For the first use case of random keyspace id generation, there is no recourse; Looking up the keyspace id may be the only way to reshard. However, this is currently only a hypothetical use case. So, there’s no need to solve this problem immediately.

For the second use case, which addresses the geo-partitioning problem, `Map` and `MapNew` have the same implementation. The only difference is that `MapNew` takes multiple columns as input, whereas `Map` takes only one column.

We have always wanted to extend `Map` to accept multiple columns as input. We now have the opportunity to do so. In such cases, a resharding will be able to use `Map` with all column values as input, thereby avoiding the need to read from lookup tables.

The VTGate itself can continue to just send the first column's value for the vindex. However, VReplication can use all column values to allow for `Map` to return efficiently.