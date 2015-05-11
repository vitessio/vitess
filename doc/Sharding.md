# Sharding in Vitess

This document describes the various options for sharding in Vitess.

## Range-based Sharding

This is the out-of-the-box sharding solution for Vitess. Each record
in the database has a Sharding Key value associated with it (and
stored in that row). Two records with the same Sharding Key are always
collocated on the same shard (allowing joins for instance). Two
records with different sharding keys are not necessarily on the same
shard.

In a Keyspace, each Shard then contains a range of Sharding Key
values. The full set of Shards covers the entire range.

In this environment, query routing needs to figure out the Sharding
Key or Keys for each query, and then route it properly to the
appropriate shard(s). We achieve this by providing either a sharding
key value (known as KeyspaceID in the API), or a sharding key range
(KeyRange). We are also developing more ways to route queries
automatically with [version 3 or our API](VTGateV3.md), where we store
more metadata in our topology to understand where to route queries.

### Sharding Key

Sharding Keys need to be compared to each other to enable range-based
sharding, as we need to figure out if a value is within a Shard's
range.

Vitess was designed to allow two types of sharding keys:
* Binary data: just an array of bytes. We use regular byte array
  comparison here. Can be used for strings. MySQL representation is a
  VARBINARY field.
* 64 bits unsigned integer: we first convert the 64 bits integer into
  a byte array (by just copying the bytes, most significant byte
  first, into 8 bytes). Then we apply the same byte array
  comparison. MySQL representation is an bigint(20) UNSIGNED .

A sharded keyspace contains information about the type of sharding key
(binary data or 64 bits unsigned integer), and the column name for the
Sharding Key (that is in every table).

To guarantee a balanced use of the shards, the Sharding Keys should be
evenly distributed in their space (if half the records of a Keyspace
map to a single sharding key, all these records will always be on the
same shard, making it impossible to split).

A common example of a sharding key in a web site that serves millions
of users is the 64 bit hash of a User Id. By using a hashing function
the Sharding Keys are evenly distributed in the space. By using a very
granular sharding key, one could shard all the way to one user per
shard.

Comparison of Sharding Keys can be a bit tricky, but if one remembers
they are always converted to byte arrays, and then converted, it
becomes easier. For instance, the value [ 0x80 ] is the mid value for
Sharding Keys. All byte sharding keys that start with numbers strictly
lower than 0x80 are lower, and all byte sharding keys that start with
number equal or greater than 0x80 are higher. Note that [ 0x80 ] can
also be compared to 64 bits unsigned integer values: any value that is
smaller than 0x8000000000000000 will be smaller, any value equal or
greater will be greater.

### Key Range

A Key Range has a Start and an End value. A value is inside the Key
Range if it is greater or equal to the Start, and strictly less than
the End.

Two Key Ranges are consecutive if the End of the first one is equal to
the Start of the next one.

Two special values exist:
* if a Start is empty, it represents the lowest value, and all values
  are greater than it.
* if an End is empty, it represents the biggest value, and all values
  are strictly lower than it.

Examples:
* Start=[], End=[]: full Key Range
* Start=[], End=[0x80]: Lower half of the Key Range.
* Start=[0x80], End=[]: Upper half of the Key Range.
* Start=[0x40], End=[0x80]: Second quarter of the Key Range.

As noted previously, this can be used for both binary and uint64
Sharding Keys. For uint64 Sharding Keys, the single byte number
represents the most significant 8 bits of the value.

### Range-based Shard Name

The Name of a Shard when it is part of a Range-based sharded keyspace
is the Start and End of its keyrange, printed in hexadecimal, and
separated by a hyphen.

For instance, if Start is the array of bytes [ 0x80 ] and End is the
array of bytes [ 0xc0 ], then the name of the Shard will be: 80-c0

We will use this convention in the rest of this document.

### Sharding Key Partition

A partition represent a set of Key Ranges that cover the entire space. For instance, the following four shards are a valid full partition:
* -40
* 40-80
* 80-c0
* c0-

When we build the serving graph for a given Range-based Sharded
Keyspace, we ensure the Shards are valid and cover the full space.

During resharding, we can split or merge consecutive Shards with very
minimal downtime.

### Resharding

Vitess provides a set of tools and processes to deal with Range Based Shards:
* [Dynamic resharding](Resharding.md) allows splitting or merging of shards with no
  read downtime, and very minimal master unavailability (<5s).
* Client APIs are designed to take sharding into account.
* [Map-reduce framework](https://github.com/youtube/vitess/blob/master/java/vtgate-client/src/main/java/com/youtube/vitess/vtgate/hadoop/README.md) fully utilizes the Key Ranges to read data as
  fast as possible, concurrently from all shards and all replicas.

### Cross-Shard Indexes

The 'primary key' for sharded data is the Sharding Key value. In order
to look up data with another index, it is very straightforward to
create a lookup table in a different Keyspace that maps that other
index to the Sharding Key.

For instance, if User ID is hashed as Sharding Key in a User keyspace,
adding a User Name to User Id lookup table in a different Lookup
keyspace allows the user to also route queries the right way.

With the current version of the API, Cross-Shard indexes have to be
handled at the application layer. However, [version 3 or our API](VTGateV3.md)
will provide multiple ways to solve this without application layer changes.

## Custom Sharding

This is designed to be used if your application already has support
for sharding, or if you want to control exactly which shard the data
goes to. In that use case, each Keyspace just has a collection of
shards, and the client code always specifies which shard to talk to.

The shards can just use any name, and they are always addressed by
name. The API calls to vtgate are ExecuteShard, ExecuteBatchShard and
StreamExecuteShard. None of the *KeyspaceIds, *KeyRanges or *EntityIds
API calls can be used.

The Map-Reduce framework can still iterate over the data across multiple shards.

Also, none of the automated resharding tools and processes that Vitess
provides for Range-Based sharding can be used here.

Note: the *Shard API calls are not exposed by all clients at the
moment. This is going to be fixed soon.

### Custom Sharding Example: Lookup-Based Sharding

One example of Custom Sharding is Lookup based: one keyspace is used
as a lookup keyspace, and contains the mapping between the identifying
key of a record, and the shard name it is on. When accessing the
records, the client first needs to find the shard name by looking it
up in the lookup table, and then knows where to route queries.
