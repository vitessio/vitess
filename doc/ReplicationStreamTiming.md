# Replication Stream Timing

## Introduction

This document describes a few ideas related to replication stream, timestamps,
and freshness guarantees.

A few ideas are driving this:

* When invalidating a cache, it would be nice to not re-populate it with wrong
  obsolete data.
* Access to TrueTime within Google’s network, or NTP outside of Google’s
  network, can be used to guarantee a good clock.
* Clients may want to read data that is less than N seconds old.
* We may want to do reads that are guaranteed to be at least at a given point in
  time.

This document started as just a gathering of crazy ideas. Then there was a
brainstorming session with Vitess team, then another one with another YouTube
team. Next step is a formal design doc.

## TrueTime / Timestamps

Google supports a
[TrueTime service](http://static.googleusercontent.com/media/research.google.com/en//archive/spanner-osdi2012.pdf)
on its production servers. Basically, the idea is instead of returning the
current time stamp, which might be wrong, TrueTime returns two timestamps,
[min, max], with the guarantee that min <= true time <= max. When using a
timestamp, the application just needs to figure out if it needs the lower or
upper bound for its computation.

For instance, when a Paxos server takes a lease for 30 seconds, it gets the
current true time [min, max], and nobody else can assume mastership until their
current min time is after original max + 30 seconds.

In practice, the difference between min and max is in the millisecond range.

For Vitess in open source, we won’t have a TrueTime service. However, we can
probably assume a configurable maximum clock drift, like 5ms, and return current
wall time minus and plus 5ms. In most public clouds, the provided time service
will be very good and fit in.

## Replication Timestamp

In this part, we’ll look at ways to add an accurate timestamp to replication
events, to measure replication delay.

### Using MySQL’s Timestamp

A first approach is to use the timestamp provided by MySQL. Its granularity
however is the second. Also, it’s the time on the current master, with no
accuracy information.

### Using a Heartbeat service

Another different approach is to use a heartbeat service, that at regular
intervals (every second for instance, let’s say using MasterFrequency), inserts
a timestamp into a table. Reading that table on slaves can give the delay. And
at this point, we can insert the accuracy of the clock too, let’s use MasterMin
and MasterMax as the values we insert in the heartbeat table.

On the slave side, we can read that value SlaveFrequency times per second, get
the time on the slave (SlaveMin and SlaveMax), and diff the right values. We
then know for certain replication delay is smaller than: SlaveMax - MasterMin.

Increasing the accuracy here means inserting data more frequently on the master
(increasing MasterFrequency), which consumes more resources and is not ideal. To
get great accuracy, the QPS on that tablet would get too high. And we depend on
another process.

### Tagging the Statements

Another possible approach is for the master vttablet to always insert its
[MasterMin, MasterMax] value as a SQL comment in the replication stream, for
each DML. The slave vttablet can then read the binlogs of its underlying MySQL
instance, and know exactly its replication delay upper bound. Since it does no
processing on the data, but just remembers the latest Master time, the local
vttablet will be able to keep up with the replication stream easily, and not add
much overhead.

Since the tags are applied by the master, we have the guarantee that when we are
on a given master, the timestamps are increasing. We may also need to enforce
it: if they are not, we wait until they are. That way, after a reparent for
instance, the new master will always have increasing timestamps. Note this is
one of the only place where the accuracy of the time source comes into play,
otherwise we don’t really care, as long at time is increasing.

Note the tag may be lost when using row-based replication. We need to address
this.

### Conclusion

It feels like a combination of the last two solutions will address most cases:
piggy back the current [MasterMin, MasterMax] values if there is traffic, add
dummy values if there isn’t (and we probably don’t need a dedicated table, just
a statement that goes through the replication stream and contains the master
timestamp).

With high QPS on a master, the replication delay is very accurate. With low QPS,
not as much, but then it’s probably not an issue anyway.

## Replica Changes

A vttablet can now read the underlying MySQL replication stream. It can remember
the timestamp of the statements it sees.

With that information, when we read data from vttablet, we can also return the
current timestamp of the replication stream right before we read that data. So
the client would get an idea of the freshness of the data.

Vttablet could also remember a few values (every once in a while) of the mapping
between timestamp and GTID. With that data, it becomes easier to find where to
start if asked to start a replication stream from a timestamp value (as opposed
to a GTID value). (we would still need a fallback way to find the next event
given a timestamp value: asking MySQL for the list of binary logs it has, and
reading the first few events of each might give us enough data as a starting
point.)

## Master Failover

It seems like with timestamps, and master election features, there could be room
for some coordination. But it’s unclear to me what it would look like at the
moment. The master-election software (Orchestrator for instance) already do
their own thing. Guaranteeing no two masters write statements with overlapping
timestamps (to prevent alternate futures) is already done in another way. The
switch-over time is in the order of at least hundreds of milliseconds, far
greater than the clock accuracy.

## Cache Invalidation

Let’s see how we can use the replication stream timestamps for cache
invalidation. Let’s take the scenario where each replica cell has a cell-wide
cache. We want to invalidate the cache when objects change.

The problem here is that we have multiple replicas in a cell, each at a
different point in time in replication. If we invalidate the cache listening to
the update stream of one replica, we have no guarantee that a client will not
use another replica (that is possibly behind) and re-populate the cache with the
old value (cache poisoning).

If the invalidation stream also provides the [MasterMin, MasterMax] pair
associated with an update, it becomes possible to either:

* A/ Add this [MasterMin, MasterMax] to the queries that will re-populate the
  cache, and ask Vitess to only get data fresher than this (new Vitess API).
* B/ Try to re-populate the cache by asking for the data, and ignore the updates
  that are older.

Let’s walk through one possible solution to see how this would look:

* We have a table with a primary key.
* We cache each row from that table in memcache, LRU cache. The contents of that
  cache entry can either be the row itself (along with its timestamp), or just
  the timestamp of the invalidation.
* We listen to an update stream. Each update that comes in, we store the
  timestamp of that statement as invalidation in the cache for that cache
  entry. (Idea: the TTL of that invalidation cache entry only needs to be
  greater than the maximum replication lag of the replicas, after that, having
  an old entry or no entry is equivalent… this is cool for batch jobs that
  change unused data in the background, the invalidation entries will fall of
  the caches really quickly. This is probably too complicated for not enough
  gain).
* When the app needs a row, it first queries the cache. If it gets a row, just
  use it. If it gets an invalidation timestamp, it can then query the
  database. Two possibilities here:
  * A/ We can ask the database for data more recent than the
    invalidation. Vtgate would have to find a good replica, and query it. Con:
    extra API, might have to wait for a replica to get data. Vtgate needs to
    know each replica delay very well, (see Optimizing Health Check
    section). There is a possibility such a replica cannot be found, then what
    do we do?
  * B/ (preferred) We ask any database, and vttablet also returns its last seen
    [MasterMin, MasterMax] pair. If the invalidation time is earlier than
    current response time, store the data in the cache. If not, don’t change the
    cache. Con: may query the database a lot before being able to cache the
    value (note this would query a hot row in the db, so that should be
    fast). In practice, either the row is in high demand, and we will query the
    most advanced DB really quickly and populate the cache, or the row is not in
    high demand, and then it doesn’t matter.

This can be extended to any number of composite objects. Let’s say an entry in
the cache is computed by joining two tables by primary key. Every time either of
them changes, we invalidate the composite cache entry. Then we can re-populate
it easily.

Note: during the initial brainstorming session, it was brought up that if we
could, when we invalidate the cache, also keep the old entry, then the app when
it reads an entry that has both an invalidation and the old value, could decide
with a fudge factor to still use the obsolete cached entry, inside of querying
the database. That way we’d stagger the database traffic due to invalidation. I
am not sure it is easy or convenient or fast or safe to do so, however, because
the invalidation would need to read the current data, and rewrite it, instead of
just dumping a new value in. Maybe using two entries, one for the value, one for
the invalidation, would work, but that seems like too much.

Reliability: if two invalidation streams run at the same time on the same data
stream, they would double the invalidation writes into the cache. Depending on
timing, that may trigger extra database queries to re-populate the cache. It
wouldn’t however be dramatic. So for short periods of time (like transitions due
to resharding, or process restarts that require re-synchronization, …) this
should be fine.

Interaction with cache: if the underlying cache is using consistent hashing to
multiple cache instances, there are cases where an old cached value can be
returned, or we may lose a cache update. We need to run through these cases and
convince ourselves they’re OK.

## Optimizing the Update Stream

Let’s assume we have 4 replicas in one cell, and we want to provide a good
update stream.

By keeping a streaming health check connection to each replicas, we know coarse
grain replication status of each replica. Picking one of the most up to date
ones is easy.

By then monitoring the replication delay from the timestamp value of each
statement, we can see if that replica is keeping up. If not, and there are
better alternatives, we can just hop on to another replica.

Note that for invalidation, it may actually be more beneficial to use the
slowest replica. That way, repopulation will have fewer misses. But in any case,
we should follow the same rules as the serving decision: If we decide to not
serve from a replica, we should also not invalidate from it. The replication
delay of the current replica we use is also the minimum time it takes to
propagate data updates to a cache. So using a replica that is way behind means
we serve stale data a lot. That is a tradeoff where experience will help to find
the right way.

For reliability purposes, the current replication point can be stored regularly
in the cache by the invalidation process. So if the invalidation process needs
to restart, it knows where to restart from. (TODO: A number of alternative
solutions for this exist. Let’s see what’s simpler for resharding too, see
below). Alternatively, we could restart the invalidation stream at a point that
is a couple hours behind current time, and catch up (if that is fast). If the
TTL of the cache is also set to that same couple of hours, it should work.

Watch for: a failover in the invalidation process should not cause cache
poisoning in any case. This might be tricky, especially if a single objects is
changed repeatedly. More research needed here.

## Optimizing Health Check

To optimize the health check between vtgate and vttablet, we could consider the
following:

* On vttablet side, we remember the last time we send health status to a client.
* When returning a result to a vtgate, vttablet will see if the last time the
  status was updated is older than a threshold, and then include the current
  MasterTime to it. (might be small enough that we can return it all the time).
* Every few seconds, vttablet sends a backup health ping to each vtgate that
  hasn’t received health recently.

Note it may be good to dissociate full health check pings (when a replica
becomes a master for instance) that happen very infrequently, from just updates
of the replication lag.

However far we can drive this, we probably still want L2 vtgates so we optimize
the number of vtgates connecting to a single vttablet. The only requirement
there is to have all vttablets for a single shard be addressed by the same
vtgate L2 pool. In other words, a vtgate needs to connect to all replicas for a
shard.

## Read Guarantees

After the Health Check is optimized, one extra API we could easily add is for
the app to provide a freshness guarantee, as in: answer this request from
replica(s) that are less than 30s behind on replication. However, the corner
case we are facing won’t be helped with this: let’s say a cell is falling behind
on replication by a large amount (one hour for instance). We still want to serve
data (that we know is stale), rather than erroring out, or sending traffic
somewhere else (that would kill that other place). So the app saying ‘fresher
than 30s’ is not going to be respected in that case. Having the app specify a
value that’s not gonna be respected seems… wrong.

But we could even go further: each DML (or commit) could return the timestamp of right before it was submitted. Then we could use that in subsequent queries to replicas to guarantee that the data read is at least at that point. This would be an alternative to critical reads:

* Pros:
  * Critical reads are not going to the master any more.
* Cons:
  * The subsequent reads will be delayed, as they need a slave to have applied
    the master statement.
  * Vtgate needs to be ultra aware of the replication delay of each slave, to
    always send these queries to one of the most advanced slaves.

If the app could store the object modification time as a cookie, then it can
just re-use that when re-querying the database later. A typical use of this
would be for a user modifying her own data. A servlet call does the update,
saves in the session the update time, and next UI requests will ask for data
fresher than the last server update time. Since subsequent queries are done
after a client-server round-trip (most likely 10s of milliseconds), at that
point the data will most likely be on the replicas and vtgate will know about
it. Note the application already needs to know it needs to do a critical read,
so also storing the timestamp is probably fairly straightforward.

## Update Stream and Resharding

Problem: how to guarantee continuity of the update stream during resharding?

Assumption: when propagating statements using filtered replication, we keep
their [MasterMin, masterMax] timestamp (why wouldn’t we, really?).

Then this becomes fairly simple. Let’s take a split case, one shard to two
shards. At first, we have one update stream for the source shard. We start two
update streams, one for each destination shard. Since the source and destination
update stream share the same timestamps stream, it’s easy to match the two
streams, and take over for half of them.

TODO: I’m thinking of having easier ways for the invalidation process to be
reliable. If the resharding case could just be a tuple: (kill the old one, start
the new ones) and it just magically works, as a regular restart of the
invalidation process would work, then we’re good. I’m thinking it’s possible if
we use MasterTrueTime instead of GTID.

Problem: vttablet on a destination replica can know its replication lag through
filtered replication. For a split case, it works, as the destination replica has
only one incoming stream of data, that’s incrementing. For a merge case, the
destination has multiple incoming streams. So it will have multiple time stamp
series, that sometimes will not be synchronized. If filtered replication inserts
its stream UID with each statement, vttablet could keep a map of stream UID to
replication lag. Then it would report the minimum of the two.

## Decorrelating Invalidator from Shards

Having one invalidator per shard is annoying. The invalidators need to know the
topology, and be kept in sync.

Instead, let’s explore having the invalidator be on a key range. It then asks a
vtgate for invalidation stream on that key range. Vtgate watches the
SrvKeyspace, so it knows which shards map to a key range. It then asks multiple
shards if needed for their invalidation stream, and merges them. When
SrvKeyspace changes, vtgate can close the streams it doesn’t need any more and
open new ones, ‘hoping’ over.

For each invalidation event, vtgate can provide two timestamps: the timestamp of
the event, and the timestamp to use to resume a stream without loosing events
(which would be the minimum of the timestamps of all streams handled by the
connection). With a one to one mapping between invalidator and shard, these
would be the same values.

The number of invalidators is then unrelated to the number of shards, and can be
tuned independently. It is expected one invalidator can handle more load than
one shard, most likely. But it could also handle less than one shard if
necessary.

More generally, this would be great for any update stream product as well, not
just the invalidator.

## Snapshot and Update Stream

To maintain a copy of the data in a different data store, and keep it up to
date, we usually resort to a full snapshot, followed by listening to an update
stream. We only used to have the GTID stream for a shard, to match the snapshot
time and the time of the update stream (and that wasn’t very good, as it depends
on sharding topology, and GTIDs don’t go across shard boundaries during
resharding). We can now also use a timestamp, and it’s consistent across shards.

The full snapshot side can be taken care of by a map-reduce dumper job. We can
ask for the vttablet’s timestamp when dumping the data, and remember the
earliest replication time the snapshot data was taken at. Then we can use that
timestamp as the starting point of an update stream.

That update stream client would subscribe to the updates (by key range as
needed, see previous section). For each update, it can re-query Vitess for
updated values. The A/B choice we had before here is more complicated, as we
don’t have the option to ignore data that is too old. So we’d have to either:

* A/ wait for the data that is at least dated from that timestamp.
* B/ send the timestamp to vtgate, and vtgate finds the appropriate vttablet.
* C/ with each data update, we could send up the server id it came from. Then when querying vtgate, we’d ask that server. (I don’t like this one).

## Concrete Change List

This is a rough estimate of the changes required:

* Full design doc and formal review.
* Experiments to figure out timing on finding the starting point for replication
  given a timestamp.
* Add a Timestamp() interface to our code. Uses TrueTime inside Google, and
  regular time with fudge factor otherwise, or a mocked time for tests.
* Annotate all DMLs with the timestamp.
* Add a vttablet go routine that reads the local binlogs and populates the
  current ReplicationTime (for serving replicas only, replica and rdonly).
* Add a health check module that uses the current ReplicationTime to return
  ‘seconds behind master’.
* Add an option to vttablet’s Execute RPC call to also return
  ReplicationTime. Also add it to all vtgate Execute (that computes the min/max
  of all).
* (big chunck) Implement vtgate’s UpdateStream(keyspace, keyrange) call. Gets
  SrvKeyspace, find all the shards it needs to talk to, keep an updated list of
  shards to query from, keeps streaming queries up to each shard, aggregates the
  results and streams back.
* Add an end-to-end example with full invalidation.
