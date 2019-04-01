# Vitess Queues

This document describes a proposed design and implementation guidelines for a
Queue feature inside Vitess. Events can be added to a Queue, and processed
asynchronously by Receivers.

## Problem and Challenges

Queues are hard to implement right on MySQL. They need to scale, and usually
issues when implementing them at the application level start creeping up at high
QPS.

This document proposes a design to address the following requirements:

* Need to support sharding transparently (and scale linearly with number of
  shards).
* Need to not lose events.
* Extracting the events from the queue for processing needs to be scalable.
* Adding to the queue needs to be transaction-safe (so changing some content and
  adding to the queue can be committed as a transaction).
* Events have an optional timestamp of when they should fire. If none is
  specified, it will be as soon as possible.
* May fire events more than once, if the event receiver is unresponsive, or in
  case of topology events (resharding, reparent, ...).
* Events that are successfully acked should never fire again.
* Strict event processing ordering is not a requirement.

While it is possible to implement high QPS queues using a sharded table, it
would introduce the following limitations:

* The Queue consumers need to poll the database to find new work.
* The Queue consumers have to be somewhat shard-aware, to only consume events
  from one shard.
* With a higher number of consumers on one shard, they step on each-others toes,
  and introduce database latency.

## Proposed Design

At a high level, we propose to still use a table as backend for a Queue. It
follows the same sharding as the Keyspace it is in, meaning it is present on all
shards. However, instead of having consumers query the table directly, we
introduce the Queue Manager:

* When a tablet becomes the master of a shard (at startup, or at reparent time),
  it creates a Queue Manager routine for each Queue Table in the schema.
* The Queue Manager is responsible for managing the data in the Queue Table.
* The Queue Manager dispatches events that are firing to a pool of
  Listeners. Each Listener maintains a streaming connection to the Queue it is
  listening on.
* The Queue Manager can then use an in-memory cache covering the next time
  interval, and be efficient.
* All requests to the Queue table are then done by Primary Key, and are very
  fast.

Each Queue Table has the following mandatory fields. At first, we can create
them using a regular schema change. Eventually, we'll support a `CREATE QUEUE
name(...)` command. Each entry in the queue has a sharding key (could be a hash
of the trigger timestamp). The Primary Key for the table is a timestamp,
nanosecond granularity:

* Timestamp in nanoseconds since Epoch (PK): uint64
* State (enum): to_be_fired, fired, acked.
* Some sharding key: user-defined, specified at creation time, with vindex.
* Other columns: user-defined, specified at creation time.

A Queue is linked to a Queue Manager on the master vttablet, on each shard. The
Queue Manager is responsible for dispatching events to the Receivers. The Queue
Manager can impose the following limits:

* Maximum QPS of events being processed.
* Maximum concurrency of event processing.

*Note*: we still use the database to store the items, so any state change is
persisted. We'll explore scalability below, but the QPS for Queues will still be
limited by the insert / update QPS of each shard master. To increase Queue
capabilities, the usual horizontal sharding process can be used.

## Implementation Details

Queue Tables are marked in the schema by a comment, in a similar way we detect
Sequence Tables
[now](https://github.com/vitessio/vitess/blob/master/go/vt/tabletserver/table_info.go#L37).

When a tablet becomes a master, and there are Queue tables, it creates a
QueueManager for each of them.

The Queue Manager has a window of ‘current or soon’ events (let’s say the next
30 seconds or 1 minute). Upon startup, it reads these events into memory. Then
for every event modification that is done for events in that window, it keeps a
memory copy of the event.

Receivers use streaming RPCs to ask for events, providing a KeyRange if
necessary to only target a subset of the Queues. That RPC is connected to the
corresponding Queue Managers on the targeted shards. It is recommended for
multiple Receivers to target the same shard, for redundancy and scalability.

Whenever an event becomes current and needs to be fired, the Queue Manager
changes the state of the event on disk to ‘fired’, and sends it to a random
Receiver (or the Receiver that is the least busy, or the most responsive, which
ever it is). The Receiver will then either:

* Ack the event after processing, and then the Manager can mark it ‘acked’ in
  the table, or even delete it.
* Not respond in time, or not ack it: the Manager can mark it as ‘to be fired’
  again. In that case, the Manager can back off and try again later. The logic
  here can be as simple or complicated as necessary.

The easiest implementation is to not guarantee an execution order for the
events. Providing ordering would not be too difficult, but has an impact on
performance (when a Receiver does't respond in time, it delays eveybody else, so
we'd need to timeout quickly and keep going).

For ‘old’ events (that have been fired but not acked), we propose to use an old
event collector that retries with some kind of longer threshold / retry
period. That way we don't use the Queue Manager for old events, and keep it
firing recent events.

Event history can be managed in multiple ways. The Queue Manager should probably
have some kind of retention policy, and delete events that have been fired and
older than N days. Deleting the table would delete the entire queue.

## Example / Code

We create the Queue:

``` sql
CREATE QUEUE my_queue(
  user_id BIGINT(10),
  payload VARCHAR(256),
)

```

Then we change VSchema to have a hash vindex for user_id.

Let's insert something into the queue:

``` sql
BEGIN
INSERT INTO my_queue(user_id, payload) VALUES (10, ‘I want to send this’);
COMMIT
```

We add another streaming endpoint to vtgate for queue event consumption, and one
for acking:

``` protocol-buffer
message QueueReceiveRequest {
  // caller_id identifies the caller. This is the effective caller ID,
  // set by the application to further identify the caller.
  vtrpc.CallerID caller_id = 1;

  // keyspace to target the query to.
  string keyspace = 2;

  // shard to target the query to, for unsharded keyspaces.
  string shard = 3;

  // KeyRange to target the query to, for sharded keyspaces.
  topodata.KeyRange key_range = 4;
}

message QueueReceiveResponse {
  // First response has fields, next ones have data.
  // Note we might get multiple notifications in a single packet.
  QueryResult result = 1;
}

message QueueAckRequest {
  // TODO: find a good way to reference the event, maybe
  // have an ‘id’ in the query result.
  Repeated Id string;
}

message QueueAckResponse {
}

service Vitess {
  ...
  rpc QueueReceive(vtgate.QueueReceiveRequest) returns (stream vtgate.QueueReceiveResponse) {};

  Rpc QueueAck(vtgate.QueueAckRequest) returns (vtgate.QueueAckResponse) {};
  ...
}

```

Then the receiver code is just an endless loop:

* Call QueueReceive(). For each QueueReceiveResponse:
  * For each row:
    * Do what needs to be done.
    * Call QueueAck() for the processed events.

*Note*: acks may need to be inside a transaction too. In that case, we may want
to remove the QueueAck, and replace it with an: `UPDATE my_queue(state)
VALUES(‘acked’) WHERE <PK>=...;` Statement. It might be re-written by vttablet.

## Scalability

How does this scale? The answer should be: very well. There is no expensive
query done to the database. Only a range query (might need locking) every N
seconds. Everything else is point updates.

This approach scales with the number of shards too. Resharding will multiply the
queue processing power.

Since we have a process on the master, we piggy-back on our regular tablet
master election.

*Event creation*: each creation is one INSERT into the database. Creation of
events that are supposed to execute as soon as possible may create a hotspot in
the table. If so, we might be better off using the keyspace id for primary key,
and having an index on timestamps. If the event creation is in the execution
window, we also keep it in memory. Multiple events can even be created in the
same transaction. *Optimization*: if the event is supposed to be fired right
away, we might even create it as ‘fired’ directly.

*Event dispatching*: mostly done by the Queue Manager. Each dispatch is one
update (by PK) for firing, and one update (by PK again) for acking. The RPC
overhead for sending these events and receiving the acks from the receivers is
negligible (as it has no DB impact).

*Queue Manager*: it does one scan of the upcoming events for the next N seconds,
every N seconds. Hopefully that query is fast. Note we could add a ‘LIMIT’ to
that query and only consider the next N seconds, or the period that has 10k
events (value TBD), whichever is smaller.

*Event Processing*: a pool of Receivers is consuming the events. Scale that up
to as many as is needed, with any desired redundancy. The shard master will send
the event to one receiver only, until it is acked, or times out.

*Hot Spots in the Queue table*: with very high QPS, we will end up having a hot
spot on the window of events that are coming up. A proposed solution is to use
an 'event_name' that has a unique value, and use it as a primary key. When the
Queue Manager gets new events however, it will still need to sort them by
timestamp, and therefore will require an index. It is unclear which solution
will work better, we'd need to experiment. For instance, if most events are only
present in the table while they are being processed, the entire table will have
high QPS anyway.

## Caveats

This is not meant to replace UpdateStream. UpdateStream is meant to send one
notification for each database event, without extra database cost, and without
any filtering. Queues have a database cost.

Events can be fired multiple times, in corner cases. However, we always do a
database access before firing the event. So in case of reparent / resharding,
that database access should fail. So we should stop dispatching events as soon
as the master is read-only. Also, if we receive an ack when the database is
read-only, we can’t store it. With the Master Buffering feature, vtgate may be
able to retry later on the new master.


## Extensions

The following extensions can be implemented, but won't be at first:

* *Queue Ordering*: only fire one event at a time, in order.

* *Event Groups*: groups events together using a common key, and when they're
  all done, fire another event.
