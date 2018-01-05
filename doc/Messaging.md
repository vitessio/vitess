# Vitess Messaging

## Overview

Vitess messaging gives the application an easy way to schedule and manage work
that needs to be performed asynchronously. Under the covers, messages are
stored in a traditional MySQL table and therefore enjoy the following
properties:

* **Scalable**: Because of vitess's sharding abilities, messages can scale to
  very large QPS or sizes.
* **Guaranteed delivery**: A message will be indefinitely retried until a
  successful ack is received.
* **Non-blocking**: If the sending is backlogged, new messages continue to be
  accepted for eventual delivery.
* **Adaptive**: Messages that fail delivery are backed off exponentially.
* **Analytics**: The retention period for messages is dictated by the
  application. One could potentially choose to never delete any messages and
  use the data for performing analytics.
* **Transactional**: Messages can be created or acked as part of an existing
  transaction. The action will complete only if the commit succeeds.

The properties of a message are chosen by the application. However, every
message needs a uniquely identifiable key. If the messages are stored in a
sharded table, the key must also be the primary vindex of the table.

Although messages will generally be delivered in the order they're created,
this is not an explicit guarantee of the system. The focus is more on keeping
track of the work that needs to be done and ensuring that it was performed.
Messages are good for:

* Handing off work to another system.
* Recording potentially time-consuming work that needs to be done
  asynchronously.
* Scheduling for future delivery.
* Accumulating work that could be done during off-peak hours.

Messages are not a good fit for the following use cases:

* Broadcasting of events to multiple subscribers.
* Ordered delivery.
* Real-time delivery.

## Creating a message table

The current implementation requires a fixed schema. This will be made more
flexible in the future. There will also be a custom DDL syntax. For now, a
message table must be created like this:

```
create table my_message(
  time_scheduled bigint,
  id bigint,
  time_next bigint,
  epoch bigint,
  time_created bigint,
  time_acked bigint,
  message varchar(128),
  primary key(time_scheduled, id),
  unique index id_idx(id),
  index next_idx(time_next, epoch)
) comment 'vitess_message,vt_ack_wait=30,vt_purge_after=86400,vt_batch_size=10,vt_cache_size=10000,vt_poller_interval=30'
```

The application-related columns are as follows:

* `id`: can be any type. Must be unique.
* `message`: can be any type.
* `time_scheduled`: must be a bigint. It will be used to store unix time in
  nanoseconds. If unspecified, the `Now` value is inserted.

The above indexes are recommended for optimum performance. However, some
variation can be allowed to achieve different performance trade-offs.

The comment section specifies additional configuration parameters. The fields
are as follows:

* `vitess_message`: Indicates that this is a message table.
* `vt_ack_wait=30`: Wait for 30s for the first message ack. If one is not
  received, resend.
* `vt_purge_after=86400`: Purge acked messages that are older than 86400
  seconds (1 day).
* `vt_batch_size=10`: Send up to 10 messages per RPC packet.
* `vt_cache_size=10000`: Store up to 10000 messages in the cache. If the demand
  is higher, the rest of the items will have to wait for the next poller cycle.
* `vt_poller_interval=30`: Poll every 30s for messages that are due to be sent.

If any of the above fields are missing, vitess will fail to load the table. No
operation will be allowed on a table that has failed to load.

## Enqueuing messages

The application can enqueue messages using an insert statement:

```
insert into my_message(id, message) values(1, 'hello world')
```

These inserts can be part of a regular transaction. Multiple messages can be
inserted to different tables. Avoid accumulating too many big messages within a
transaction as it consumes memory on the VTTablet side. At the time of commit,
memory permitting, all messages are instantly enqueued to be sent.

Messages can also be created to be sent in the future:

```
insert into my_message(id, message, time_scheduled) values(1, 'hello world', :future_time)
```

`future_time` must be the unix time expressed in nanoseconds.

## Receiving messages

Processes can subscribe to receive messages by sending a `MessageStream`
request to VTGate. If there are multiple subscribers, the messages will be
delivered in a round-robin fashion. Note that this is not a broadcast; Each
message will be sent to at most one subscriber.

The format for messages is the same as a vitess `Result`. This means that
standard database tools that understand query results can also be message
recipients. Currently, there is no SQL format for subscribing to messages, but
one will be provided soon.

### Subsetting

It's possible that you may want to subscribe to specific shards or groups of
shards while requesting messages. This is useful for partitioning or load
balancing. The `MessageStream` API allows you to specify these constraints. The
request parameters are as follows:

* `Name`: Name of the message table.
* `Keyspace`: Keyspace where the message table is present.
* `Shard`: For unsharded keyspaces, this is usually "0". However, an empty
  shard will also work. For sharded keyspaces, a specific shard name can be
  specified.
* `KeyRange`: If the keyspace is sharded, streaming will be performed only from
  the shards that match the range. This must be an exact match.

## Acknowledging messages

A received (or processed) message can be acknowledged using the `MessageAck`
API call. This call accepts the following parameters:

* `Name`: Name of the message table.
* `Keyspace`: Keypsace where the message table is present. This field can be
  empty if the table name is unique across all keyspaces.
* `Ids`: The list of ids that need to be acked.

Once a message is successfully acked, it will never be resent.

## Exponential backoff

A message that was successfully sent will wait for the specified ack wait time.
If no ack is received by then, it will be resent. The next attempt will be 2x
the previous wait, and this delay is doubled for every attempt.

## Purging

Messages that have been successfully acked will be deleted after their age
exceeds the time period specified by `vt_purge_after`.

## Advanced usage

The `MessageAck` functionality is currently an API call and cannot be used
inside a transaction. However, you can ack messages using a regular DML. It
should look like this:

```
update my_message set time_acked = :time_acked, time_next = null where id in ::ids and time_acked is null
```

You can manually change the schedule of existing messages with a statement like
this:

```
update my_message set time_next = :time_next, epoch = :epoch where id in ::ids and time_acked is null
```

This comes in handy if a bunch of messages had chronic failures and got
postponed to the distant future. If the root cause of the problem was fixed,
the application could reschedule them to be delivered immediately. You can also
optionally change the epoch. Lower epoch values increase the priority of the
message and the back-off is less aggressive.

You can also view messages using regular `select` queries.

## Known limitations

The message feature is currently in alpha, and can be improved. Here is the
list of possible limitations/improvements:

* Flexible columns: Allow any number of application defined columns to be in
  the message table.
* No ACL check for receivers: To be added.
* Proactive scheduling: Upcoming messages can be proactively scheduled for
  timely delivery instead of waiting for the next polling cycle.
* Monitoring support: To be added.
* Dropped tables: The message engine does not currently detect dropped tables.
* Changed properties: Although the engine detects new message tables, it does
  not refresh properties of an existing table.
* A `SELECT` style syntax for subscribing to messages.
* No rate limiting.
* Usage of partitions for efficient purging.
