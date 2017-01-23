# Vitess Messaging

# Overview

Vitess messaging gives the application an easy way to schedule and manage work that needs to be performed asynchronously. Under the covers, messages are stored in a traditional MySQL table and therefore enjoy the following properties:

* **Transactional**: Messages can be created or acked as part of an existing transaction. The action will complete only if the commit succeeds.
* **Scalable**: Because of vitess's sharding abilities, messages can scale to very large QPS or sizes.
* **Guaranteed delivery**: A message will be indefinitely retried until a successful ack is received.
* **Non-blocking**: If the sending is backlogged, new messages continue to be accepted for eventual delivery.
* **Adaptive**: Messages that fail delivery are backed off exponentially.
* **Analytics**: The retention period for messages is dictated by the application. One could potentially choose to never delete any messages and use the data for performing analytics.

The properties of a message are chosen by the application. However, every message needs a uniquely identifiable key. If the messages are stored in a sharded table, the key must also be the primary vindex of the table.

Although messages will generally be delivered in the order they're created, this is not an explicit guarantee of the system. The focus is more on keeping track of the work that needs to be done and ensuring that it was performed. Messages are good for:

* Handing off work to another system.
* Recording potentially time-consuming work that needs to be done asynchronously.
* Scheduling for future delivery.
* Accumulating work that could be done during off-peak hours.

Messages are not a good fit for the following use cases:

* Broadcasting of events to multiple subscribers.
* Ordered delivery.
* Real-time delivery.

# Creating a message table

The current implementation requires a fixed schema. This will be made more flexible in the future. There will also be a custom DDL syntax. For now, a message table must be created like this:

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
* `time_scheduled`: must be a bigint. It will be used to store unix time in nanoseconds. If unspecified, the `Now` value is inserted.

The above indexes are recommended for optimum performance. However, some variation can be allowed to achieve different performance trade-offs.

The comment section specifies additional configuration parameters. The fields are as follows:

* `vitess_message`: Indicates that this is a message table.
* `vt_ack_wait=30`: Wait for 30s for the first message ack. If one is not received, resend.
* `vt_purge_after=86400`: Purge acked messages that are older than 86400 seconds (1 day).
* `vt_batch_size=10`: Send up to 10 messages per batch.
* `vt_cache_seze=10000`: Store up to 10000 messages in the cache.
* `vt_poller_interval=30`: Poll every 30s for messages that are due to be sent.

If any of the above fields are missing, vitess will fail to load the table. No operation will be allowed on a table that has failed to load.
