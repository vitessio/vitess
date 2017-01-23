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
