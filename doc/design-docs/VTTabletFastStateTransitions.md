# Fast vttablet state transitions

This issue is in response to #6645. When vttablet transitions from primary to non-primary, the following problems can occur under different circumstances:

* When a query is killed, it may take a long time for mysql to return the error if it has to do a lot of cleanup. This can delay a vttablet transition. Just closing the pools is not enough because the tablet shutdown also waits for all executing goroutines to return.
* It is possible that the query timeout is much greater than the transaction timeout. In such cases, the query timeout must be reduced to match the transaction timeout. Otherwise, a running query can hold a transaction hostage and prevent a vttablet from transitioning.
* The `transaction_shutdown_grace_period` must acquire a new meaning. It should be renamed to `shutdown_grace_period`, and must also apply to queries that are exceeding this time limit. This limit applies to all queries: streaming, oltp read, reserved, and in_transaction.
* The transaction shutdown code "waits for empty", but reserved connections (not in a transaction) are now part of this pool and will prevent the pool from going empty until they timeout. We need to close them more proactively during a shutdown.
* The transition from primary to non-primary uses the immediate flag. This should wait for transactions to complete. Fortunately, due to how PRS works, that code path is not used. We instead use the "dont_serve" code path. But this needs to be fixed for future-proofing.

Many approaches were discussed in #6645. Those approaches are all non-viable because they don't address all of the above concerns.

To fix all these problems, some refactoring will need to be done. Here's the proposal:

* The query killer (DBConn.Kill) will be changed to proactively close the connection. This will cause the execution to return immediately, thereby addressing the problem where slow kills delay a shutdown.
* Change the query execution to use the minimum of the transaction timeout and query timeout, but only if the request is part of a transaction.
* Build a list of all currently active queries by extending StreamQueryList. During a shutdown, the state manager will use this list to kill all active queries if shutdown_grace_period is hit. This, along with the Kill change, will cause all those executes to immediately return and the connections will be returned to their respective pools.
* tx_engine/tx_pool: Will now have two modes during shutdown:
  * "kill_reserved" will cause all reserved connections to be closed. If a reserved connection is returned to the pool during this state, it will also be closed. This will be the initial shutdown state until the shutdown grace period is hit.
  * "kill_all" will cause all reserved and transaction connections to be closed. We enter this state after the shutdown grace period is hit. While in this state, the state manager would have killed all currently executing queries. As these connections are returned to the pool, we just close them. This will cause the tx pool to close in a timely manner.
* Fix transition to not be immediate (dead code).

During a previous code review, I also found a (rare) race condition in tx_engine, which I forgot to document. I'll redo the analysis and fix the race if it's trivial.

The most important guarantee of this change is that a shutdown will not take longer than the shutdown_grace_period if it was specified.
