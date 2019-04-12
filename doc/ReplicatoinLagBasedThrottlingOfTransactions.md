# Replication Lag Based Throttling of Transactions 
Vitess supports throttling of transactions based on replication lag. When this
feature is turned on, each VTTablet master monitors the replication lag from
the replicas, and based on the observed replication lag tries to rate-limit the
received transactions to keep the replication lag under a configured limit.

The decision of whether to throttle a transaction is done in the `BEGIN`
statement rather than in the `COMMIT` statement to avoid having a transaction
perform a lot of work just to eventually be throttled and potentially 
rolled-back if the open-transaction timeout is exceeded.

If a BEGIN statement is throttled the client receives the gRPC UNAVAILABLE
error code.

The following VTTablet command line flags control the replication-lag based 
throttler:

* *enable-tx-throttler*

A boolean flag controlling whether the replication-lag-based throttling is enabled.

* *tx-throttler-config*

A text-format representation of the  [throttlerdata.Configuration](https://github.com/vitessio/vitess/blob/master/proto/throttlerdata.proto) protocol buffer 
that contains configuration options for the throttler. 
The most important fields in that message are *target_replication_lag_sec* and 
*max_replication_lag_sec* that specify the desired limits on the replication lag. See the comments in the protocol definition file for more details.
If this is not specified a [default](https://github.com/vitessio/vitess/tree/master/go/vt/vttablet/tabletserver/tabletenv/config.go) configuration will be used.

* *tx-throttler-healthcheck-cells*

A comma separated list of datacenter cells. The throttler will only monitor
the non-RDONLY replicas found in these cells for replication lag.

# Caveats and Known Issues
* The throttler keeps trying to explore the maximum rate possible while keeping
the replication lag under the desired limit; as such the desired replication 
lag limit may occasionally be slightly violated.

* Transactions are considered homogenous. There is currently no support
for specifying how `expensive` a transaction is.

