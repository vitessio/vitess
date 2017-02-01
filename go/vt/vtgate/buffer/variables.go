package buffer

import "github.com/youtube/vitess/go/stats"

// This file contains all status variables which can be used to monitor the
// buffer.

var (
	// requestsInFlightMax has the maximum value of buffered requests in flight
	// of the last failover.
	requestsInFlightMax = stats.NewMultiCounters("BufferRequestsInFlightMax", []string{"Keyspace", "ShardName"})
	// requestsDryRunMax has the maximum number of requests which were seen during
	// a dry-run buffering of the last failover.
	// The value for a given shard will be reset at the next failover.
	requestsDryRunMax = stats.NewMultiCounters("BufferRequestsDryRunMax", []string{"Keyspace", "ShardName"})
	// failoverDurationMs tracks for how long vtgate buffered requests during the
	// last failover.
	failoverDurationMs = stats.NewMultiCounters("BufferFailoverDurationMs", []string{"Keyspace", "ShardName"})

	// requestsWindowExceeded tracks for how many requests buffering stopped
	// early because the configured window was exceeded.
	requestsWindowExceeded = stats.NewMultiCounters("BufferRequestsWindowExceeded", []string{"Keyspace", "ShardName"})
)
