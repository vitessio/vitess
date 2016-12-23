package buffer

import "github.com/youtube/vitess/go/stats"

// This file contains all status variables which can be used to monitor the
// buffer.

var (
	// requestsInFlightMax has the maximum value of buffered requests in flight
	// of the last failover.
	requestsInFlightMax = stats.NewMultiCounters("BufferRequestsInFlightMax", []string{"Keyspace", "ShardName"})
	// failoverDurationMs tracks for how long vtgate buffered requests during the
	// last failover.
	failoverDurationMs = stats.NewMultiCounters("BufferFailoverDurationMs", []string{"Keyspace", "ShardName"})
)
