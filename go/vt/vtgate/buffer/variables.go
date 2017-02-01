package buffer

import "github.com/youtube/vitess/go/stats"

// This file contains all status variables which can be used to monitor the
// buffer.

var (
	// bufferSize publishes the configured per vtgate buffer size. It can be used
	// to calculate the utilization of the buffer.
	bufferSize = stats.NewInt("BufferSize")
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

	// starts counts how often we started buffering (including dry-run bufferings).
	starts = stats.NewMultiCounters("BufferStarts", []string{"Keyspace", "ShardName"})
	// stops counts how often we triggered the stop of a buffering, including
	// dry-run bufferings.
	// See the type "stopReason" below for all possible values of "Reason".
	stops = stats.NewMultiCounters("BufferStops", []string{"Keyspace", "ShardName", "Reason"})

	// startsSkipped tracks *how many requests* would have started buffering but
	// eventually did not (includes dry-run bufferings).
	// See the type "startSkippedReason" below for all possible values of "Reason".
	startsSkipped = stats.NewMultiCounters("BufferStartsSkipped", []string{"Keyspace", "ShardName", "Reason"})
)

// stopReason is used in "stopsByReason" as "Reason" label.
type stopReason string

const (
	stopReasonFailoverEndDetected         stopReason = "NewMasterSeen"
	stopReasonMaxFailoverDurationExceeded            = "MaxDurationExceeded"
)

// startSkippedReason is used in "startsSkippedByReason" as "Reason" label.
type startSkippedReason string

const (
	startSkippedLastReparentTooRecent startSkippedReason = "LastReparentTooRecent"
	startSkippedLastFailoverTooRecent                    = "LastFailoverTooRecent"
)
