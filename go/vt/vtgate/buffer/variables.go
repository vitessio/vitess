package buffer

import "github.com/youtube/vitess/go/stats"

// This file contains all status variables which can be used to monitor the
// buffer.

var (
	// starts counts how often we started buffering (including dry-run bufferings).
	starts = stats.NewMultiCounters("BufferStarts", []string{"Keyspace", "ShardName"})
	// stops counts how often we triggered the stop of a buffering, including
	// dry-run bufferings.
	// See the type "stopReason" below for all possible values of "Reason".
	stops = stats.NewMultiCounters("BufferStops", []string{"Keyspace", "ShardName", "Reason"})

	// failoverDurationSumMs is the cumulative sum of all failover durations.
	// In connection with "starts" it can be used to calculate a moving average.
	failoverDurationSumMs = stats.NewMultiCounters("BufferFailoverDurationSumMs", []string{"Keyspace", "ShardName"})

	// utilizationSum is the cumulative sum of the maximum buffer utilization
	// (in percentage) during each failover.
	// Utilization = maximum number of requests buffered / buffer size.
	// In connection with "starts" it can be used to calculate a moving average.
	// TODO(mberlin): Replace this with a MultiHistogram once it's available.
	utilizationSum = stats.NewMultiCounters("BufferUtilizationSum", []string{"Keyspace", "ShardName"})
	// utilizationDryRunSum is the cumulative sum of the maximum *theoretical*
	// buffer utilization (in percentage) during each failover.
	// Utilization = maximum number of requests buffered seen / buffer size.
	// In connection with "starts" it can be used to calculate a moving average.
	// Example: Buffer size = 10. Two consecutive failovers with maximum values of
	// 15 and 5 seen requests each add up to a value of 200% (150% + 50%
	// utilization). The moving average would be 100% because there were two
	// failovers in that period.
	// TODO(mberlin): Replace this with a MultiHistogram once it's available.
	utilizationDryRunSum = stats.NewMultiCounters("BufferUtilizationDryRunSum", []string{"Keyspace", "ShardName"})

	// requestsWindowExceeded tracks for how many requests buffering stopped
	// early because the configured window was exceeded.
	requestsWindowExceeded = stats.NewMultiCounters("BufferRequestsWindowExceeded", []string{"Keyspace", "ShardName"})

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

// TODO(mberlin): Remove the gauge values below once we store them
// internally and have a /bufferz page where we can show this.
var (
	// bufferSize publishes the configured per vtgate buffer size. It can be used
	// to calculate the utilization of the buffer.
	bufferSize = stats.NewInt("BufferSize")
	// lastFailoverDurationMs tracks for how long vtgate buffered requests during
	// the last failover.
	// The value for a given shard will be reset at the next failover.
	lastFailoverDurationMs = stats.NewMultiCounters("BufferLastFailoverDurationMs", []string{"Keyspace", "ShardName"})
	// lastRequestsInFlightMax has the maximum value of buffered requests in flight
	// of the last failover.
	// The value for a given shard will be reset at the next failover.
	lastRequestsInFlightMax = stats.NewMultiCounters("BufferLastRequestsInFlightMax", []string{"Keyspace", "ShardName"})
	// lastRequestsDryRunMax has the maximum number of requests which were seen during
	// a dry-run buffering of the last failover.
	// The value for a given shard will be reset at the next failover.
	lastRequestsDryRunMax = stats.NewMultiCounters("BufferLastRequestsDryRunMax", []string{"Keyspace", "ShardName"})
)
