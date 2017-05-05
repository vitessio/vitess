/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Package worker contains the framework, utility methods and core
functions for long running actions. 'vtworker' binary will use these.
*/
package worker

import (
	"flag"
	"html/template"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/stats"
)

// Worker is the base interface for all long running workers.
type Worker interface {
	// State returns the current state using the internal representation.
	State() StatusWorkerState

	// StatusAsHTML returns the current worker status in HTML.
	StatusAsHTML() template.HTML

	// StatusAsText returns the current worker status in plain text.
	StatusAsText() string

	// Run is the main entry point for the worker. It will be
	// called in a go routine.  When the passed in context is canceled, Run()
	// should exit as soon as possible.
	Run(context.Context) error
}

var (
	retryDuration         = flag.Duration("retry_duration", 2*time.Hour, "Amount of time we wait before giving up on a retryable action (e.g. write to destination, waiting for healthy tablets)")
	executeFetchRetryTime = flag.Duration("executefetch_retry_time", 30*time.Second, "Amount of time we should wait before retrying ExecuteFetch calls")
	remoteActionsTimeout  = flag.Duration("remote_actions_timeout", time.Minute, "Amount of time to wait for remote actions (like replication stop, ...)")
	useV3ReshardingMode   = flag.Bool("use_v3_resharding_mode", false, "True iff the workers should use V3-style resharding, which doesn't require a preset sharding key column.")

	healthCheckTopologyRefresh = flag.Duration("worker_healthcheck_topology_refresh", 30*time.Second, "refresh interval for re-reading the topology")
	healthcheckRetryDelay      = flag.Duration("worker_healthcheck_retry_delay", 5*time.Second, "delay before retrying a failed healthcheck")
	healthCheckTimeout         = flag.Duration("worker_healthcheck_timeout", time.Minute, "the health check timeout period")

	statsState = stats.NewString("WorkerState")
	// statsRetryCount is the total number of times a query to vttablet had to be retried.
	statsRetryCount = stats.NewInt("WorkerRetryCount")
	// statsRetryCount groups the number of retries by category e.g. "TimeoutError" or "Readonly".
	statsRetryCounters = stats.NewCounters("WorkerRetryCounters")
	// statsThrottledCounters is the number of times a write has been throttled,
	// grouped by (keyspace, shard, threadID). Mainly used for testing.
	// If throttling is enabled, this should always be non-zero for all threads.
	statsThrottledCounters = stats.NewMultiCounters("WorkerThrottledCounters", []string{"Keyspace", "ShardName", "ThreadId"})
	// statsStateDurations tracks for each state how much time was spent in it. Mainly used for testing.
	statsStateDurationsNs = stats.NewCounters("WorkerStateDurations")

	// statsOnlineInsertsCounters tracks for every table how many rows were
	// inserted during the online clone (reconciliation) phase.
	statsOnlineInsertsCounters = stats.NewCounters("WorkerOnlineInsertsCounters")
	// statsOnlineUpdatesCounters tracks for every table how many rows were updated.
	statsOnlineUpdatesCounters = stats.NewCounters("WorkerOnlineUpdatesCounters")
	// statsOnlineUpdatesCounters tracks for every table how many rows were deleted.
	statsOnlineDeletesCounters = stats.NewCounters("WorkerOnlineDeletesCounters")
	// statsOnlineEqualRowsCounters tracks for every table how many rows were equal.
	statsOnlineEqualRowsCounters = stats.NewCounters("WorkerOnlineEqualRowsCounters")

	// statsOfflineInsertsCounters tracks for every table how many rows were
	// inserted during the online clone (reconciliation) phase.
	statsOfflineInsertsCounters = stats.NewCounters("WorkerOfflineInsertsCounters")
	// statsOfflineUpdatesCounters tracks for every table how many rows were updated.
	statsOfflineUpdatesCounters = stats.NewCounters("WorkerOfflineUpdatesCounters")
	// statsOfflineUpdatesCounters tracks for every table how many rows were deleted.
	statsOfflineDeletesCounters = stats.NewCounters("WorkerOfflineDeletesCounters")
	// statsOfflineEqualRowsCounters tracks for every table how many rows were equal.
	statsOfflineEqualRowsCounters = stats.NewCounters("WorkerOfflineEqualRowsCounters")

	// statsStreamingQueryRestartsCounters tracks for every tablet alias how often
	// a streaming query was succesfully established there.
	statsStreamingQueryCounters = stats.NewCounters("StreamingQueryCounters")
	// statsStreamingQueryErrorsCounters tracks for every tablet alias how often
	// a (previously successfully established) streaming query did error.
	statsStreamingQueryErrorsCounters = stats.NewCounters("StreamingQueryErrorsCounters")
	// statsStreamingQueryRestartsSameTabletCounters tracks for every tablet alias
	// how often we successfully restarted a streaming query on the first retry.
	// This kind of restart is usually necessary when our streaming query is idle
	// and MySQL aborts it after a timeout.
	statsStreamingQueryRestartsSameTabletCounters = stats.NewCounters("StreamingQueryRestartsSameTabletCounters")
	// statsStreamingQueryRestartsDifferentTablet records how many restarts were
	// successful on the 2 (or higher) retry after the initial retry to the same
	// tablet failed and we switched to a different tablet. In practice, this
	// happens when a tablet did go away due to a maintenance operation.
	statsStreamingQueryRestartsDifferentTablet = stats.NewInt("StreamingQueryRestartsDifferentTablet")
)

const (
	retryCategoryReadOnly          = "ReadOnly"
	retryCategoryTimeoutError      = "TimeoutError"
	retryCategoryConnectionError   = "ConnectionError"
	retryCategoryNoMasterAvailable = "NoMasterAvailable"
)

// resetVars resets the debug variables that are meant to provide information on a
// per-run basis. This should be called at the beginning of each worker run.
func resetVars() {
	statsState.Set("")
	statsRetryCount.Set(0)
	statsRetryCounters.Reset()

	statsOnlineInsertsCounters.Reset()
	statsOnlineUpdatesCounters.Reset()
	statsOnlineDeletesCounters.Reset()
	statsOnlineEqualRowsCounters.Reset()

	statsOfflineInsertsCounters.Reset()
	statsOfflineUpdatesCounters.Reset()
	statsOfflineDeletesCounters.Reset()
	statsOfflineEqualRowsCounters.Reset()

	statsStreamingQueryCounters.Reset()
	statsStreamingQueryErrorsCounters.Reset()
}

// checkDone returns ctx.Err() iff ctx.Done().
func checkDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}
