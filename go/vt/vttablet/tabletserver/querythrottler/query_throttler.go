/*
Copyright 2025 The Vitess Authors.

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

package querythrottler

import (
	"context"
	"strconv"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

const (
	queryThrottlerAppName = "QueryThrottler"
	// defaultPriority is the default priority value when none is specified
	defaultPriority = 100 // sqlparser.MaxPriorityValue
)

type Stats struct {
	requestsTotal     *stats.CountersWithMultiLabels
	requestsThrottled *stats.CountersWithMultiLabels
	totalLatency      *servenv.MultiTimingsWrapper
	evaluateLatency   *servenv.MultiTimingsWrapper
}

type QueryThrottler struct {
	ctx            context.Context
	throttleClient *throttle.Client
	tabletConfig   *tabletenv.TabletConfig
	mu             sync.RWMutex
	// cfg holds the current configuration for the throttler.
	cfg Config
	// cfgLoader is responsible for loading the configuration.
	cfgLoader ConfigLoader
	// strategy is the current throttling strategy handler.
	strategy registry.ThrottlingStrategyHandler
	env      tabletenv.Env
	stats    Stats
}

// NewQueryThrottler creates a new  query throttler.
func NewQueryThrottler(ctx context.Context, throttler *throttle.Throttler, cfgLoader ConfigLoader, env tabletenv.Env) *QueryThrottler {
	client := throttle.NewBackgroundClient(throttler, throttlerapp.QueryThrottlerName, base.UndefinedScope)

	qt := &QueryThrottler{
		ctx:            ctx,
		throttleClient: client,
		tabletConfig:   env.Config(),
		cfg:            Config{},
		cfgLoader:      cfgLoader,
		strategy:       &registry.NoOpStrategy{}, // default strategy until config is loaded
		env:            env,
		stats: Stats{
			requestsTotal:     env.Exporter().NewCountersWithMultiLabels(queryThrottlerAppName+"Requests", "query throttler requests", []string{"Strategy", "Workload", "Priority"}),
			requestsThrottled: env.Exporter().NewCountersWithMultiLabels(queryThrottlerAppName+"Throttled", "query throttler requests throttled", []string{"Strategy", "Workload", "Priority", "MetricName", "MetricValue", "DryRun"}),
			totalLatency:      env.Exporter().NewMultiTimings(queryThrottlerAppName+"TotalLatencyNs", "Total time each request takes in query throttling including evaluation, metric checks, and other overhead (nanoseconds)", []string{"Strategy", "Workload", "Priority"}),
			evaluateLatency:   env.Exporter().NewMultiTimings(queryThrottlerAppName+"EvaluateLatencyNs", "Time each request takes to make the throttling decision (nanoseconds)", []string{"Strategy", "Workload", "Priority"}),
		},
	}

	// Start the initial strategy
	qt.strategy.Start()

	// starting the loop which will be responsible for refreshing the config.
	qt.startConfigRefreshLoop()

	return qt
}

// Shutdown gracefully stops the throttler and cleans up resources.
// This should be called when the QueryThrottler is no longer needed.
func (qt *QueryThrottler) Shutdown() {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	// Stop the current strategy to clean up any background processes
	if qt.strategy != nil {
		qt.strategy.Stop()
	}
}

// Throttle checks if the tablet is under heavy load
// and enforces throttling by rejecting the incoming request if necessary.
// Note: This method performs lock-free reads of config and strategy for optimal performance.
// Config updates are rare (default: every 1 minute) compared to query frequency,
// so the tiny risk of reading slightly stale data during config updates is acceptable
// for the significant performance improvement of avoiding mutex contention.
func (qt *QueryThrottler) Throttle(ctx context.Context, tabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, transactionID int64, options *querypb.ExecuteOptions) error {
	// Lock-free read: for maximum performance in the hot path as cfg and strategy are updated rarely (default once per minute).
	// They are word-sized and safe for atomic reads; stale data for one query is acceptable and avoids mutex contention in the hot path.
	tCfg := qt.cfg
	tStrategy := qt.strategy

	if !tCfg.Enabled {
		return nil
	}

	// Capture start time for latency measurements only when throttling is enabled
	startTime := time.Now()

	// Extract query attributes once to avoid re computation in strategies
	attrs := registry.QueryAttributes{
		WorkloadName: extractWorkloadName(options),
		Priority:     extractPriority(options),
	}
	strategyName := tStrategy.GetStrategyName()
	workload := attrs.WorkloadName
	priorityStr := strconv.Itoa(attrs.Priority)

	// Defer total latency recording to ensure it's always emitted regardless of return path.
	defer func() {
		qt.stats.totalLatency.Record([]string{strategyName, workload, priorityStr}, startTime)
	}()

	// Evaluate the throttling decision
	decision := qt.strategy.Evaluate(ctx, tabletType, parsedQuery, transactionID, attrs)

	// Record evaluate-window latency immediately after Evaluate returns
	qt.stats.evaluateLatency.Record([]string{strategyName, workload, priorityStr}, startTime)

	qt.stats.requestsTotal.Add([]string{strategyName, workload, priorityStr}, 1)

	// If no throttling is needed, allow the query
	if !decision.Throttle {
		return nil
	}

	// Emit metric of query being throttled.
	qt.stats.requestsThrottled.Add([]string{strategyName, workload, priorityStr, decision.MetricName, strconv.FormatFloat(decision.MetricValue, 'f', -1, 64), strconv.FormatBool(tCfg.DryRun)}, 1)

	// If dry-run mode is enabled, log the decision but don't throttle
	if tCfg.DryRun {
		log.Warningf("[DRY-RUN] %s, metric name: %s, metric value: %f", decision.Message, decision.MetricName, decision.MetricValue)
		return nil
	}

	// Normal throttling: return an error to reject the query
	return vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, decision.Message)
}

// extractWorkloadName extracts the workload name from ExecuteOptions.
// If no workload name is provided, returns a default value.
func extractWorkloadName(options *querypb.ExecuteOptions) string {
	if options == nil {
		return "unknown"
	}

	if options.WorkloadName != "" {
		return options.WorkloadName
	}

	return "default"
}

// extractPriority extracts the priority from ExecuteOptions.
// Priority is stored as a string but represents an integer value (0-100).
// If no priority is provided, returns the default priority.
func extractPriority(options *querypb.ExecuteOptions) int {
	if options == nil {
		return defaultPriority
	}

	if options.Priority == "" {
		return defaultPriority
	}

	optionsPriority, err := strconv.Atoi(options.Priority)
	// This should never error out, as the value for Priority has been validated in the vtgate already.
	// Still, handle it just to make sure.
	if err != nil || optionsPriority < 0 || optionsPriority > 100 {
		log.Warningf("Invalid priority value '%s' in ExecuteOptions, expected integer 0-100, using default priority %d", options.Priority, defaultPriority)
		return defaultPriority
	}

	return optionsPriority
}

// selectThrottlingStrategy returns the appropriate strategy implementation based on the config.
func selectThrottlingStrategy(cfg Config, client *throttle.Client, tabletConfig *tabletenv.TabletConfig) registry.ThrottlingStrategyHandler {
	deps := registry.Deps{
		ThrottleClient: client,
		TabletConfig:   tabletConfig,
	}
	return registry.CreateStrategy(cfg, deps)
}

// startConfigRefreshLoop launches a background goroutine that refreshes the throttler's configuration
// at the interval specified by QueryThrottlerConfigRefreshInterval.
func (qt *QueryThrottler) startConfigRefreshLoop() {
	go func() {
		refreshInterval := qt.tabletConfig.QueryThrottlerConfigRefreshInterval
		configRefreshTicker := time.NewTicker(refreshInterval)
		defer configRefreshTicker.Stop()

		for {
			select {
			case <-qt.ctx.Done():
				return
			case <-configRefreshTicker.C:
				newCfg, err := qt.cfgLoader.Load(qt.ctx)
				if err != nil {
					continue
				}

				// Only restart strategy if the strategy type has changed
				if qt.cfg.Strategy != newCfg.Strategy {
					// Stop the current strategy before switching to a new one
					if qt.strategy != nil {
						qt.strategy.Stop()
					}

					newStrategy := selectThrottlingStrategy(newCfg, qt.throttleClient, qt.tabletConfig)
					// Update strategy and start the new one
					qt.mu.Lock()
					qt.strategy = newStrategy
					qt.mu.Unlock()
					if qt.strategy != nil {
						qt.strategy.Start()
					}
				}

				// Always update the configuration
				qt.mu.Lock()
				qt.cfg = newCfg
				qt.mu.Unlock()
			}
		}
	}()
}
