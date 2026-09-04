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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	// Import strategy packages for side-effect registration via init()
	_ "vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/strategy/tablethrottler"
)

const (
	queryThrottlerAppName = "QueryThrottler"
	// defaultPriority is the default priority value when none is specified
	defaultPriority = 100 // sqlparser.MaxPriorityValue
	// unknownWorkload is the Workload label value used when the client supplies no workload,
	// and when per-workload metrics are off. See initThrottlerMetrics.
	unknownWorkload = "unknown"

	// throttledLogInterval bounds how often hot-path log lines (e.g. the dry-run decision)
	// are emitted, so a high query rate cannot spam the logs. Counters carry the volume.
	throttledLogInterval = 5 * time.Second
)

var (
	metricsInitOnce   sync.Once
	requestsTotal     *stats.CountersWithMultiLabels
	requestsThrottled *stats.CountersWithMultiLabels
	totalLatency      *stats.MultiTimings
	evaluateLatency   *stats.MultiTimings
)

// initThrottlerMetrics registers the query throttler stats once per process.
//
// The schema always carries a Workload label, whatever each instance set
// EnablePerWorkloadTableMetrics to. Only the label's *value* varies per instance
// (see buildLabels); the count never does. A mismatched count would panic on the hot path.
func initThrottlerMetrics() {
	metricsInitOnce.Do(func() {
		baseLabels := []string{"Strategy", "Workload", "Priority"}
		throttledLabels := []string{"Strategy", "Workload", "Priority", "MetricName", "DryRun"}
		requestsTotal = stats.NewCountersWithMultiLabels(queryThrottlerAppName+"Requests", "query throttler requests", baseLabels)
		requestsThrottled = stats.NewCountersWithMultiLabels(queryThrottlerAppName+"Throttled", "query throttler requests throttled", throttledLabels)
		totalLatency = stats.NewMultiTimings(queryThrottlerAppName+"TotalLatencyNs", "Total time each request takes in query throttling including evaluation, metric checks, and other overhead (nanoseconds)", baseLabels)
		evaluateLatency = stats.NewMultiTimings(queryThrottlerAppName+"EvaluateLatencyNs", "Time each request takes to make the throttling decision (nanoseconds)", baseLabels)
	})
}

// stateSnapshot is the immutable {cfg, strategy} pair Throttle() loads atomically.
// HandleConfigUpdate swaps the whole snapshot, so Throttle() never sees a torn pair.
type stateSnapshot struct {
	cfg      *querythrottlerpb.Config
	strategy registry.ThrottlingStrategyHandler
}

type QueryThrottler struct {
	ctx                context.Context
	cancelWatchContext context.CancelFunc

	throttlerClient *throttle.Client
	tabletConfig    *tabletenv.TabletConfig

	keyspace      string
	cell          string
	srvTopoServer srvtopo.Server

	// mu serializes config updates against Shutdown and each other. Throttle() does not
	// take it; it loads the snapshot atomically instead.
	mu           sync.Mutex
	watchStarted atomic.Bool

	// shutdown, guarded by mu, latches true on the first Shutdown(). HandleConfigUpdate
	// builds its strategy outside the lock, so it must re-check this before Start()'ing —
	// otherwise it leaks the strategy's ticker and watch goroutines past shutdown.
	shutdown bool

	// snapshot holds the current {cfg, strategy} pair. Non-nil after NewQueryThrottler.
	snapshot atomic.Pointer[stateSnapshot]

	// perWorkloadMetrics gates the Workload label's value, never the label count.
	// Read once at construction from env.Config().EnablePerWorkloadTableMetrics.
	perWorkloadMetrics bool

	// newStrategyFactory builds a strategy from a config. Tests may swap it; when nil,
	// buildNewStrategy falls back to the production wiring.
	newStrategyFactory func(*querythrottlerpb.Config) registry.ThrottlingStrategyHandler

	// throttledLogger rate-limits hot-path log lines. Never nil after NewQueryThrottler.
	throttledLogger *logutil.ThrottledLogger

	env tabletenv.Env
}

// NewQueryThrottler creates a new  query throttler.
func NewQueryThrottler(ctx context.Context, throttler *throttle.Throttler, env tabletenv.Env, alias *topodatapb.TabletAlias, srvTopoServer srvtopo.Server) *QueryThrottler {
	client := throttle.NewBackgroundClient(throttler, throttlerapp.QueryThrottlerName, base.UndefinedScope)

	perWorkloadMetrics := env.Config().EnablePerWorkloadTableMetrics
	initThrottlerMetrics()

	qt := &QueryThrottler{
		ctx:                ctx,
		throttlerClient:    client,
		tabletConfig:       env.Config(),
		cell:               alias.GetCell(),
		srvTopoServer:      srvTopoServer,
		env:                env,
		perWorkloadMetrics: perWorkloadMetrics,
		throttledLogger:    logutil.NewThrottledLogger("QueryThrottler", throttledLogInterval),
	}
	qt.newStrategyFactory = func(cfg *querythrottlerpb.Config) registry.ThrottlingStrategyHandler {
		return selectThrottlingStrategy(cfg, qt.throttlerClient, qt.tabletConfig, qt.env, qt.keyspace, qt.cell, qt.srvTopoServer)
	}

	// Initialize snapshot with empty config and default NoOp strategy so Throttle()
	// always sees a non-nil snapshot until the first config update lands.
	initial := &stateSnapshot{
		cfg:      &querythrottlerpb.Config{},
		strategy: &registry.NoOpStrategy{},
	}
	qt.snapshot.Store(initial)

	// Start the initial strategy
	initial.strategy.Start()

	return qt
}

// Shutdown gracefully stops the throttler and cleans up resources.
// This should be called when the QueryThrottler is no longer needed.
func (qt *QueryThrottler) Shutdown() {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	// Latch first, so a HandleConfigUpdate callback blocked on qt.mu sees it the moment
	// it gets the lock and discards the strategy it built rather than starting it.
	qt.shutdown = true

	// Cancel the watch context to stop the watch goroutine
	if qt.cancelWatchContext != nil {
		qt.cancelWatchContext()
	}

	// Reset the watch started flag to allow restarting the watch if needed
	qt.watchStarted.Store(false)

	// Stop the current strategy to clean up any background processes
	if snap := qt.snapshot.Load(); snap != nil && snap.strategy != nil {
		snap.strategy.Stop()
	}
}

// IsShutdown reports whether Shutdown has been called. Used to verify lifecycle wiring.
func (qt *QueryThrottler) IsShutdown() bool {
	qt.mu.Lock()
	defer qt.mu.Unlock()
	return qt.shutdown
}

// InitDBConfig sets the keyspace and starts the config watch. Called once by
// TabletServer.InitDBConfig on tablet startup. Until it runs, the throttler is on the
// NoOp strategy, so a restarting tablet would otherwise serve unthrottled.
func (qt *QueryThrottler) InitDBConfig(keyspace string) {
	qt.keyspace = keyspace
	log.Info("QueryThrottler: initialized with keyspace=" + keyspace)

	// Start the topo server watch post the keyspace is set.
	qt.startSrvKeyspaceWatch()
}

// Throttle rejects the incoming request if the tablet is under heavy load.
// The hot path takes no lock: one atomic snapshot load gives a consistent (cfg, strategy).
func (qt *QueryThrottler) Throttle(ctx context.Context, tabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, statementType sqlparser.StatementType, transactionID int64, options *querypb.ExecuteOptions) error {
	// Single atomic load gives a consistent (cfg, strategy) pair for this call.
	snap := qt.snapshot.Load()
	tCfg := snap.cfg
	tStrategy := snap.strategy

	if !tCfg.GetEnabled() {
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
	priorityStr := strconv.Itoa(attrs.Priority)
	labels := qt.buildLabels(strategyName, attrs.WorkloadName, priorityStr)

	// Defer total latency recording to ensure it's always emitted regardless of return path.
	defer func() {
		totalLatency.Record(labels, startTime)
	}()

	// Evaluate the throttling decision
	decision := tStrategy.Evaluate(ctx, tabletType, parsedQuery, statementType, transactionID, attrs)

	// Record evaluate-window latency immediately after Evaluate returns
	evaluateLatency.Record(labels, startTime)

	requestsTotal.Add(labels, 1)

	// If no throttling is needed, allow the query
	if !decision.Throttle {
		return nil
	}

	// Emit metric of query being throttled.
	requestsThrottled.Add(qt.buildLabels(strategyName, attrs.WorkloadName, priorityStr, decision.MetricName, strconv.FormatBool(tCfg.GetDryRun())), 1)

	// If dry-run mode is enabled, log the decision but don't throttle.
	if tCfg.GetDryRun() {
		// Rate-limited: with a 100% rule this path fires per query; the requestsThrottled
		// counter (incremented above) carries the volume.
		qt.throttledLogger.Warningf("[DRY-RUN] %s, metric name: %s, metric value: %f", decision.Message, decision.MetricName, decision.MetricValue)
		return nil
	}

	// Normal throttling: return an error to reject the query.
	// Prefix the stable marker so sqlerror.demuxResourceExhaustedErrors maps this to
	// ER_OUT_OF_RESOURCES (1041) instead of the default ER_TOO_MANY_USER_CONNECTIONS (1203).
	return vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, sqlerror.QueryThrottledMarker+" "+decision.Message)
}

// startSrvKeyspaceWatch loads the config once up front, then watches SrvKeyspace for
// updates. The upfront load is best-effort — a failure is only logged, since the watch
// delivers the current value on establishment and retries transient errors anyway.
// The watchStarted flag ensures at most one watch goroutine ever runs.
func (qt *QueryThrottler) startSrvKeyspaceWatch() {
	// Pre-flight validation: ensure required fields are set
	if qt.srvTopoServer == nil || qt.keyspace == "" {
		log.Error(fmt.Sprintf("QueryThrottler: cannot start SrvKeyspace watch, srvTopoServer=%v, keyspace=%s", qt.srvTopoServer != nil, qt.keyspace))
		return
	}

	srvKS, err := qt.srvTopoServer.GetSrvKeyspace(qt.ctx, qt.cell, qt.keyspace)
	if err != nil {
		log.Warn(fmt.Sprintf("QueryThrottler: failed to load initial config for keyspace=%s (GetSrvKeyspace): %v", qt.keyspace, err))
	}
	if srvKS == nil {
		log.Warn(fmt.Sprintf("QueryThrottler: srv keyspace fetched is nil for keyspace=%s ", qt.keyspace))
	}
	qt.HandleConfigUpdate(srvKS, nil)

	// Start the watch even if the load above failed, so we recover once config appears.
	if !qt.watchStarted.CompareAndSwap(false, true) {
		log.Info("QueryThrottler: SrvKeyspace watch already started for keyspace=" + qt.keyspace)
		return
	}
	watchCtx, cancel := context.WithCancel(qt.ctx)
	// Publish the cancel func and read the shutdown latch in one critical section:
	// a listener registered after Shutdown leaks, since only a later notification drops it.
	qt.mu.Lock()
	alreadyShutdown := qt.shutdown
	qt.cancelWatchContext = cancel
	qt.mu.Unlock()

	if alreadyShutdown {
		cancel() // nothing registered; release the derived context
		log.Info("QueryThrottler: not starting SrvKeyspace watch, already shut down for keyspace=" + qt.keyspace)
		return
	}

	go func() {
		// Delivers the current value immediately (deduped against the load above), then
		// streams updates. The resilient watcher retries transient errors itself.
		qt.srvTopoServer.WatchSrvKeyspace(watchCtx, qt.cell, qt.keyspace, qt.srvKeyspaceListener(watchCtx))
	}()

	log.Info(fmt.Sprintf("QueryThrottler: started event-driven watch for SrvKeyspace keyspace=%s cell=%s", qt.keyspace, qt.cell))
}

// srvKeyspaceListener returns the callback for the resilient SrvKeyspace watcher.
// Returning false is the only way to deregister — srvtopo re-appends any listener that
// returned true, so cancelling watchCtx alone is not enough. The drop is lazy: it takes
// effect on the next notification.
func (qt *QueryThrottler) srvKeyspaceListener(watchCtx context.Context) func(*topodatapb.SrvKeyspace, error) bool {
	return func(srvks *topodatapb.SrvKeyspace, err error) bool {
		if watchCtx.Err() != nil {
			return false
		}
		return qt.HandleConfigUpdate(srvks, err)
	}
}

// buildLabels returns {Strategy, Workload, Priority} plus any extras. Workload collapses to
// unknownWorkload unless per-workload metrics are on, since the client-supplied WORKLOAD_NAME
// is unbounded and would blow up label cardinality.
func (qt *QueryThrottler) buildLabels(strategyName, workload, priorityStr string, extras ...string) []string {
	if !qt.perWorkloadMetrics {
		workload = unknownWorkload
	}
	return append([]string{strategyName, workload, priorityStr}, extras...)
}

// extractWorkloadName returns the workload name from ExecuteOptions, or unknownWorkload
// when none was supplied (nil options or an empty WorkloadName).
func extractWorkloadName(options *querypb.ExecuteOptions) string {
	if options == nil || options.WorkloadName == "" {
		return unknownWorkload
	}
	return options.WorkloadName
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
		log.Warn(fmt.Sprintf("Invalid priority value '%s' in ExecuteOptions, expected integer 0-100, using default priority %d", options.Priority, defaultPriority))
		return defaultPriority
	}

	return optionsPriority
}

// HandleConfigUpdate applies a SrvKeyspace change to the QueryThrottler's config and
// strategy. It is only meant to run as a srvtopo.WatchSrvKeyspace callback, and is the
// only writer of qt.snapshot.
//
// It always returns true to keep the watch alive: errors are logged, never fatal, matching
// throttle.Throttler.WatchSrvKeyspaceCallback.
func (qt *QueryThrottler) HandleConfigUpdate(srvks *topodatapb.SrvKeyspace, err error) bool {
	// Log by error type, but keep watching — the resilient watcher retries transient errors.
	if err != nil {
		switch {
		case topo.IsErrType(err, topo.NoNode):
			log.Warn(fmt.Sprintf("HandleConfigUpdate: keyspace %s not found in topology (may not be created yet): %v", qt.keyspace, err))
		case errors.Is(err, context.Canceled) || topo.IsErrType(err, topo.Interrupted):
			log.Info(fmt.Sprintf("HandleConfigUpdate: watch interrupted for keyspace %s: %v", qt.keyspace, err))
		default:
			log.Error(fmt.Sprintf("HandleConfigUpdate: SrvKeyspace watch error for keyspace %s: %v", qt.keyspace, err))
		}
		return true
	}

	if srvks == nil {
		log.Warn("HandleConfigUpdate: srvks is nil")
		return true
	}

	newCfg := srvks.GetQueryThrottlerConfig()

	// Canonicalize threshold order first, so two Configs differing only in that order
	// compare equal below and count as a no-op update.
	newCfg = normalizeThresholds(newCfg)

	// Safe without the lock: per the contract above, only this callback writes the snapshot.
	currentSnap := qt.snapshot.Load()

	// Compare the whole proto, not just the top-level scalars: a nested-only change to
	// TabletStrategyConfig must still reach the strategy.
	if proto.Equal(currentSnap.cfg, newCfg) {
		return true
	}

	needsStrategyChange := currentSnap.cfg.GetStrategy() != newCfg.GetStrategy()
	newStrategy := currentSnap.strategy
	if needsStrategyChange {
		// Build outside the lock; this can be slow. The factory takes newCfg, so the new
		// strategy already holds the latest nested config and needs no UpdateConfig below.
		newStrategy = qt.buildNewStrategy(newCfg)
	}

	// Lock only for the swap. Start() runs under it so the shutdown check, Start, and
	// Store are atomic — otherwise a Shutdown landing between Start and Store would leak
	// the new strategy's goroutines. Safe to hold, since Start() is non-blocking.
	shutdownLost := false
	func() {
		qt.mu.Lock()
		defer qt.mu.Unlock()

		// Shutdown may have run while we built newStrategy outside the lock. If so,
		// drop the update; the discarded strategy is Stop()'d below.
		if qt.shutdown {
			shutdownLost = true
			return
		}

		if needsStrategyChange && newStrategy != nil {
			newStrategy.Start()
		} else if newStrategy != nil {
			// Strategy unchanged: push the config into the live strategy before storing the
			// snapshot, so both land together. Otherwise re-enabling with new rules could
			// briefly throttle against the strategy's stale nested config.
			newStrategy.UpdateConfig(newCfg)
		}
		qt.snapshot.Store(&stateSnapshot{
			cfg:      newCfg,
			strategy: newStrategy,
		})
	}()

	if shutdownLost {
		// The new strategy was never installed. Stop() it anyway, in case a future
		// constructor spawns side effects (today's is side-effect-free).
		if needsStrategyChange && newStrategy != nil {
			newStrategy.Stop()
		}
		log.Info("HandleConfigUpdate: discarded config update after Shutdown for keyspace=" + qt.keyspace)
		return true
	}

	// Stop the old strategy outside the lock; this can be slow.
	if needsStrategyChange && currentSnap.strategy != nil {
		currentSnap.strategy.Stop()
	}

	log.Info(fmt.Sprintf("HandleConfigUpdate: config updated, strategy=%s, enabled=%v", newCfg.GetStrategy(), newCfg.GetEnabled()))
	return true
}

// normalizeThresholds returns a Config with every Thresholds slice sorted ascending by
// Above, as GetThrottleDecision's binary search requires. A config written straight to
// topo bypasses sanitizeQueryThrottlerConfig and may arrive unsorted. Sorts a clone:
// srvtopo hands this same pointer to every other reader.
func normalizeThresholds(cfg *querythrottlerpb.Config) *querythrottlerpb.Config {
	if cfg.GetStrategy() != querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER {
		return cfg
	}

	out := cfg.CloneVT()
	for _, stmtRuleSet := range out.GetTabletStrategyConfig().GetTabletRules() {
		for _, metricRuleSet := range stmtRuleSet.GetStatementRules() {
			for _, rule := range metricRuleSet.GetMetricRules() {
				if ts := rule.GetThresholds(); len(ts) > 1 {
					sort.Slice(ts, func(i, j int) bool { return ts[i].GetAbove() < ts[j].GetAbove() })
				}
			}
		}
	}
	return out
}

// selectThrottlingStrategy returns the appropriate strategy implementation based on the config.
func selectThrottlingStrategy(cfg *querythrottlerpb.Config, client *throttle.Client, tabletConfig *tabletenv.TabletConfig, env tabletenv.Env, keyspace string, cell string, srvTopoServer srvtopo.Server) registry.ThrottlingStrategyHandler {
	deps := registry.Deps{
		ThrottleClient: client,
		TabletConfig:   tabletConfig,
		Env:            env,
		Keyspace:       keyspace,
		Cell:           cell,
		SrvTopoServer:  srvTopoServer,
	}
	return registry.CreateStrategy(cfg, deps)
}

// buildNewStrategy builds via the newStrategyFactory hook when set (tests inject a
// deterministic one), otherwise via the production wiring.
func (qt *QueryThrottler) buildNewStrategy(cfg *querythrottlerpb.Config) registry.ThrottlingStrategyHandler {
	if qt.newStrategyFactory != nil {
		return qt.newStrategyFactory(cfg)
	}
	return selectThrottlingStrategy(cfg, qt.throttlerClient, qt.tabletConfig, qt.env, qt.keyspace, qt.cell, qt.srvTopoServer)
}
