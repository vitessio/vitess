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

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/vt/log"
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
	// unknownWorkload is the Workload label value used both when the client supplies no
	// workload and as the bounded value when per-workload metrics are disabled. The stats
	// schema always carries a Workload label so the registered label cardinality is identical
	// for every QueryThrottler in the process, regardless of how each instance set
	// EnablePerWorkloadTableMetrics. Collapsing the value to this constant when the feature is
	// off avoids the unbounded cardinality of client-supplied WORKLOAD_NAME directives.
	unknownWorkload = "unknown"
)

var (
	metricsInitOnce   sync.Once
	requestsTotal     *stats.CountersWithMultiLabels
	requestsThrottled *stats.CountersWithMultiLabels
	totalLatency      *stats.MultiTimings
	evaluateLatency   *stats.MultiTimings
)

// initThrottlerMetrics registers the query throttler stats exactly once per process.
// The schema always carries a Workload label so the registered label cardinality is identical
// for every QueryThrottler in the process. This decouples registration (process-global, here)
// from the per-instance decision of what value to emit for Workload: buildLabels emits the
// client-supplied workload only when EnablePerWorkloadTableMetrics is on and the bounded
// unknownWorkload sentinel otherwise, so divergent per-instance flags can never produce a
// label-count mismatch (which would panic on the hot path).
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

// stateSnapshot is the immutable {cfg, strategy} pair that Throttle() reads
// from atomically on the hot path. HandleConfigUpdate replaces the whole snapshot
// rather than mutating individual fields, so Throttle() always observes a
// consistent (cfg, strategy) pair without locking.
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

	// mu serializes config updates (HandleConfigUpdate) against Shutdown and against
	// each other. The hot-path Throttle() reads do not take this lock; they load
	// the snapshot atomically instead.
	mu           sync.Mutex
	watchStarted atomic.Bool

	// shutdown, guarded by mu, latches true the first time Shutdown() runs. A
	// concurrent HandleConfigUpdate callback that built its new strategy outside
	// the lock must check this flag once it acquires mu and discard its work —
	// without it, a callback that loses the lock race to Shutdown would still
	// Start() and publish a strategy, leaking the strategy's background
	// goroutines (ticker, srvtopo watch) past process-level shutdown.
	shutdown bool

	// snapshot holds the current immutable {cfg, strategy} pair. Always non-nil after NewQueryThrottler. Updated via atomic Store in HandleConfigUpdate.
	snapshot atomic.Pointer[stateSnapshot]

	// perWorkloadMetrics, when true, makes throttler stats emit the client-supplied workload
	// as the Workload label value; when false the bounded unknownWorkload sentinel is emitted
	// instead. It gates only the label *value*, never the registered label *count* (the schema
	// always carries a Workload label). Read once at construction from
	// env.Config().EnablePerWorkloadTableMetrics; the workload value is otherwise unbounded
	// (client-supplied via WORKLOAD_NAME directive).
	perWorkloadMetrics bool

	// newStrategyFactory builds a strategy from a config snapshot. NewQueryThrottler
	// wires this to selectThrottlingStrategy with all production deps; tests that
	// bypass NewQueryThrottler may leave it nil and buildNewStrategy falls back.
	newStrategyFactory func(*querythrottlerpb.Config) registry.ThrottlingStrategyHandler

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

	// Latch shutdown before doing anything else so a concurrent
	// HandleConfigUpdate callback that is currently blocked on qt.mu observes it
	// the moment it acquires the lock — and discards any strategy it built
	// outside the lock rather than Start()'ing it past Shutdown.
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

// InitDBConfig initializes the keyspace for the config watch and loads the initial configuration.
// This method is called by TabletServer during the tablet initialization sequence (see
// go/vt/vttablet/tabletserver/tabletserver.go:InitDBConfig), which happens when:
//   - A tablet first starts up
//   - A tablet restarts after a crash or upgrade
//   - A new tablet node is added to the cluster
//
// Why initial config loading is critical:
// When a tablet starts (or restarts), it needs to immediately have the correct throttling
// configuration from the topology server. Without this, the tablet would run with the default
// NoOp strategy until the next configuration update is pushed to the topology, which could
// result in:
//   - Unthrottled queries overwhelming a recovering tablet
//   - Inconsistent throttling behavior across the fleet during rolling restarts
//   - Missing critical throttling rules during high-load periods
func (qt *QueryThrottler) InitDBConfig(keyspace string) {
	qt.keyspace = keyspace
	log.Info("QueryThrottler: initialized with keyspace=" + keyspace)

	// Start the topo server watch post the keyspace is set.
	qt.startSrvKeyspaceWatch()
}

// Throttle checks if the tablet is under heavy load and enforces throttling by rejecting the incoming request if necessary.
// Note: The hot path uses a single atomic load of the snapshot — no lock is taken — so concurrent config updates never tear the (cfg, strategy) pair.
// Config updates are rare  compared to query frequency, and the snapshot pointer is swapped atomically by HandleConfigUpdate.
func (qt *QueryThrottler) Throttle(ctx context.Context, tabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, transactionID int64, options *querypb.ExecuteOptions) error {
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
	decision := tStrategy.Evaluate(ctx, tabletType, parsedQuery, transactionID, attrs)

	// Record evaluate-window latency immediately after Evaluate returns
	evaluateLatency.Record(labels, startTime)

	requestsTotal.Add(labels, 1)

	// If no throttling is needed, allow the query
	if !decision.Throttle {
		return nil
	}

	// Emit metric of query being throttled.
	requestsThrottled.Add(qt.buildLabels(strategyName, attrs.WorkloadName, priorityStr, decision.MetricName, strconv.FormatBool(tCfg.GetDryRun())), 1)

	// If dry-run mode is enabled, log the decision but don't throttle
	if tCfg.GetDryRun() {
		log.Warn(fmt.Sprintf("[DRY-RUN] %s, metric name: %s, metric value: %f", decision.Message, decision.MetricName, decision.MetricValue))
		return nil
	}

	// Normal throttling: return an error to reject the query
	return vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, decision.Message)
}

// startSrvKeyspaceWatch starts watching the SrvKeyspace for event-driven config updates.
// This method performs two critical operations:
//  1. Initial Configuration Load (with retry):
//     Fetches the current SrvKeyspace configuration from the topology server using GetSrvKeyspace.
//     This is essential for tablets starting up or restarting, as they need immediate access to
//     throttling rules without waiting for a configuration change event.
//  2. Watch Establishment:
//     Starts a background goroutine that watches for future SrvKeyspace changes using WatchSrvKeyspace.
//     This ensures the tablet receives real-time configuration updates throughout its lifecycle.
//
// Thread Safety: This method uses the watchStarted atomic flag to ensure it only runs once, even if called
// concurrently. Only the first caller will actually start the watch; subsequent calls return early.
func (qt *QueryThrottler) startSrvKeyspaceWatch() {
	// Pre-flight validation: ensure required fields are set
	if qt.srvTopoServer == nil || qt.keyspace == "" {
		log.Error(fmt.Sprintf("QueryThrottler: cannot start SrvKeyspace watch, srvTopoServer=%v, keyspace=%s", qt.srvTopoServer != nil, qt.keyspace))
		return
	}

	// Phase 1: Load initial configuration with retry logic
	// This ensures tablets have the correct throttling config immediately after startup/restart.
	// TODO(Siddharth) add retry for this initial load
	srvKS, err := qt.srvTopoServer.GetSrvKeyspace(qt.ctx, qt.cell, qt.keyspace)
	if err != nil {
		log.Warn(fmt.Sprintf("QueryThrottler: failed to load initial config for keyspace=%s (GetSrvKeyspace): %v", qt.keyspace, err))
	}
	if srvKS == nil {
		log.Warn(fmt.Sprintf("QueryThrottler: srv keyspace fetched is nil for keyspace=%s ", qt.keyspace))
	}
	qt.HandleConfigUpdate(srvKS, nil)

	// Phase 2: Start the watch for future configuration updates
	// Always start the watch, even if initial load failed, to enable recovery when config becomes available

	// Only start the watch once (protected by atomic flag)
	if !qt.watchStarted.CompareAndSwap(false, true) {
		log.Info("QueryThrottler: SrvKeyspace watch already started for keyspace=" + qt.keyspace)
		return
	}
	watchCtx, cancel := context.WithCancel(qt.ctx)
	qt.cancelWatchContext = cancel

	go func() {
		// WatchSrvKeyspace will:
		// 1. Provide the current value immediately (may duplicate our GetSrvKeyspace result, but deduped)
		// 2. Stream future configuration updates via the callback
		// 3. Automatically retry on transient errors (handled by resilient watcher)
		qt.srvTopoServer.WatchSrvKeyspace(watchCtx, qt.cell, qt.keyspace, qt.HandleConfigUpdate)
	}()

	log.Info(fmt.Sprintf("QueryThrottler: started event-driven watch for SrvKeyspace keyspace=%s cell=%s", qt.keyspace, qt.cell))
}

// buildLabels returns the throttler stat label set: the base {Strategy, Workload, Priority}
// followed by any extras. The schema always carries a Workload label; this method decides only
// its value — the client-supplied workload when per-workload metrics are enabled, otherwise the
// bounded unknownWorkload sentinel, because the workload comes from the client-controlled
// WORKLOAD_NAME directive and is otherwise unbounded, which would cause label-cardinality blowup
// in the hot path.
func (qt *QueryThrottler) buildLabels(strategyName, workload, priorityStr string, extras ...string) []string {
	if !qt.perWorkloadMetrics {
		workload = unknownWorkload
	}
	return append([]string{strategyName, workload, priorityStr}, extras...)
}

// extractWorkloadName extracts the workload name from ExecuteOptions.
// Returns "unknown" whenever no workload was supplied — both when ExecuteOptions
// itself is nil and when it carries an empty WorkloadName. The two cases were
// previously distinguished as "unknown"/"default" but the split only added
// metric-label cardinality without giving operators an actionable signal.
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

// HandleConfigUpdate is the callback invoked when the SrvKeyspace topology changes.
// It loads the updated configuration from the topo server and updates the QueryThrottler's
// strategy and configuration accordingly.
//
// IMPORTANT: This method is designed ONLY to be called as a callback from srvtopo.WatchSrvKeyspace.
// It relies on the resilient watcher's auto-retry behavior (see go/vt/srvtopo/watch.go) and should
// not be called directly from other contexts.
//
// Return value contract (required by WatchSrvKeyspace):
//   - Always returns true to keep the watch alive. Errors are logged but never stop the watch,
//     matching the pattern used by throttle.Throttler.WatchSrvKeyspaceCallback.
//
// **NOTE: this method is written with the assumption that this is the only piece of code which will be changing the config of QueryThrottler**
func (qt *QueryThrottler) HandleConfigUpdate(srvks *topodatapb.SrvKeyspace, err error) bool {
	// Log errors by type for observability, but always keep watching.
	// The resilient watcher will automatically retry on transient errors.
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

	// Get the query throttler configuration from the SrvKeyspace that the QueryThrottler uses to manage its throttling behavior.
	newCfg := srvks.GetQueryThrottlerConfig()

	// Defensively sort each MetricRule's Thresholds slice ascending by Above so
	// GetThrottleDecision's binary search (and any downstream "thresholds[0] is
	// the floor" assumption) is correct even if this SrvKeyspace was written
	// directly to topo, bypassing sanitizeQueryThrottlerConfig on the RPC path.
	// Sorting BEFORE the proto.Equal short-circuit below lets the short-circuit
	// also operate on a canonical order — two semantically-equal Configs that
	// differ only in threshold order are treated as no-op updates.
	if newCfg.GetStrategy() == querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER {
		for _, stmtRuleSet := range newCfg.GetTabletStrategyConfig().GetTabletRules() {
			for _, metricRuleSet := range stmtRuleSet.GetStatementRules() {
				for _, rule := range metricRuleSet.GetMetricRules() {
					ts := rule.GetThresholds()
					if len(ts) > 1 {
						sort.Slice(ts, func(i, j int) bool {
							return ts[i].GetAbove() < ts[j].GetAbove()
						})
					}
				}
			}
		}
	}

	// Atomic load: safe without the lock. Per the function contract, only this
	// callback writes to qt.snapshot, so the read here is also single-writer.
	currentSnap := qt.snapshot.Load()

	// Full proto.Equal short-circuit: skip all work when the new SrvKeyspace carries
	// an identical Config (top-level + nested). Replaces an older helper that only
	// compared the three top-level scalars (Enabled, Strategy, DryRun) and silently
	// dropped nested-only TabletStrategyConfig updates once the strategy stopped
	// running its own SrvKeyspace watch.
	if proto.Equal(currentSnap.cfg, newCfg) {
		return true
	}

	needsStrategyChange := currentSnap.cfg.GetStrategy() != newCfg.GetStrategy()
	newStrategy := currentSnap.strategy
	if needsStrategyChange {
		// Build the new strategy outside the lock; this can be slow (e.g. wiring
		// up dependencies). The factory consumes newCfg, so the freshly built
		// strategy already holds the latest nested config — no UpdateConfig call
		// is needed below for this path.
		newStrategy = qt.buildNewStrategy(newCfg)
	}

	// Take the lock only for the swap to serialize with Shutdown. Start() runs
	// under the lock so the shutdown check, Start, and snapshot.Store are atomic —
	// this prevents a concurrent Shutdown from completing between Start and Store and
	// leaking the new strategy's background goroutines. This is safe to hold across
	// Start() because Start() is non-blocking (its initial cache prime is async), so a
	// slow throttle check can no longer stall this lock. Throttle() never contends on it.
	shutdownLost := false
	func() {
		qt.mu.Lock()
		defer qt.mu.Unlock()

		// Shutdown may have run while we were building newStrategy outside the
		// lock. If so, drop the update on the floor; the discarded strategy is
		// Stop()'d below as defense-in-depth.
		if qt.shutdown {
			shutdownLost = true
			return
		}

		if needsStrategyChange && newStrategy != nil {
			newStrategy.Start()
		} else if newStrategy != nil {
			// Strategy unchanged: push the new config into the live strategy
			// BEFORE publishing the new snapshot so the (top-level, nested)
			// pair becomes visible atomically. Without this synchronous push,
			// re-enabling with new rules could briefly throttle against the
			// strategy's stale nested config.
			newStrategy.UpdateConfig(newCfg)
		}
		qt.snapshot.Store(&stateSnapshot{
			cfg:      newCfg,
			strategy: newStrategy,
		})
	}()

	if shutdownLost {
		// The freshly built strategy was never installed. Stop() it so any
		// side effects the constructor may have spawned (current
		// TabletThrottlerStrategy is side-effect-free, but future changes may
		// not be) are released rather than leaked.
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

// buildNewStrategy routes strategy construction through the optional test hook
// newStrategyFactory; tests can swap it for deterministic injection. When unset
// (tests that bypass NewQueryThrottler), falls back to the production wiring so
// existing tests continue to work unchanged.
func (qt *QueryThrottler) buildNewStrategy(cfg *querythrottlerpb.Config) registry.ThrottlingStrategyHandler {
	if qt.newStrategyFactory != nil {
		return qt.newStrategyFactory(cfg)
	}
	return selectThrottlingStrategy(cfg, qt.throttlerClient, qt.tabletConfig, qt.env, qt.keyspace, qt.cell, qt.srvTopoServer)
}
