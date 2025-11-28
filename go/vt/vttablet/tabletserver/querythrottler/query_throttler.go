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
	"strconv"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"

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
	// defaultPriority is the default priority value when none is specified
	defaultPriority = 100 // sqlparser.MaxPriorityValue
)

type QueryThrottler struct {
	ctx                context.Context
	cancelWatchContext context.CancelFunc

	throttleClient *throttle.Client
	tabletConfig   *tabletenv.TabletConfig

	keyspace      string
	cell          string
	srvTopoServer srvtopo.Server

	mu           sync.RWMutex
	watchStarted atomic.Bool

	// cfg holds the current configuration for the throttler.
	cfg Config
	// strategyHandlerInstance is the current throttling strategy handler instance
	strategyHandlerInstance registry.ThrottlingStrategyHandler
}

// NewQueryThrottler creates a new  query throttler.
func NewQueryThrottler(ctx context.Context, throttler *throttle.Throttler, env tabletenv.Env, alias *topodatapb.TabletAlias, srvTopoServer srvtopo.Server) *QueryThrottler {
	client := throttle.NewBackgroundClient(throttler, throttlerapp.QueryThrottlerName, base.UndefinedScope)

	qt := &QueryThrottler{
		ctx:                     ctx,
		throttleClient:          client,
		tabletConfig:            env.Config(),
		cell:                    alias.GetCell(),
		srvTopoServer:           srvTopoServer,
		cfg:                     Config{},
		strategyHandlerInstance: &registry.NoOpStrategy{}, // default strategy until config is loaded
	}

	// Start the initial strategy
	qt.strategyHandlerInstance.Start()

	return qt
}

// Shutdown gracefully stops the throttler and cleans up resources.
// This should be called when the QueryThrottler is no longer needed.
func (qt *QueryThrottler) Shutdown() {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	// Stop the current strategy to clean up any background processes
	if qt.strategyHandlerInstance != nil {
		qt.strategyHandlerInstance.Stop()
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
	log.Infof("QueryThrottler: initialized with keyspace=%s", keyspace)

	// Start the topo server watch post the keyspace is set.
	//qt.startSrvKeyspaceWatch()
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
	if !qt.cfg.Enabled {
		return nil
	}

	// Extract query attributes once to avoid re computation in strategies
	attrs := registry.QueryAttributes{
		WorkloadName: extractWorkloadName(options),
		Priority:     extractPriority(options),
	}

	// Evaluate the throttling decision
	decision := qt.strategyHandlerInstance.Evaluate(ctx, tabletType, parsedQuery, transactionID, attrs)

	// If no throttling is needed, allow the query
	if !decision.Throttle {
		return nil
	}

	// If dry-run mode is enabled, log the decision but don't throttle
	if qt.cfg.DryRun {
		log.Warningf("[DRY-RUN] %s", decision.Message)
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
		log.Errorf("QueryThrottler: cannot start SrvKeyspace watch, srvTopoServer=%v, keyspace=%s", qt.srvTopoServer != nil, qt.keyspace)
		return
	}

	// Phase 1: Load initial configuration with retry logic
	// This ensures tablets have the correct throttling config immediately after startup/restart.
	// TODO(Siddharth) add retry for this initial load
	srvKS, err := qt.srvTopoServer.GetSrvKeyspace(qt.ctx, qt.cell, qt.keyspace)
	if err != nil {
		log.Warningf("QueryThrottler: failed to load initial config for keyspace=%s (GetSrvKeyspace): %v", qt.keyspace, err)
	}
	if srvKS == nil {
		log.Warningf("QueryThrottler: srv keyspace fetched is nil for keyspace=%s ", qt.keyspace)
	}
	qt.HandleConfigUpdate(srvKS, nil)

	// Phase 2: Start the watch for future configuration updates
	// Always start the watch, even if initial load failed, to enable recovery when config becomes available

	// Only start the watch once (protected by atomic flag)
	if !qt.watchStarted.CompareAndSwap(false, true) {
		log.Infof("QueryThrottler: SrvKeyspace watch already started for keyspace=%s", qt.keyspace)
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

	log.Infof("QueryThrottler: started event-driven watch for SrvKeyspace keyspace=%s cell=%s", qt.keyspace, qt.cell)
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

// HandleConfigUpdate is the callback invoked when the SrvKeyspace topology changes.
// It loads the updated configuration from the topo server and updates the QueryThrottler's
// strategy and configuration accordingly.
//
// IMPORTANT: This method is designed ONLY to be called as a callback from srvtopo.WatchSrvKeyspace.
// It relies on the resilient watcher's auto-retry behavior (see go/vt/srvtopo/watch.go) and should
// not be called directly from other contexts.
//
// Return value contract (required by WatchSrvKeyspace):
//   - true: Continue watching (resilient watcher will auto-retry on transient errors)
//   - false: Stop watching permanently (for fatal errors like NoNode, context canceled, or Interrupted)
//
// **NOTE: this method is written with the assumption that this is the only piece of code which will be changing the config of QueryThrottler**
func (qt *QueryThrottler) HandleConfigUpdate(srvks *topodatapb.SrvKeyspace, err error) bool {
	// Handle topology errors using a hybrid approach:
	// - Permanent errors (NoNode, context canceled): stop watching (return false)
	// - Transient errors (network issues, etc.): keep watching (return true, auto-retry will reconnect)
	if err != nil {
		// Keyspace deleted from topology - stop watching
		if topo.IsErrType(err, topo.NoNode) {
			log.Warningf("HandleConfigUpdate: keyspace %s deleted or not found, stopping watch", qt.keyspace)
			return false
		}

		// Context canceled or interrupted - graceful shutdown, stop watching
		if errors.Is(err, context.Canceled) || topo.IsErrType(err, topo.Interrupted) {
			log.Infof("HandleConfigUpdate: watch stopped (context canceled or interrupted)")
			return false
		}

		// Transient error (network, temporary topo server issue) - keep watching
		// The resilient watcher will automatically retry as defined in go/vt/srvtopo/resilient_server.go:46
		log.Warningf("HandleConfigUpdate: transient topo watch error (will retry): %v", err)
		return true
	}

	if srvks == nil {
		log.Warningf("HandleConfigUpdate: srvks is nil")
		return true
	}

	// Get the query throttler configuration from the SrvKeyspace that the QueryThrottler uses to manage its throttling behavior.
	iqtConfig := srvks.GetQueryThrottlerConfig()
	newCfg := ConfigFromProto(iqtConfig)

	// If the config is not changed, return early.
	if !isConfigUpdateRequired(qt.cfg, newCfg) {
		return true
	}

	// No Locking is required because only this function updates the configs of Query Throttler.
	needsStrategyChange := qt.cfg.GetStrategyName() != newCfg.GetStrategyName()
	oldStrategyInstance := qt.strategyHandlerInstance

	var newStrategy registry.ThrottlingStrategyHandler
	if needsStrategyChange {
		// Create the new strategy (doesn't need lock)
		newStrategy = selectThrottlingStrategy(newCfg, qt.throttleClient, qt.tabletConfig)
	}

	// Acquire write lock only for the actual swap operation
	qt.mu.Lock()
	if needsStrategyChange {
		qt.strategyHandlerInstance = newStrategy
		// Start a new strategy after assignment, still under lock for consistency.
		if newStrategy != nil {
			newStrategy.Start()
		}
	}
	// Always update the configuration
	qt.cfg = newCfg
	qt.mu.Unlock()

	// Stop the old strategy (if needed) outside the lock to avoid blocking.
	if needsStrategyChange && oldStrategyInstance != nil {
		oldStrategyInstance.Stop()
	}

	log.Infof("HandleConfigUpdate: config updated, strategy=%s, enabled=%v", newCfg.GetStrategyName(), newCfg.Enabled)
	return true
}

// selectThrottlingStrategy returns the appropriate strategy implementation based on the config.
func selectThrottlingStrategy(cfg Config, client *throttle.Client, tabletConfig *tabletenv.TabletConfig) registry.ThrottlingStrategyHandler {
	deps := registry.Deps{
		ThrottleClient: client,
		TabletConfig:   tabletConfig,
	}
	return registry.CreateStrategy(cfg, deps)
}

// isConfigUpdateRequired checks if the new config is different from the old config.
// This only checks for enabled, strategy name, and dry run because the strategy itself will update the strategy-specific config
// during runtime by having a separate watcher similar to the one used in QueryThrottler.
func isConfigUpdateRequired(oldCfg, newCfg Config) bool {
	if oldCfg.Enabled != newCfg.Enabled {
		return true
	}

	if oldCfg.StrategyName != newCfg.StrategyName {
		return true
	}

	if oldCfg.DryRun != newCfg.DryRun {
		return true
	}

	return false
}
