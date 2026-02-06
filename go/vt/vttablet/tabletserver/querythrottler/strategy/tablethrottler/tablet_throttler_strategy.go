/*
Copyright 2026 The Vitess Authors.

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

package tabletthrottler

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sort"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

var (
	// Compile-time interface compliance check
	_ registry.ThrottlingStrategyHandler = (*TabletThrottlerStrategy)(nil)
	_ registry.StrategyFactory           = (*tabletThrottlerStrategyFactory)(nil)

	// Named exporter for TabletThrottlerStrategy metrics.
	// Using a named exporter (non-empty name) ensures thread-safe deduplication
	// via exporterMu mutex and exportedSingleCountVars/exportedTimingsVars maps.
	// This prevents panics when switching strategies (TabletThrottler → Cinnamon → TabletThrottler)
	// because the exporter returns existing metrics instead of re-registering them.
	throttlerExporter = servenv.NewExporter("TabletThrottler", "Tablet")
	sharedMetrics     *tabletThrottlerMetrics
)

func init() {
	registry.Register(querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, &tabletThrottlerStrategyFactory{})
}

// initMetrics initializes metrics for TabletThrottlerStrategy using a dedicated named exporter.
// Named exporters provide thread-safe deduplication via exporterMu mutex, preventing panics
// when switching strategies (TabletThrottler → Cinnamon → TabletThrottler).
//
// Unlike unnamed exporters (empty name) which directly call expvar.Publish and panic on
// duplicate names, named exporters check tracking maps first and return existing metrics.
func initMetrics() *tabletThrottlerMetrics {
	if sharedMetrics != nil {
		return sharedMetrics
	}

	prefix := querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER.String()
	sharedMetrics = &tabletThrottlerMetrics{
		cacheMisses: throttlerExporter.NewCounter(
			prefix+"CacheMisses",
			"incoming query throttler cache misses",
		),
		cacheHits: throttlerExporter.NewCounter(
			prefix+"CacheHits",
			"incoming query throttler cache hits",
		),
		decisionCount: throttlerExporter.NewCountersWithMultiLabels(
			prefix+"DecisionCount",
			"tablet throttler decisions by outcome and reason",
			[]string{"tablet_type", "stmt_type", "path", "outcome", "reason"},
		),
		fastDecisionLatency: throttlerExporter.NewMultiTimings(
			prefix+"FastDecisionLatencyMicroseconds",
			"fast-path tablet throttler decision latency in microseconds",
			[]string{"tablet_type", "outcome"},
		),
		fullDecisionLatency: throttlerExporter.NewMultiTimings(
			prefix+"FullDecisionLatencyMicroseconds",
			"full-path tablet throttler decision latency in microseconds",
			[]string{"tablet_type", "stmt_type", "outcome"},
		),
		cacheLoadLatency: throttlerExporter.NewMultiTimings(
			prefix+"CacheLoadLatencyMilliseconds",
			"tablet throttler cache load latency in milliseconds",
			[]string{"status"},
		),
	}
	return sharedMetrics
}

// tabletThrottlerStrategyFactory creates TabletThrottlerStrategy instances.
type tabletThrottlerStrategyFactory struct{}

func (f *tabletThrottlerStrategyFactory) New(deps registry.Deps, cfg *querythrottlerpb.Config) (registry.ThrottlingStrategyHandler, error) {
	tabletCfg := cfg.GetTabletStrategyConfig()
	if tabletCfg == nil {
		tabletCfg = &querythrottlerpb.TabletStrategyConfig{}
	}
	return NewTabletThrottlerStrategy(deps.ThrottleClient, tabletCfg, deps.TabletConfig, deps.Env, deps.Keyspace, deps.Cell, deps.SrvTopoServer), nil
}

// Configuration constants for caching behavior
const (
	// _throttleCheckTimeout defines the timeout for individual throttle check calls
	_throttleCheckTimeout = 5 * time.Second

	_stmtTypeNotAvailable              = "NA"
	_decisionPathFast                  = "fast"
	_decisionPathFull                  = "full"
	_decisionOutcomeAllowed            = "allowed"
	_decisionOutcomeThrottled          = "throttled"
	_decisionReasonBypassFast          = "bypass_fast"
	_decisionReasonBypassPrior         = "bypass_priority"
	_decisionReasonNoRuleForTabletType = "no_rule_for_tablet_type"
	_decisionReasonNoRuleForStmtType   = "no_rule_for_stmt_type"
	_decisionReasonNoMetricBreach      = "no_metric_breach"
	_decisionReasonMetricBreach        = "metric_breach"
	_decisionReasonQueryAllowed        = "query_allowed"

	_cacheRefreshStatusSuccess = "success"
	_cacheRefreshStatusTimeout = "timeout"
)

// cacheState holds the immutable cached throttle check result state.
type cacheState struct {
	ok     bool
	result *throttle.CheckResult
}

// tabletThrottlerMetrics holds all metrics for the TabletThrottlerStrategy.
type tabletThrottlerMetrics struct {
	cacheMisses         *stats.Counter
	cacheHits           *stats.Counter
	decisionCount       *stats.CountersWithMultiLabels
	fastDecisionLatency *servenv.MultiTimingsWrapper
	fullDecisionLatency *servenv.MultiTimingsWrapper
	cacheLoadLatency    *servenv.MultiTimingsWrapper
}

// TabletThrottlerStrategy uses the Vitess Tablet Throttler (https://vitess.io/docs/21.0/reference/features/tablet-throttler) to enforce throttling.
type TabletThrottlerStrategy struct {
	throttleClient ThrottleClientWrapper
	config         atomic.Pointer[querythrottlerpb.TabletStrategyConfig]
	tabletConfig   *tabletenv.TabletConfig
	keyspace       string // keyspace for targeted SrvKeyspace watch
	cell           string
	srvTopoServer  srvtopo.Server

	// Caching field for throttle check results - single atomic for race-free access
	cachedState atomic.Pointer[cacheState]

	// Background updater lifecycle management
	ctx          context.Context
	cancel       context.CancelFunc
	updateTicker *time.Ticker
	done         chan struct{}
	running      atomic.Bool

	// SrvKeyspace watch lifecycle management
	watchStarted atomic.Bool

	// metrics - pointer to shared metrics using dedicated named exporter for thread-safe deduplication
	metrics                   *tabletThrottlerMetrics
	fastPathLatencySampleRate float64

	// Injectable random functions for testing (defaults to math/rand/v2)
	randFloat64 func() float64
	randIntN    func(n int) int
}

// NewTabletThrottlerStrategy creates a new TabletThrottlerStrategy.
func NewTabletThrottlerStrategy(throttleClient ThrottleClientWrapper, cfg *querythrottlerpb.TabletStrategyConfig, tabletConfig *tabletenv.TabletConfig, env tabletenv.Env, keyspace string, cell string, srvTopoServer srvtopo.Server) *TabletThrottlerStrategy {
	ctx, cancel := context.WithCancel(context.Background())

	strategy := &TabletThrottlerStrategy{
		throttleClient:            throttleClient,
		tabletConfig:              tabletConfig,
		ctx:                       ctx,
		cancel:                    cancel,
		keyspace:                  keyspace,
		cell:                      cell,
		srvTopoServer:             srvTopoServer,
		done:                      make(chan struct{}),
		metrics:                   initMetrics(),
		fastPathLatencySampleRate: 0.1,
		randFloat64:               rand.Float64,
		randIntN:                  rand.IntN,
	}
	// Store the default config.
	strategy.config.Store(cfg)

	// Start the topo server watch post the keyspace is set.
	strategy.startSrvKeyspaceWatch()

	return strategy
}

// Start begins the background throttle check updater.
// This should be called after creating the strategy to enable caching.
func (s *TabletThrottlerStrategy) Start() {
	if s.running.CompareAndSwap(false, true) {
		// Prime the cache immediately to eliminate initial cache misses
		s.refreshCache()

		updateInterval := s.tabletConfig.TabletThrottlerCacheUpdateInterval
		s.updateTicker = time.NewTicker(updateInterval)
		go s.runCacheUpdater()
		log.Info("TabletThrottlerStrategy: started background throttle cache updater")
	}
}

// Stop stops the background throttle check updater and releases resources.
func (s *TabletThrottlerStrategy) Stop() {
	if s.running.CompareAndSwap(true, false) {
		// Cancel the main context - this stops both the cache updater goroutine
		// and the SrvKeyspace watch goroutine (via parent-child context propagation)
		s.cancel()

		// Reset the watch-started flag to allow restarting the watch if needed
		s.watchStarted.Store(false)

		if s.updateTicker != nil {
			s.updateTicker.Stop()
		}
		<-s.done // Wait for cache updater to finish

		log.Info("TabletThrottlerStrategy: stopped background throttle cache updater and watch")
	}
}

// startSrvKeyspaceWatch starts watching the SrvKeyspace for event-driven config updates.
// This uses the topo server watch mechanism to receive immediate notifications when the SrvKeyspace configuration changes.
func (s *TabletThrottlerStrategy) startSrvKeyspaceWatch() {
	log.Infof("TabletThrottlerStrategy: starting SrvKeyspace watch for keyspace=%s cell=%s", s.keyspace, s.cell)
	// Only start once
	if !s.watchStarted.CompareAndSwap(false, true) {
		return
	}

	if s.srvTopoServer == nil || s.keyspace == "" || s.cell == "" {
		log.Warningf("TabletThrottlerStrategy: cannot start SrvKeyspace watch, srvTopoServer=%v, keyspace=%s, cell=%s", s.srvTopoServer != nil, s.keyspace, s.cell)
		s.watchStarted.Store(false)
		return
	}

	// Start the watch using the main context. When s.cancel() is called in Stop(), the resilient watcher will detect the context cancellation and stop retrying.
	go func() {
		s.srvTopoServer.WatchSrvKeyspace(s.ctx, s.cell, s.keyspace, s.HandleConfigUpdate)
	}()

	log.Infof("TabletThrottlerStrategy: started event-driven watch for SrvKeyspace keyspace=%s cell=%s", s.keyspace, s.cell)
}

// runCacheUpdater runs in a background goroutine to periodically refresh cached throttle results.
func (s *TabletThrottlerStrategy) runCacheUpdater() {
	defer close(s.done)
	defer s.updateTicker.Stop()

	for {
		select {
		case <-s.updateTicker.C:
			s.refreshCache()
		case <-s.ctx.Done():
			return
		}
	}
}

// refreshCache updates the cached throttle check results.
func (s *TabletThrottlerStrategy) refreshCache() {
	ctx, cancel := context.WithTimeout(s.ctx, _throttleCheckTimeout)
	defer cancel()

	start := time.Now()
	checkResult, checkOk := s.throttleClient.ThrottleCheckOK(ctx, throttlerapp.QueryThrottlerName)

	status := _cacheRefreshStatusSuccess
	if err := ctx.Err(); err == context.DeadlineExceeded {
		status = _cacheRefreshStatusTimeout
	} else {
		// Create new immutable state and store atomically
		state := &cacheState{
			ok:     checkOk,
			result: checkResult,
		}
		s.cachedState.Store(state)
	}

	s.metrics.cacheLoadLatency.Record([]string{status}, start)
}

// getCachedThrottleResult returns the current cached throttle check result.
// Always returns a valid result by falling back to direct calls when needed.
func (s *TabletThrottlerStrategy) getCachedThrottleResult(ctx context.Context) (*throttle.CheckResult, bool) {
	if s.running.Load() {
		// Use cached results for better performance with single atomic load
		state := s.cachedState.Load()

		// If cache is running but state is nil (not initialized yet), fall back to direct call
		if state == nil {
			s.metrics.cacheMisses.Add(1)
			return s.throttleClient.ThrottleCheckOK(ctx, throttlerapp.QueryThrottlerName)
		}

		s.metrics.cacheHits.Add(1)
		return state.result, state.ok
	}

	// Fallback: direct call if cache not running (e.g., during startup)
	return s.throttleClient.ThrottleCheckOK(ctx, throttlerapp.QueryThrottlerName)
}

// Evaluate determines whether a given SQL query should be throttled
// based on tablet type, SQL statement type, and real-time metrics.
//
// It follows a rule-based configuration where specific tablet types and
// SQL statement types are associated with metric thresholds. If a threshold
// is exceeded and a probabilistic throttle condition is met, the query should be throttled.
//
// Parameters:
//   - ctx: context for timeout/cancellation control.
//   - targetTabletType: the type of tablet the query is being run against (e.g., MASTER, REPLICA).
//   - sql: the raw SQL query string.
//   - transactionID: the ID of the transaction (not used in throttling logic).
//   - attrs: pre-computed query attributes containing workload name and priority.
//
// Returns:
//   - ThrottleDecision containing detailed information about the throttling decision.
func (s *TabletThrottlerStrategy) Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, transactionID int64, attrs registry.QueryAttributes) registry.ThrottleDecision {
	// FOR DDL statements parsedQuery can be nil because ParsedQuery is `plan.FullQuery` which is nil for DDL statements.
	// `plan.go` file has `func Build(env *vtenv.Environment, statement sqlparser.Statement, tables map[string]*schema.Table, dbName string, viewsEnabled bool) (plan *Plan, err error)`
	// This function for ALTER MIGRATION and REVERT MIGRATION does not pass FullQuery and hence parsedQuery comes as nil here.
	// TODO(Siddharth) Discuss with OSS folks why this is the case, ideally having FullQuery in Plan for Alter and Revert should be harmless.
	if parsedQuery == nil {
		return registry.ThrottleDecision{
			Throttle: false,
			Message:  "No query to throttle",
		}
	}

	startTime := time.Now()
	tabletTypeStr := targetTabletType.String()
	// FAST PATH: Check if system is healthy before doing any expensive work
	// This optimizes for the common case (90-95% of queries) where checkOk == true
	if s.running.Load() {
		if state := s.cachedState.Load(); state != nil && state.ok {
			s.recordFastDecision(tabletTypeStr, _decisionOutcomeAllowed, startTime)
			return registry.ThrottleDecision{
				Throttle: false,
				Message:  "System healthy, fast-path bypass",
			}
		}
	}

	// Use pre-computed query attributes to avoid recomputation
	workloadName := attrs.WorkloadName
	priority := attrs.Priority
	stmtType := sqlparser.Preview(parsedQuery.Query).String()

	// Step 1: Early priority-based throttling check
	// Similar to tx_throttler.go: lower priority values (higher priority) are less likely to be throttled
	// Priority behavior:
	//   - Priority 0 (highest): NEVER throttled (rand(0-99) < 0 is always false)
	//   - Priority 100 (lowest): ALWAYS checked for throttling (rand(0-99) < 100 is always true)
	//   - Priority 1-99: Probabilistically checked based on priority value
	// If priority check fails, skip all expensive throttle checks
	priorityCheck := s.randIntN(sqlparser.MaxPriorityValue) < priority
	if !priorityCheck {
		s.recordFullDecision(tabletTypeStr, stmtType, _decisionOutcomeAllowed, _decisionReasonBypassPrior, startTime)
		return registry.ThrottleDecision{
			Throttle: false,
			Message:  fmt.Sprintf("High priority query (priority=%d), skip throttling", priority),
		}
	}

	// Step 2: Look up throttling rules for this tablet type (e.g., PRIMARY, REPLICA)
	// Load config once to ensure consistent view throughout the evaluation
	cfg := s.config.Load()
	stmtRules, ok := cfg.TabletRules[tabletTypeStr]
	if !ok {
		s.recordFullDecision(tabletTypeStr, stmtType, _decisionOutcomeAllowed, _decisionReasonNoRuleForTabletType, startTime)
		return registry.ThrottleDecision{
			Throttle: false,
			Message:  "No throttling rules for tablet type: " + targetTabletType.String(),
		}
	}

	// Step 3: Determine SQL statement type (e.g., INSERT, SELECT)
	// Step 4: Look up metric rules for this statement type
	metricRuleSet, ok := stmtRules.GetStatementRules()[stmtType]
	if !ok {
		s.recordFullDecision(tabletTypeStr, stmtType, _decisionOutcomeAllowed, _decisionReasonNoRuleForStmtType, startTime)
		return registry.ThrottleDecision{
			Throttle: false,
			Message:  "No throttling rules for SQL type: " + stmtType,
		}
	}

	// Step 5: Get cached throttle check results
	// checkResult is guaranteed to be non-nil now due to fallback logic in getCachedThrottleResult
	checkResult, checkOk := s.getCachedThrottleResult(ctx)
	// If check passes, system is not overloaded → allow query
	if checkOk {
		s.recordFullDecision(tabletTypeStr, stmtType, _decisionOutcomeAllowed, _decisionReasonNoMetricBreach, startTime)
		return registry.ThrottleDecision{
			Throttle: false,
			Message:  "System not overloaded, allowing query",
		}
	}

	// Step 6: Evaluate metrics and find the max throttle ratio
	var (
		maxThrottleRatio     float64
		maxMetricName        string
		maxMetricValue       float64
		maxBreachedThreshold float64
	)

	for metricName, result := range checkResult.Metrics {
		// Skip metrics that did not breach a configured threshold
		if result.ResponseCode != tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED {
			continue
		}

		// Only act on metrics explicitly configured for this SQL type
		rule, found := metricRuleSet.GetMetricRules()[metricName]
		if !found {
			continue
		}

		// Step 7: Calculate throttle probability and breached threshold
		throttleRatio, breachedThreshold := GetThrottleDecision(result.Value, rule.GetThresholds())

		// Track the metric with the highest throttle ratio
		if throttleRatio > maxThrottleRatio {
			maxThrottleRatio = throttleRatio
			maxMetricName = metricName
			maxMetricValue = result.Value
			maxBreachedThreshold = breachedThreshold
		}
	}

	// Step 8: Apply probabilistic throttling based on max throttle ratio (priority check already passed)
	if maxThrottleRatio > 0 && maxThrottleRatio > s.randFloat64() {
		s.recordFullDecision(tabletTypeStr, stmtType, _decisionOutcomeThrottled, _decisionReasonMetricBreach, startTime)
		return registry.ThrottleDecision{
			Throttle: true,
			Message: fmt.Sprintf("[VTTabletThrottler] Query=\"%s\" throttled: workload=%s priority=%d metric=%s value=%."+
				"2f breached threshold=%.2f throttle=%.0f%%", parsedQuery.Query, workloadName, priority, maxMetricName, maxMetricValue, maxBreachedThreshold,
				maxThrottleRatio*100),
			MetricName:         maxMetricName,
			MetricValue:        maxMetricValue,
			Threshold:          maxBreachedThreshold,
			ThrottlePercentage: maxThrottleRatio,
		}
	}
	// No throttle triggered → allow query
	s.recordFullDecision(tabletTypeStr, stmtType, _decisionOutcomeAllowed, _decisionReasonQueryAllowed, startTime)
	return registry.ThrottleDecision{
		Throttle: false,
		Message:  "No throttling conditions met",
	}
}

// GetStrategyName returns the name of the strategy.
func (s *TabletThrottlerStrategy) GetStrategyName() string {
	return string(querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER.String())
}

// GetThrottleDecision determines the throttle ratio (0.0–1.0) and the breached threshold.
// It uses binary search to find the highest threshold where value > threshold.Above.
// Assumes thresholds are sorted in ascending order by the Above field.
func GetThrottleDecision(value float64, thresholds []*querythrottlerpb.ThrottleThreshold) (throttleRatio float64, breached float64) {
	// Binary search to find the rightmost threshold where value > threshold.Above
	// sort.Search returns the smallest index i where thresholds[i].Above >= value
	idx := sort.Search(len(thresholds), func(i int) bool {
		return thresholds[i].GetAbove() >= value
	})

	// If idx == 0, value is <= all thresholds, so no threshold is breached
	// Otherwise, idx-1 is the highest threshold that was breached
	if idx > 0 {
		threshold := thresholds[idx-1]
		throttleRatio = float64(threshold.GetThrottle()) / 100.0
		breached = threshold.GetAbove()
	}

	return
}

func (s *TabletThrottlerStrategy) recordFastDecision(tabletType, outcome string, start time.Time) {
	s.metrics.decisionCount.Add([]string{tabletType, _stmtTypeNotAvailable, _decisionPathFast, outcome, _decisionReasonBypassFast}, 1)

	if s.fastPathLatencySampleRate > 0 && s.randFloat64() < s.fastPathLatencySampleRate {
		s.metrics.fastDecisionLatency.Record([]string{tabletType, outcome}, start)
	}
}

func (s *TabletThrottlerStrategy) recordFullDecision(tabletType, stmtType, outcome, reason string, start time.Time) {
	s.metrics.decisionCount.Add([]string{tabletType, stmtType, _decisionPathFull, outcome, reason}, 1)

	s.metrics.fullDecisionLatency.Record([]string{tabletType, stmtType, outcome}, start)
}

// HandleConfigUpdate is the callback invoked when the SrvKeyspace topology changes.
// It loads the updated TabletThrottlerStrategy configuration from the topo server and updates the strategy's internal configuration accordingly.
// IMPORTANT: This method is designed ONLY to be called as a callback from srvtopo.WatchSrvKeyspace.
// It relies on the resilient watcher's auto-retry behavior (see go/vt/srvtopo/watch.go) and should not be called directly from other contexts.
// Return value contract (required by WatchSrvKeyspace):
//   - true: Continue watching (resilient watcher will auto-retry on transient errors)
//   - false: Stop watching permanently (for fatal errors like NoNode, context canceled, or Interrupted)
func (s *TabletThrottlerStrategy) HandleConfigUpdate(srvks *topodatapb.SrvKeyspace, err error) bool {
	// Handle topology errors using a hybrid approach:
	// - Permanent errors (NoNode, context canceled): stop watching (return false)
	// - Transient errors (network issues, etc.): keep watching (return true, auto-retry will reconnect)
	if err != nil {
		// Keyspace deleted from topology - stop watching
		if topo.IsErrType(err, topo.NoNode) {
			log.Warningf("tabletThrottler.HandleConfigUpdate: keyspace %s deleted or not found, stopping watch", s.keyspace)
			return false
		}

		// Context canceled or interrupted - graceful shutdown, stop watching
		if errors.Is(err, context.Canceled) || topo.IsErrType(err, topo.Interrupted) {
			log.Infof("tabletThrottler.HandleConfigUpdate: watch stopped (context canceled or interrupted)")
			return false
		}

		// Transient error (network, temporary topo server issue) - keep watching
		// The resilient watcher will automatically retry as defined in go/vt/srvtopo/resilient_server.go:46
		log.Warningf("tabletThrottler.HandleConfigUpdate: transient topo watch error (will retry): %v", err)
		return true
	}

	newCfg := srvks.GetQueryThrottlerConfig().GetTabletStrategyConfig()
	if newCfg == nil {
		log.Errorf("tabletThrottler.HandleConfigUpdate: TabletStrategyConfig is nil for keyspace=%s, ignoring config update", s.keyspace)
		return true
	}

	// Check if config actually changed before updating.
	// We use reflect.DeepEqual here instead of manual field comparison because:
	// 1. TabletStrategyConfig has 3-level nested maps (TabletRules -> StatementRuleSet -> MetricRuleSet)
	// 2. Manual comparison would require ~50 lines of nested loops and is error-prone
	// 3. Performance is not critical here - this runs in watch callback (~1/minute), not in query hot path
	// 4. reflect.DeepEqual overhead (~1-2 microseconds) is negligible for async callback
	currentCfg := s.config.Load()
	if reflect.DeepEqual(currentCfg, newCfg) {
		return true
	}

	// Using atomic.Pointer ensures race-free reads in Evaluate() without needing locks
	s.config.Store(newCfg)

	log.Infof("tabletThrottler.HandleConfigUpdate: config updated for keyspace=%s", s.keyspace)
	return true
}
