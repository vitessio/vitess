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
	"fmt"
	"math/rand/v2"
	"sort"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

var (
	// Compile-time interface compliance check
	_ registry.ThrottlingStrategyHandler = (*TabletThrottlerStrategy)(nil)
	_ registry.StrategyFactory           = (*tabletThrottlerStrategyFactory)(nil)

	_metricsPrefix = "TabletThrottler"

	cacheMisses           = stats.NewCounter(_metricsPrefix+"CacheMisses", "incoming query throttler cache misses")
	cacheHits             = stats.NewCounter(_metricsPrefix+"CacheHits", "incoming query throttler cache hits")
	cacheRefreshFailures  = stats.NewCounter(_metricsPrefix+"CacheRefreshFailures", "background tablet throttler cache refreshes that did not produce a fresh state (e.g. timeouts)")
	cacheStaleRefreshes   = stats.NewCounter(_metricsPrefix+"CacheStaleRefreshes", "background refresh ticks that observed the cache already older than the staleness threshold (queries are silently failing open against stale state)")
	cacheStaleConsumption = stats.NewCounter(_metricsPrefix+"CacheStaleConsumption", "hot-path cache reads that discarded a state older than the staleness threshold and failed open (prevents indefinite throttling during a metrics outage)")
	decisionCount         = stats.NewCountersWithMultiLabels(_metricsPrefix+"DecisionCount", "tablet throttler decisions by outcome and reason", []string{"TabletType", "StmtType", "Path", "Outcome", "Reason"})
	fastDecisionLatency   = stats.NewMultiTimings(_metricsPrefix+"FastDecisionLatencyMicroseconds", "fast-path tablet throttler decision latency in microseconds", []string{"TabletType", "Outcome"})
	fullDecisionLatency   = stats.NewMultiTimings(_metricsPrefix+"FullDecisionLatencyMicroseconds", "full-path tablet throttler decision latency in microseconds", []string{"TabletType", "StmtType", "Outcome"})
	cacheLoadLatency      = stats.NewMultiTimings(_metricsPrefix+"CacheLoadLatencyMilliseconds", "tablet throttler cache load latency in milliseconds", []string{"Status"})
)

func init() {
	registry.Register(querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, &tabletThrottlerStrategyFactory{})
}

// tabletThrottlerStrategyFactory creates TabletThrottlerStrategy instances.
type tabletThrottlerStrategyFactory struct{}

func (f *tabletThrottlerStrategyFactory) New(deps registry.Deps, cfg *querythrottlerpb.Config) (registry.ThrottlingStrategyHandler, error) {
	tabletCfg := cfg.GetTabletStrategyConfig()
	if tabletCfg == nil {
		tabletCfg = &querythrottlerpb.TabletStrategyConfig{}
	}
	return NewTabletThrottlerStrategy(deps.ThrottleClient, tabletCfg, deps.TabletConfig), nil
}

// Configuration constants for caching behavior
const (
	// throttleCheckTimeout defines the timeout for individual throttle check calls
	throttleCheckTimeout = 5 * time.Second

	stmtTypeNotAvailable              = "NA"
	decisionPathFast                  = "fast"
	decisionPathFull                  = "full"
	decisionOutcomeAllowed            = "allowed"
	decisionOutcomeThrottled          = "throttled"
	decisionReasonBypassFast          = "bypass_fast"
	decisionReasonBypassPrior         = "bypass_priority"
	decisionReasonNoRuleForTabletType = "no_rule_for_tablet_type"
	decisionReasonNoRuleForStmtType   = "no_rule_for_stmt_type"
	decisionReasonNoMetricBreach      = "no_metric_breach"
	decisionReasonMetricBreach        = "metric_breach"
	decisionReasonQueryAllowed        = "query_allowed"

	defaultCacheUpdateInterval = 10 * time.Second
	// cacheStalenessMultiplier is how many refresh intervals may pass without a successful
	// refresh before the cached state counts as stale. See getCachedThrottleResult.
	cacheStalenessMultiplier = 6

	cacheRefreshStatusSuccess = "success"
	cacheRefreshStatusTimeout = "timeout"
)

// cacheState holds the immutable cached throttle check result state.
type cacheState struct {
	ok          bool
	result      *throttle.CheckResult
	refreshedAt time.Time
}

// TabletThrottlerStrategy uses the Vitess Tablet Throttler (https://vitess.io/docs/21.0/reference/features/tablet-throttler) to enforce throttling.
type TabletThrottlerStrategy struct {
	throttlerClient ThrottlerClientWrapper
	config          atomic.Pointer[querythrottlerpb.TabletStrategyConfig]
	tabletConfig    *tabletenv.TabletConfig

	// Caching field for throttle check results - single atomic for race-free access
	cachedState atomic.Pointer[cacheState]

	// Background updater lifecycle management
	ctx          context.Context
	cancel       context.CancelFunc
	updateTicker *time.Ticker
	done         chan struct{}
	running      atomic.Bool

	fastPathLatencySampleRate float64

	// cacheStalenessThreshold is the age past which a cached state is discarded.
	// Set once in the constructor from the resolved refresh interval; never mutated.
	cacheStalenessThreshold time.Duration

	// Injectable random functions for testing (defaults to math/rand/v2)
	randFloat64 func() float64
	randIntN    func(n int) int
}

// NewTabletThrottlerStrategy creates a new TabletThrottlerStrategy. The strategy does not
// watch SrvKeyspace itself; config reaches it only through UpdateConfig.
func NewTabletThrottlerStrategy(throttleClient ThrottlerClientWrapper, cfg *querythrottlerpb.TabletStrategyConfig, tabletConfig *tabletenv.TabletConfig) *TabletThrottlerStrategy {
	ctx, cancel := context.WithCancel(context.Background())

	strategy := &TabletThrottlerStrategy{
		throttlerClient:           throttleClient,
		tabletConfig:              tabletConfig,
		ctx:                       ctx,
		cancel:                    cancel,
		done:                      make(chan struct{}),
		fastPathLatencySampleRate: 0.1,
		cacheStalenessThreshold:   cacheStalenessMultiplier * resolveCacheUpdateInterval(tabletConfig),
		randFloat64:               rand.Float64,
		randIntN:                  rand.IntN,
	}
	// Normalize nil to empty: Evaluate reads cfg.TabletRules directly, which would panic
	// on a nil proto. Empty rules match nothing, so Evaluate falls through to "allow".
	if cfg == nil {
		cfg = &querythrottlerpb.TabletStrategyConfig{}
	}
	strategy.config.Store(cfg)

	return strategy
}

// Start spawns the background cache updater. Kept out of the constructor so a strategy
// that is built but never installed leaks nothing. Non-blocking: the first cache prime
// runs in the updater goroutine, since callers hold a lock across Start.
func (s *TabletThrottlerStrategy) Start() {
	if s.running.CompareAndSwap(false, true) {
		updateInterval := resolveCacheUpdateInterval(s.tabletConfig)
		s.updateTicker = time.NewTicker(updateInterval)
		go s.runCacheUpdater()

		log.Info("TabletThrottlerStrategy: started background throttle cache updater")
	}
}

// resolveCacheUpdateInterval returns the configured cache refresh interval, defaulting
// to defaultCacheUpdateInterval when the config value is non-positive.
func resolveCacheUpdateInterval(cfg *tabletenv.TabletConfig) time.Duration {
	if cfg == nil || cfg.TabletThrottlerCacheUpdateInterval <= 0 {
		return defaultCacheUpdateInterval
	}
	return cfg.TabletThrottlerCacheUpdateInterval
}

// Stop shuts down the background updater. It is terminal — the instance must not be
// restarted; QueryThrottler builds a fresh one on every strategy change. Safe to call
// when Start never ran, and safe to call twice.
func (s *TabletThrottlerStrategy) Stop() {
	// Unconditional, so any goroutine bound to s.ctx exits even if Start never ran.
	s.cancel()

	if s.running.CompareAndSwap(true, false) {
		if s.updateTicker != nil {
			s.updateTicker.Stop()
		}
		<-s.done // Wait for cache updater to finish
		log.Info("TabletThrottlerStrategy: stopped background throttle cache updater")
	}
}

// UpdateConfig applies a new config to this live strategy. QueryThrottler.HandleConfigUpdate
// calls it before swapping its snapshot, so the top-level and nested config land together.
// Safe to call before Start: it only swaps the atomic pointer Evaluate reads.
func (s *TabletThrottlerStrategy) UpdateConfig(cfg *querythrottlerpb.Config) {
	newTabletCfg := cfg.GetTabletStrategyConfig()
	if newTabletCfg == nil {
		// Normalize to empty; Evaluate would panic reading TabletRules off a nil proto.
		newTabletCfg = &querythrottlerpb.TabletStrategyConfig{}
	}

	// Skip the store when nothing changed.
	if proto.Equal(s.config.Load(), newTabletCfg) {
		return
	}
	s.config.Store(newTabletCfg)
}

// runCacheUpdater runs in a background goroutine to periodically refresh cached throttle results.
func (s *TabletThrottlerStrategy) runCacheUpdater() {
	defer close(s.done)
	defer s.updateTicker.Stop()

	// Prime immediately so the first ticker interval isn't served cold. Done here rather
	// than in Start to keep Start non-blocking; the hot path fails open until this lands.
	s.refreshCache()

	for {
		select {
		case <-s.updateTicker.C:
			s.refreshCache()
		case <-s.ctx.Done():
			return
		}
	}
}

// refreshCache updates the cached throttle check result.
//
// A refresh fails on a ctx error (DeadlineExceeded from our timeout, or Canceled from a
// racing Stop) or a nil checkResult — storing nil would later panic in Evaluate's metric
// loop. A failed refresh keeps the previous state and bumps CacheRefreshFailures, plus
// CacheStaleRefreshes if that state has already aged out. getCachedThrottleResult is what
// eventually discards it.
func (s *TabletThrottlerStrategy) refreshCache() {
	ctx, cancel := context.WithTimeout(s.ctx, throttleCheckTimeout)
	defer cancel()

	start := time.Now()
	checkResult, checkOk := s.throttlerClient.ThrottleCheckOK(ctx, throttlerapp.QueryThrottlerName)

	status := cacheRefreshStatusSuccess
	if ctx.Err() != nil || checkResult == nil {
		status = cacheRefreshStatusTimeout
		cacheRefreshFailures.Add(1)
		if s.isStale(s.cachedState.Load()) {
			cacheStaleRefreshes.Add(1)
		}
	} else {
		// Create new immutable state and store atomically
		state := &cacheState{
			ok:          checkOk,
			result:      checkResult,
			refreshedAt: time.Now(),
		}
		s.cachedState.Store(state)
	}

	cacheLoadLatency.Record([]string{status}, start)
}

// getCachedThrottleResult returns the cached throttle check result, failing open
// (checkOk=true) in three cases rather than calling the throttler synchronously, which
// would spike hot-path latency:
//
//   - the strategy is not running, or the cache is not primed yet — a brief window after Start
//   - the state has aged past cacheStalenessThreshold. refreshCache keeps the last state when
//     refreshes fail, so a metrics outage can freeze it at ok=false and throttle forever.
//
// The age check costs nothing on the healthy path, which the fast path already bypassed.
func (s *TabletThrottlerStrategy) getCachedThrottleResult() (*throttle.CheckResult, bool) {
	if !s.running.Load() {
		cacheMisses.Add(1)
		return nil, true
	}

	state := s.cachedState.Load()
	if state == nil {
		cacheMisses.Add(1)
		return nil, true
	}

	if s.isStale(state) {
		cacheStaleConsumption.Add(1)
		return nil, true
	}

	cacheHits.Add(1)
	return state.result, state.ok
}

// isStale reports whether the cached state is older than the configured staleness
// threshold. A nil state or zero refreshedAt is never stale (those are misses,
// handled separately).
func (s *TabletThrottlerStrategy) isStale(state *cacheState) bool {
	if state == nil || state.refreshedAt.IsZero() {
		return false
	}
	return time.Since(state.refreshedAt) > s.cacheStalenessThreshold
}

// Evaluate decides whether to throttle a query. Rules pair a tablet type and statement type
// with metric thresholds; when a threshold is breached, the query is throttled with the
// probability that threshold configures. transactionID is unused.
func (s *TabletThrottlerStrategy) Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, statementType sqlparser.StatementType, transactionID int64, attrs registry.QueryAttributes) registry.ThrottleDecision {
	// parsedQuery is plan.FullQuery, which Build() leaves nil for ALTER/REVERT MIGRATION
	// and partially-parsed DDL. Fail open there.
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
			s.recordFastDecision(tabletTypeStr, decisionOutcomeAllowed, startTime)
			return registry.ThrottleDecision{
				Throttle: false,
				Message:  "System healthy, fast-path bypass",
			}
		}
	}

	// Use pre-computed query attributes to avoid recomputation
	workloadName := attrs.WorkloadName
	priority := attrs.Priority
	// The caller resolves statementType from the AST, so CTE queries (WITH ... SELECT/DML)
	// match the same rules as plain ones. A textual scan would call them UNKNOWN.
	stmtType := statementType.String()

	// Step 1: Early priority-based throttling check
	// Similar to tx_throttler.go: lower priority values (higher priority) are less likely to be throttled
	// Priority behavior:
	//   - Priority 0 (highest): NEVER throttled (rand(0-99) < 0 is always false)
	//   - Priority 100 (lowest): ALWAYS checked for throttling (rand(0-99) < 100 is always true)
	//   - Priority 1-99: Probabilistically checked based on priority value
	// If priority check fails, skip all expensive throttle checks
	priorityCheck := s.randIntN(sqlparser.MaxPriorityValue) < priority
	if !priorityCheck {
		s.recordFullDecision(tabletTypeStr, stmtType, decisionOutcomeAllowed, decisionReasonBypassPrior, startTime)
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
		s.recordFullDecision(tabletTypeStr, stmtType, decisionOutcomeAllowed, decisionReasonNoRuleForTabletType, startTime)
		return registry.ThrottleDecision{
			Throttle: false,
			Message:  "No throttling rules for tablet type: " + targetTabletType.String(),
		}
	}

	// Step 3: Determine SQL statement type (e.g., INSERT, SELECT)
	// Step 4: Look up metric rules for this statement type
	metricRuleSet, ok := stmtRules.GetStatementRules()[stmtType]
	if !ok {
		s.recordFullDecision(tabletTypeStr, stmtType, decisionOutcomeAllowed, decisionReasonNoRuleForStmtType, startTime)
		return registry.ThrottleDecision{
			Throttle: false,
			Message:  "No throttling rules for SQL type: " + stmtType,
		}
	}

	// Step 5: Get cached throttle check results. May fail open with a nil checkResult.
	checkResult, checkOk := s.getCachedThrottleResult()
	// checkOk means the system is not overloaded. The nil check is defense-in-depth:
	// refreshCache never stores a nil result, but ranging over one below would panic.
	if checkOk || checkResult == nil {
		s.recordFullDecision(tabletTypeStr, stmtType, decisionOutcomeAllowed, decisionReasonNoMetricBreach, startTime)
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

		// Only act on metrics configured for this SQL type. CheckResult.Metrics is keyed by
		// the bare name ("lag") with the scope in result.Scope, but a config rule may use
		// either form, so try the bare key first and then the scoped one ("shard/lag").
		rule, found := metricRuleSet.GetMetricRules()[metricName]
		if !found {
			if scope, err := base.ScopeFromString(result.Scope); err == nil {
				rule, found = metricRuleSet.GetMetricRules()[base.MetricName(metricName).AggregatedName(scope)]
			}
		}
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
		s.recordFullDecision(tabletTypeStr, stmtType, decisionOutcomeThrottled, decisionReasonMetricBreach, startTime)
		return registry.ThrottleDecision{
			Throttle: true,
			Message: fmt.Sprintf("[VTTabletThrottler] query throttled: stmtType=%s workload=%s priority=%d metric=%s value=%."+
				"2f breached threshold=%.2f throttle=%.0f%%", stmtType, workloadName, priority, maxMetricName, maxMetricValue, maxBreachedThreshold,
				maxThrottleRatio*100),
			MetricName:         maxMetricName,
			MetricValue:        maxMetricValue,
			Threshold:          maxBreachedThreshold,
			ThrottlePercentage: maxThrottleRatio,
		}
	}
	// No throttle triggered → allow query
	s.recordFullDecision(tabletTypeStr, stmtType, decisionOutcomeAllowed, decisionReasonQueryAllowed, startTime)
	return registry.ThrottleDecision{
		Throttle: false,
		Message:  "No throttling conditions met",
	}
}

// GetStrategyName returns the name of the strategy.
func (s *TabletThrottlerStrategy) GetStrategyName() string {
	return querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER.String()
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
	decisionCount.Add([]string{tabletType, stmtTypeNotAvailable, decisionPathFast, outcome, decisionReasonBypassFast}, 1)

	if s.fastPathLatencySampleRate > 0 && s.randFloat64() < s.fastPathLatencySampleRate {
		fastDecisionLatency.Record([]string{tabletType, outcome}, start)
	}
}

func (s *TabletThrottlerStrategy) recordFullDecision(tabletType, stmtType, outcome, reason string, start time.Time) {
	decisionCount.Add([]string{tabletType, stmtType, decisionPathFull, outcome, reason}, 1)

	fullDecisionLatency.Record([]string{tabletType, stmtType, outcome}, start)
}
