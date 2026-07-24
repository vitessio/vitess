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

	_metricsPrefix = querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER.String()

	cacheMisses          = stats.NewCounter(_metricsPrefix+"CacheMisses", "incoming query throttler cache misses")
	cacheHits            = stats.NewCounter(_metricsPrefix+"CacheHits", "incoming query throttler cache hits")
	cacheRefreshFailures = stats.NewCounter(_metricsPrefix+"CacheRefreshFailures", "background tablet throttler cache refreshes that did not produce a fresh state (e.g. timeouts)")
	cacheStaleRefreshes  = stats.NewCounter(_metricsPrefix+"CacheStaleRefreshes", "background refresh ticks that observed the cache already older than the staleness threshold (queries are silently failing open against stale state)")
	decisionCount        = stats.NewCountersWithMultiLabels(_metricsPrefix+"DecisionCount", "tablet throttler decisions by outcome and reason", []string{"tablet_type", "stmt_type", "path", "outcome", "reason"})
	fastDecisionLatency  = stats.NewMultiTimings(_metricsPrefix+"FastDecisionLatencyMicroseconds", "fast-path tablet throttler decision latency in microseconds", []string{"tablet_type", "outcome"})
	fullDecisionLatency  = stats.NewMultiTimings(_metricsPrefix+"FullDecisionLatencyMicroseconds", "full-path tablet throttler decision latency in microseconds", []string{"tablet_type", "stmt_type", "outcome"})
	cacheLoadLatency     = stats.NewMultiTimings(_metricsPrefix+"CacheLoadLatencyMilliseconds", "tablet throttler cache load latency in milliseconds", []string{"status"})
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
	// cacheStalenessMultiplier defines how many refresh intervals must pass without a
	// successful refresh before a cached state is considered stale (for observability).
	// Stale states are still served (fail open) but increment CacheStaleConsumption.
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

	// cacheStalenessThreshold is the duration past which a cached state is treated as stale for observability purposes (CacheStaleConsumption counter).
	// Set once in the constructor from the resolved refresh interval; never mutated after.
	cacheStalenessThreshold time.Duration

	// Injectable random functions for testing (defaults to math/rand/v2)
	randFloat64 func() float64
	randIntN    func(n int) int
}

// NewTabletThrottlerStrategy creates a new TabletThrottlerStrategy.
//
// The strategy does not watch SrvKeyspace itself. Config updates flow exclusively
// through UpdateConfig, invoked by QueryThrottler.HandleConfigUpdate under the
// snapshot lock so the top-level and nested config are published atomically.
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
	// Normalize nil to an empty config so Evaluate() can safely read cfg.TabletRules
	// (direct field access on a nil proto would panic). An empty TabletRules map means
	// no rules match for any tablet type, so Evaluate falls through to "allow".
	if cfg == nil {
		cfg = &querythrottlerpb.TabletStrategyConfig{}
	}
	strategy.config.Store(cfg)

	return strategy
}

// Start begins the background throttle check updater.
// Keeping the spawn out of the constructor makes NewTabletThrottlerStrategy
// side-effect-free: a strategy that is built but never installed (e.g. because
// QueryThrottler.HandleConfigUpdate races with Shutdown and the strategy is
// discarded before the snapshot swap) cannot leak background goroutines.
//
// Start is non-blocking: the initial cache prime runs inside the updater goroutine
// (runCacheUpdater), not synchronously here, so Start never blocks on a throttle check.
// This matters because callers may hold a lock across Start — QueryThrottler.HandleConfigUpdate
// invokes it under qt.mu — and a synchronous prime (up to throttleCheckTimeout) would stall
// Shutdown and subsequent config callbacks behind that lock. The hot path fails open while
// the cache is unprimed.
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

// Stop stops the background throttle check updater and releases resources.
// Stop is terminal: this instance must not be restarted after Stop returns.
// The context and done channel are permanently canceled/closed. Strategy switches
// in QueryThrottler always create a fresh instance via NewTabletThrottlerStrategy.
//
// Stop is safe to call when Start was never invoked: s.cancel() runs unconditionally
// so any goroutine bound to s.ctx (now or added later) exits promptly, while the
// ticker/done-wait cleanup only runs when the cache updater was actually started.
func (s *TabletThrottlerStrategy) Stop() {
	// Cancel the strategy context unconditionally so any goroutine bound to s.ctx
	// (now or added later) exits even if Start was never called. cancel() is
	// idempotent, so repeated Stop calls are safe.
	s.cancel()

	if s.running.CompareAndSwap(true, false) {
		if s.updateTicker != nil {
			s.updateTicker.Stop()
		}
		<-s.done // Wait for cache updater to finish
		log.Info("TabletThrottlerStrategy: stopped background throttle cache updater")
	}
}

// UpdateConfig applies a new querythrottler config to this live strategy. It is
// invoked synchronously by QueryThrottler.HandleConfigUpdate before the snapshot
// is swapped, so the (top-level, nested) config pair becomes visible together.
// This replaces the strategy's prior independent SrvKeyspace watch and removes
// the race where a top-level Enabled flip could be published while the nested
// rules still held a stale value.
//
// UpdateConfig is safe to call before Start: it only mutates the atomic config
// pointer that Evaluate reads from on the hot path.
func (s *TabletThrottlerStrategy) UpdateConfig(cfg *querythrottlerpb.Config) {
	newTabletCfg := cfg.GetTabletStrategyConfig()
	if newTabletCfg == nil {
		// Normalize to an empty config so Evaluate() can safely read TabletRules
		// (direct field access on a nil proto would panic).
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

	// Prime the cache immediately so the first ticker interval isn't served with a
	// cold cache. Running it here rather than synchronously in Start keeps Start
	// non-blocking (callers may hold a lock across it); the hot path fails open until
	// this first refresh lands.
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

// refreshCache updates the cached throttle check results.
// On timeout/failure the previous cached state is intentionally preserved (fail open):
// a synchronous hot-path retry would only add latency on an already-degraded path. The
// CacheRefreshFailures counter tracks failed refreshes, and on each failed refresh we
// also check whether the existing cache has aged past cacheStalenessThreshold and bump
// CacheStaleRefreshes — so operators can alert on prolonged outages without per-query
// time arithmetic in the hot path.
//
// A failed refresh is any of: a derived-ctx error (DeadlineExceeded from our timeout
// OR Canceled from a Stop racing the in-flight call) OR a nil checkResult returned
// by the throttler client. Storing a nil result would later panic in Evaluate's
// metric loop, so we discard it here.
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

// getCachedThrottleResult returns the current cached throttle check result.
// If the cache is not yet primed (state is nil) or the strategy is not running,
// it fails open (returns checkOk=true) instead of making a synchronous throttler
// call. This avoids latency spikes in the query hot path during the brief
// cache-priming window after Start() or when Evaluate() is called outside the
// running lifecycle.
//
// Staleness is tracked by the background refresher (CacheStaleRefreshes counter),
// not here — keeping the hot path free of per-query time arithmetic.
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

// Evaluate determines whether a given SQL query should be throttled
// based on tablet type, SQL statement type, and real-time metrics.
//
// It follows a rule-based configuration where specific tablet types and
// SQL statement types are associated with metric thresholds. If a threshold
// is exceeded and a probabilistic throttle condition is met, the query should be throttled.
//
// Parameters:
//   - ctx: context for timeout/cancellation control.
//   - targetTabletType: the type of tablet the query is being run against (e.g., PRIMARY, REPLICA).
//   - sql: the raw SQL query string.
//   - transactionID: the ID of the transaction (not used in throttling logic).
//   - attrs: pre-computed query attributes containing workload name and priority.
//
// Returns:
//   - ThrottleDecision containing detailed information about the throttling decision.
func (s *TabletThrottlerStrategy) Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, statementType sqlparser.StatementType, transactionID int64, attrs registry.QueryAttributes) registry.ThrottleDecision {
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
	// statementType is resolved from the parsed AST by the caller (sqlparser.ASTToStatementType),
	// so CTE queries (WITH ... SELECT/DML) match the same rules as their non-CTE counterparts.
	// A textual scan (sqlparser.Preview) would classify "WITH ..." as UNKNOWN and fail open.
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

	// Step 5: Get cached throttle check results.
	// When the cache is not primed (state is nil) or the strategy is not running,
	// this returns checkOk=true (fail open) to avoid synchronous throttler calls
	// in the hot path. checkResult may be nil in that case, but the early return
	// on checkOk below skips the metrics evaluation that would dereference it.
	checkResult, checkOk := s.getCachedThrottleResult()
	// If check passes, system is not overloaded → allow query.
	// checkResult==nil is defense-in-depth: refreshCache must never store a nil
	// result, but if a future change regresses we fail open here rather than
	// panicking on the `for ... range checkResult.Metrics` loop below.
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

		// Only act on metrics explicitly configured for this SQL type.
		// CheckResult.Metrics is keyed by the bare metric name (the throttler disaggregates
		// scoped names) with the actual scope in result.Scope. Config rules may be keyed by
		// the bare name ("lag") or a scoped name ("shard/lag"), so try the bare key first,
		// then the reconstructed aggregated name.
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
