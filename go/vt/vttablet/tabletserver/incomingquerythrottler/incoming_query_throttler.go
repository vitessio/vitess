package incomingquerythrottler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/incomingquerythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

type IncomingQueryThrottler struct {
	throttleClient *throttle.Client
	tabletConfig   *tabletenv.TabletConfig

	// Cached enabled state for hot path performance
	enabledCache atomic.Bool

	// Cached current strategy for hot path performance
	mu       sync.RWMutex
	strategy registry.ThrottlingStrategyHandler

	// Current strategy name for change detection
	currentStrategyName registry.ThrottlingStrategy

	// Last config refresh time to avoid excessive Viper calls
	lastRefresh atomic.Value // stores time.Time
}

// NewIncomingQueryThrottler creates a new incoming query throttler.
func NewIncomingQueryThrottler(ctx context.Context, throttler *throttle.Throttler, env tabletenv.Env) *IncomingQueryThrottler {
	client := throttle.NewBackgroundClient(throttler, throttlerapp.IncomingQueryThrottlerName, base.UndefinedScope)

	// Initialize strategy based on current configuration
	currentConfig := GetCurrentConfig()
	strategy := selectThrottlingStrategy(currentConfig, client, env.Config())

	i := &IncomingQueryThrottler{
		throttleClient:      client,
		tabletConfig:        env.Config(),
		strategy:            strategy,
		currentStrategyName: currentConfig.Strategy,
	}

	// Set initial enabled cache
	i.enabledCache.Store(currentConfig.Enabled)

	// Initialize last refresh time
	i.lastRefresh.Store(time.Now())

	// Start the initial strategy
	i.strategy.Start()

	return i
}

// Shutdown gracefully stops the throttler and cleans up resources.
// This should be called when the IncomingQueryThrottler is no longer needed.
func (i *IncomingQueryThrottler) Shutdown() {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Stop the current strategy to clean up any background processes
	if i.strategy != nil {
		i.strategy.Stop()
	}
}

// EnforceThrottlingIfNodeOverloaded checks if the tablet is under heavy load
// and enforces throttling by rejecting the incoming request if necessary.
// Optimized for hot path performance with cached values.
func (i *IncomingQueryThrottler) EnforceThrottlingIfNodeOverloaded(ctx context.Context, tabletType topodatapb.TabletType, sql string, transactionID int64, options *querypb.ExecuteOptions) error {
	// Fast path: check cached enabled state (atomic read, no Viper call)
	if !i.enabledCache.Load() {
		return nil
	}

	// Periodically update cache and strategy (less frequent than every query)
	i.refreshConfigIfNeeded()

	// Lock-free strategy read - safe for hot path
	i.mu.RLock()
	currentStrategy := i.strategy
	i.mu.RUnlock()

	// Evaluate the throttling decision
	decision := currentStrategy.Evaluate(ctx, tabletType, sql, transactionID, options)

	// If no throttling is needed, allow the query
	if !decision.Throttle {
		return nil
	}

	// Normal throttling: return an error to reject the query
	return vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, decision.Message)
}

// selectThrottlingStrategy returns the appropriate strategy implementation based on the config.
func selectThrottlingStrategy(cfg Config, client *throttle.Client, tabletConfig *tabletenv.TabletConfig) registry.ThrottlingStrategyHandler {
	deps := registry.Deps{
		ThrottleClient: client,
		TabletConfig:   tabletConfig,
	}
	return registry.CreateStrategy(cfg, deps)
}

const configRefreshInterval = 5 * time.Second // Check config changes every 5 seconds

// refreshConfigIfNeeded periodically checks for configuration changes and updates caches.
// This avoids calling Viper on every query while still providing reasonable responsiveness.
func (i *IncomingQueryThrottler) refreshConfigIfNeeded() {
	now := time.Now()
	lastRefresh := i.lastRefresh.Load().(time.Time)

	// Only check for config changes every few seconds, not on every query
	if now.Sub(lastRefresh) < configRefreshInterval {
		return
	}

	// Get current config from Viper
	currentConfig := GetCurrentConfig()

	// Update enabled cache (atomic write)
	i.enabledCache.Store(currentConfig.Enabled)

	// Check if strategy changed
	if currentConfig.Strategy != i.currentStrategyName {
		i.updateStrategy(currentConfig)
	}

	// Update last refresh time
	i.lastRefresh.Store(now)
}

// updateStrategy switches to a new throttling strategy
func (i *IncomingQueryThrottler) updateStrategy(config Config) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Double-check strategy name to avoid race conditions
	if config.Strategy == i.currentStrategyName {
		return
	}

	// Stop the current strategy before switching
	if i.strategy != nil {
		i.strategy.Stop()
	}

	// Create and start the new strategy
	i.strategy = selectThrottlingStrategy(config, i.throttleClient, i.tabletConfig)
	i.currentStrategyName = config.Strategy

	if i.strategy != nil {
		i.strategy.Start()
	}
}
