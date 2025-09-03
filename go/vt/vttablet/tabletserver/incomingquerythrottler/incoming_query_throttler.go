package incomingquerythrottler

import (
	"context"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
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
}

// NewIncomingQueryThrottler creates a new incoming query throttler.
func NewIncomingQueryThrottler(ctx context.Context, throttler *throttle.Throttler, cfgLoader ConfigLoader, env tabletenv.Env) *IncomingQueryThrottler {
	client := throttle.NewBackgroundClient(throttler, throttlerapp.IncomingQueryThrottlerName, base.UndefinedScope)

	i := &IncomingQueryThrottler{
		ctx:            ctx,
		throttleClient: client,
		tabletConfig:   env.Config(),
		cfg:            Config{},
		cfgLoader:      cfgLoader,
		strategy:       &registry.NoOpStrategy{}, // default strategy until config is loaded
	}

	// Start the initial strategy
	i.strategy.Start()

	// starting the loop which will be responsible for refreshing the config.
	i.startConfigRefreshLoop()

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
// Note: This method performs lock-free reads of config and strategy for optimal performance.
// Config updates are rare (default: every 1 minute) compared to query frequency,
// so the tiny risk of reading slightly stale data during config updates is acceptable
// for the significant performance improvement of avoiding mutex contention.
func (i *IncomingQueryThrottler) EnforceThrottlingIfNodeOverloaded(ctx context.Context, tabletType topodatapb.TabletType, sql string, transactionID int64, options *querypb.ExecuteOptions) error {
	// Lock-free read: for maximum performance in the hot path as cfg and strategy are updated rarely (default once per minute).
	// They are word-sized and safe for atomic reads; stale data for one query is acceptable and avoids mutex contention in the hot path.
	tCfg := i.cfg
	tStrategy := i.strategy

	if !tCfg.Enabled {
		return nil
	}

	// Evaluate the throttling decision
	decision := tStrategy.Evaluate(ctx, tabletType, sql, transactionID, options)

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

// startConfigRefreshLoop launches a background goroutine that refreshes the throttler's configuration
// at the interval specified by IncomingQueryThrottlerConfigRefreshInterval.
func (i *IncomingQueryThrottler) startConfigRefreshLoop() {
	go func() {
		refreshInterval := i.tabletConfig.IncomingQueryThrottlerConfigRefreshInterval
		configRefreshTicker := time.NewTicker(refreshInterval)
		defer configRefreshTicker.Stop()

		for {
			select {
			case <-i.ctx.Done():
				return
			case <-configRefreshTicker.C:
				newCfg, err := i.cfgLoader.Load(i.ctx)
				if err != nil {
					log.Errorf("Error loading config: %v", err)
					continue
				}

				// Only restart strategy if the strategy type has changed
				if i.cfg.Strategy != newCfg.Strategy {
					// Stop the current strategy before switching to a new one
					if i.strategy != nil {
						i.strategy.Stop()
					}

					newStrategy := selectThrottlingStrategy(newCfg, i.throttleClient, i.tabletConfig)
					// Update strategy and start the new one
					i.mu.Lock()
					i.strategy = newStrategy
					i.mu.Unlock()
					if i.strategy != nil {
						i.strategy.Start()
					}
				}

				// Always update the configuration
				i.mu.Lock()
				i.cfg = newCfg
				i.mu.Unlock()
			}
		}
	}()
}
