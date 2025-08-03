package incomingquerythrottler

import (
	"context"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

var (
	_configRefreshTicket = time.NewTicker(1 * time.Minute)
)

type IncomingQueryThrottler struct {
	ctx            context.Context
	throttleClient *throttle.Client
	mu             sync.RWMutex
	// cfg holds the current configuration for the throttler.
	cfg Config
	// cfgLoader is responsible for loading the configuration.
	cfgLoader ConfigLoader
	// ThrottlingStrategyHandler is the strategy to use for throttling.
	strategy ThrottlingStrategyHandler
}

// NewIncomingQueryThrottler creates a new incoming query throttler.
func NewIncomingQueryThrottler(ctx context.Context, throttler *throttle.Throttler, cfgLoader ConfigLoader) *IncomingQueryThrottler {
	client := throttle.NewBackgroundClient(throttler, throttlerapp.IncomingQueryThrottlerName, base.UndefinedScope)

	i := &IncomingQueryThrottler{
		ctx:            ctx,
		throttleClient: client,
		cfg:            Config{},
		cfgLoader:      cfgLoader,
		strategy:       &NoOpStrategy{}, // default strategy until config is loaded
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
func (i *IncomingQueryThrottler) EnforceThrottlingIfNodeOverloaded(ctx context.Context, tabletType topodatapb.TabletType, sql string, transactionID int64, options *querypb.ExecuteOptions) error {
	i.mu.RLock()
	tCfg := i.cfg
	tStrategy := i.strategy
	i.mu.RUnlock()

	if !tCfg.Enabled {
		return nil
	}

	return tStrategy.ThrottleIfNeeded(ctx, tabletType, sql, transactionID, options)
}

// selectThrottlingStrategy returns the appropriate strategy implementation based on the config.
func selectThrottlingStrategy(cfg Config, client *throttle.Client) ThrottlingStrategyHandler {
	switch cfg.Strategy {
	case ThrottlingStrategyTabletThrottler:
		fallthrough // TODO (to be implemented in next PR)
	default:
		log.Warningf("Unknown throttling strategy: %v, defaulting to NoOpStrategy", cfg.Strategy)
		return &NoOpStrategy{}
	}
}

// startConfigRefreshLoop launches a background goroutine that refreshes the throttler's configuration every minute.
func (i *IncomingQueryThrottler) startConfigRefreshLoop() {
	go func() {
		defer _configRefreshTicket.Stop()

		for {
			select {
			case <-i.ctx.Done():
				return
			case <-_configRefreshTicket.C:
				i.mu.Lock()
				newCfg, err := i.cfgLoader.Load(i.ctx)
				if err != nil {
					log.Errorf("Error loading config: %v", err)
				}

				// Only restart strategy if the strategy type has changed
				newStrategy := selectThrottlingStrategy(newCfg, i.throttleClient)
				if i.cfg.Strategy != newCfg.Strategy {
					// Stop the current strategy before switching to a new one
					if i.strategy != nil {
						i.strategy.Stop()
					}

					// Update strategy and start the new one
					i.strategy = newStrategy
					if i.strategy != nil {
						i.strategy.Start()
					}
				}

				// Always update the configuration
				i.cfg = newCfg

				i.mu.Unlock()
			}
		}
	}()
}
