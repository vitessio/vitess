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
	"sync"
	"time"

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
// This method uses RWMutex read locks to safely access qt.cfg and qt.strategy
// while allowing concurrent reads from multiple goroutines.
func (qt *QueryThrottler) Throttle(ctx context.Context, tabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, transactionID int64, options *querypb.ExecuteOptions) error {
	qt.mu.RLock()
	cfg := qt.cfg
	strategy := qt.strategy
	qt.mu.RUnlock()

	if !cfg.Enabled {
		return nil
	}

	// Evaluate the throttling decision
	decision := strategy.Evaluate(ctx, tabletType, parsedQuery, transactionID, options)

	// If no throttling is needed, allow the query
	if !decision.Throttle {
		return nil
	}

	// If dry-run mode is enabled, log the decision but don't throttle
	if cfg.DryRun {
		log.Warningf("[DRY-RUN] %s", decision.Message)
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
					log.Errorf("Error loading config: %v", err)
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
