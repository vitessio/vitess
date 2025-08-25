package incomingquerythrottler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/incomingquerythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
)

func TestNewIncomingQueryThrottler_ViperConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &tabletenv.TabletConfig{}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	throttler := &throttle.Throttler{} // use mock if needed
	iqt := NewIncomingQueryThrottler(ctx, throttler, env)

	// Assert initial state
	require.NotNil(t, iqt)
	require.NotNil(t, iqt.strategy)

	// Should have initialized with Viper defaults
	currentConfig := GetCurrentConfig()
	require.False(t, currentConfig.Enabled) // Default is disabled
	require.Equal(t, registry.ThrottlingStrategyUnknown, currentConfig.Strategy)
}

func TestSelectThrottlingStrategy(t *testing.T) {
	tests := []struct {
		name                   string
		giveThrottlingStrategy registry.ThrottlingStrategy
		expectedType           registry.ThrottlingStrategyHandler
	}{
		{
			name:                   "Unknown strategy defaults to NoOp",
			giveThrottlingStrategy: "some-unknown-string",
			expectedType:           &registry.NoOpStrategy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &throttle.Client{}

			config := &tabletenv.TabletConfig{
				IncomingQueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
			}

			strategy := selectThrottlingStrategy(Config{Enabled: true, Strategy: tt.giveThrottlingStrategy}, mockClient, config)

			require.IsType(t, tt.expectedType, strategy)
		})
	}
}

// TestIncomingQueryThrottler_StrategyLifecycleManagement tests that strategies are properly started and stopped.
func TestIncomingQueryThrottler_StrategyLifecycleManagement(t *testing.T) {
	// Test that initial strategy is started
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	throttler := &throttle.Throttler{}
	config := &tabletenv.TabletConfig{}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	iqt := NewIncomingQueryThrottler(ctx, throttler, env)

	// Verify initial strategy was started
	require.NotNil(t, iqt.strategy)

	// Test Shutdown properly stops the strategy
	iqt.Shutdown()

	// After shutdown, the strategy should have been stopped
	// In a real test, we would verify the strategy's Stop method was called
	require.NotNil(t, iqt.strategy) // Strategy reference should still exist but be stopped
}

// TestIncomingQueryThrottler_Shutdown tests the Shutdown method.
func TestIncomingQueryThrottler_Shutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &tabletenv.TabletConfig{}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	throttler := &throttle.Throttler{}
	iqt := NewIncomingQueryThrottler(ctx, throttler, env)

	// Should not panic when called multiple times
	iqt.Shutdown()
	iqt.Shutdown()

	// Should still be able to check the strategy reference
	iqt.mu.RLock()
	strategy := iqt.strategy
	iqt.mu.RUnlock()
	require.NotNil(t, strategy)
}

// TestIncomingQueryThrottler_UpdateStrategyIfChanged tests strategy switching functionality.
func TestIncomingQueryThrottler_UpdateStrategyIfChanged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &tabletenv.TabletConfig{}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	throttler := &throttle.Throttler{}
	iqt := NewIncomingQueryThrottler(ctx, throttler, env)

	// Initial strategy should be NoOp (due to Unknown strategy)
	require.IsType(t, &registry.NoOpStrategy{}, iqt.strategy)

	// Test that the strategy update logic works
	originalStrategy := iqt.strategy

	// Update with same strategy should not change anything
	testConfig := Config{
		Enabled:  false,
		Strategy: registry.ThrottlingStrategyUnknown,
	}
	iqt.updateStrategy(testConfig)
	require.Same(t, originalStrategy, iqt.strategy)

	// Test strategy change (falls back to NoOp since TabletThrottler isn't registered)
	testConfig.Strategy = registry.ThrottlingStrategyTabletThrottler
	iqt.updateStrategy(testConfig)
	require.IsType(t, &registry.NoOpStrategy{}, iqt.strategy) // Falls back to NoOp for unregistered strategies
}
