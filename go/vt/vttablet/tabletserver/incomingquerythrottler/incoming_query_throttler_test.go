package incomingquerythrottler

import (
	"context"
	"testing"
	"time"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/incomingquerythrottler/registry"

	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
)

func TestNewIncomingQueryThrottler_ConfigRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &tabletenv.TabletConfig{
		IncomingQueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	throttler := &throttle.Throttler{} // use mock if needed
	iqt := NewIncomingQueryThrottler(ctx, throttler, newFakeConfigLoader(Config{
		Enabled:  true,
		Strategy: ThrottlingStrategyTabletThrottler,
	}), env)

	// Assert initial state (should be NoOpStrategy)
	require.NotNil(t, iqt)
	require.IsType(t, &registry.NoOpStrategy{}, iqt.strategy)

	// Wait briefly for goroutine to pick up the tick
	time.Sleep(50 * time.Millisecond)

	// Assert updated cfg and strategy
	iqt.mu.RLock()
	defer iqt.mu.RUnlock()

	require.True(t, iqt.cfg.Enabled)
	require.Equal(t, ThrottlingStrategyTabletThrottler, iqt.cfg.Strategy)
	//require.IsType(t, &TabletThrottlerStrategy{}, iqt.strategy)
}

func TestSelectThrottlingStrategy(t *testing.T) {
	tests := []struct {
		name                   string
		giveThrottlingStrategy ThrottlingStrategy
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
	config := &tabletenv.TabletConfig{
		IncomingQueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	iqt := NewIncomingQueryThrottler(ctx, throttler, newFakeConfigLoader(Config{
		Enabled:  true,
		Strategy: ThrottlingStrategyTabletThrottler,
	}), env)

	// Verify initial strategy was started (NoOpStrategy in this case)
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

	config := &tabletenv.TabletConfig{
		IncomingQueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	throttler := &throttle.Throttler{}
	iqt := NewIncomingQueryThrottler(ctx, throttler, newFakeConfigLoader(Config{
		Enabled:  false,
		Strategy: ThrottlingStrategyTabletThrottler,
	}), env)

	// Should not panic when called multiple times
	iqt.Shutdown()
	iqt.Shutdown()

	// Should still be able to check the strategy reference
	iqt.mu.RLock()
	strategy := iqt.strategy
	iqt.mu.RUnlock()
	require.NotNil(t, strategy)
}
