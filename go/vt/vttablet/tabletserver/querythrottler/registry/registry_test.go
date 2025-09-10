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

package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// reset clears all registered factories. This is used for testing.
func reset() {
	mu.Lock()
	defer mu.Unlock()
	factories = make(map[ThrottlingStrategy]StrategyFactory)
}

// testStrategyFactory is a simple factory for testing.
type testStrategyFactory struct{}

// Compile-time interface compliance check
var _ StrategyFactory = (*testStrategyFactory)(nil)

func (f testStrategyFactory) New(deps Deps, cfg StrategyConfig) (ThrottlingStrategyHandler, error) {
	return &NoOpStrategy{}, nil
}

func TestRegister(t *testing.T) {
	reset()

	// Test successful registration
	testFactory := testStrategyFactory{}
	Register("test-strategy", testFactory)

	factory, exists := Get("test-strategy")
	require.True(t, exists)
	require.Equal(t, testFactory, factory)
}

func TestRegisterDuplicate(t *testing.T) {
	reset()

	testFactory := testStrategyFactory{}
	Register("test-strategy", testFactory)

	// Should panic on duplicate registration
	require.Panics(t, func() {
		Register("test-strategy", testFactory)
	})
}

func TestGetUnknown(t *testing.T) {
	reset()

	factory, exists := Get("unknown-strategy")
	require.False(t, exists)
	require.Nil(t, factory)
}

// testConfig implements the Config interface for testing
type testConfig struct {
	strategy ThrottlingStrategy
}

func (c testConfig) GetStrategy() ThrottlingStrategy {
	return c.strategy
}

func (c testConfig) GetTabletStrategyConfig() interface{} {
	return nil
}

func TestCreateStrategy(t *testing.T) {
	reset()

	// Register a test factory
	Register("test-strategy", testStrategyFactory{})

	cfg := testConfig{
		strategy: "test-strategy",
	}
	deps := Deps{}

	strategy := CreateStrategy(cfg, deps)
	require.IsType(t, &NoOpStrategy{}, strategy)
}

func TestCreateStrategyUnknown(t *testing.T) {
	reset()

	cfg := testConfig{
		strategy: "unknown-strategy",
	}
	deps := Deps{}

	strategy := CreateStrategy(cfg, deps)
	// Should fallback to NoOpStrategy
	require.IsType(t, &NoOpStrategy{}, strategy)
}

func TestListRegistered(t *testing.T) {
	reset()

	// Initially empty
	strategies := ListRegistered()
	require.Empty(t, strategies)

	// Register some strategies
	Register("strategy1", testStrategyFactory{})
	Register("strategy2", testStrategyFactory{})

	strategies = ListRegistered()
	require.Len(t, strategies, 2)
	require.Contains(t, strategies, ThrottlingStrategy("strategy1"))
	require.Contains(t, strategies, ThrottlingStrategy("strategy2"))
}
