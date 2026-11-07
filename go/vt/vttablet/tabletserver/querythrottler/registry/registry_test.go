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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/vterrors"

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// reset clears all registered factories. This is used for testing.
func reset() {
	mu.Lock()
	defer mu.Unlock()
	factories = make(map[querythrottlerpb.ThrottlingStrategy]StrategyFactory)
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
	Register(querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, testFactory)

	factory, exists := Get(querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER)
	require.True(t, exists)
	require.Equal(t, testFactory, factory)
}

func TestRegisterDuplicate(t *testing.T) {
	reset()

	testFactory := testStrategyFactory{}
	Register(querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, testFactory)

	// Should panic on duplicate registration
	require.Panics(t, func() {
		Register(querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, testFactory)
	})
}

func TestGetUnknown(t *testing.T) {
	reset()

	factory, exists := Get(querythrottlerpb.ThrottlingStrategy_UNKNOWN)
	require.False(t, exists)
	require.Nil(t, factory)
}

func TestCreateStrategy(t *testing.T) {
	reset()

	// Register a test factory
	Register(querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, testStrategyFactory{})

	cfg := &querythrottlerpb.Config{
		Enabled:  true,
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
	}
	deps := Deps{}

	strategy := CreateStrategy(cfg, deps)
	require.IsType(t, &NoOpStrategy{}, strategy)
}

func TestCreateStrategyUnknown(t *testing.T) {
	reset()

	cfg := &querythrottlerpb.Config{
		Enabled:  true,
		Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
	}
	deps := Deps{}

	strategy := CreateStrategy(cfg, deps)
	// Should fallback to NoOpStrategy
	require.IsType(t, &NoOpStrategy{}, strategy)
}

// failingStrategyFactory is a factory whose strategies fail to init.
type failingStrategyFactory struct{}

func (f failingStrategyFactory) New(deps Deps, cfg StrategyConfig) (ThrottlingStrategyHandler, error) {
	return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "no throttler client")
}

func TestCreateStrategyInitError(t *testing.T) {
	reset()

	Register(querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, failingStrategyFactory{})
	cfg := &querythrottlerpb.Config{
		Enabled:  true,
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
	}

	logs := logtest.Capture(t, slog.LevelWarn)
	strategy := CreateStrategy(cfg, Deps{})
	require.IsType(t, &NoOpStrategy{}, strategy)

	record := logs.Record(t, "Strategy failed to init, using NoOp")
	assert.Equal(t, "TABLET_THROTTLER", record["strategy"])
	assert.Equal(t, "no throttler client", record["error"])
	assert.Equal(t, "FAILED_PRECONDITION", record["error_code"])
}

func TestListRegistered(t *testing.T) {
	reset()

	// Initially empty
	strategies := ListRegistered()
	require.Empty(t, strategies)

	// Register strategies using proto enum values
	Register(querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, testStrategyFactory{})

	strategies = ListRegistered()
	require.Len(t, strategies, 1)
	require.Contains(t, strategies, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER)
}
