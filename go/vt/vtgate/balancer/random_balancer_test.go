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

package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestRandomBalancerUniformDistribution(t *testing.T) {
	// Test with uneven distribution of tablets across cells to verify
	// that random mode ignores cell affinity and treats all tablets equally
	tablets := []*discovery.TabletHealth{
		createTestTablet("cell1"),
		createTestTablet("cell1"),
		createTestTablet("cell1"),
		createTestTablet("cell2"),
		createTestTablet("cell3"),
		createTestTablet("cell3"),
	}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	// Use localCell = "cell1" to verify it doesn't get preferential treatment
	b := newRandomBalancer("cell1", []string{})

	const numPicks = 60000
	pickCounts := make(map[uint32]int)

	for i := 0; i < numPicks; i++ {
		th := b.Pick(target, tablets, PickOpts{})
		require.NotNil(t, th, "Pick should not return nil")
		pickCounts[th.Tablet.Alias.Uid]++
	}

	// Each individual tablet should be picked with 1/N probability
	expectedPerTablet := numPicks / len(tablets)
	for _, tablet := range tablets {
		count := pickCounts[tablet.Tablet.Alias.Uid]
		assert.InEpsilon(t, expectedPerTablet, count, 0.05,
			"tablet %d in cell %s should receive uniform picks (expected ~%d, got %d)",
			tablet.Tablet.Alias.Uid, tablet.Tablet.Alias.Cell, expectedPerTablet, count)
	}
}

func TestRandomBalancerPickEmpty(t *testing.T) {
	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	b := newRandomBalancer("cell1", []string{})

	th := b.Pick(target, []*discovery.TabletHealth{}, PickOpts{})
	assert.Nil(t, th, "Pick should return nil for empty tablet list")
}

func TestRandomBalancerPickSingle(t *testing.T) {
	tablets := []*discovery.TabletHealth{
		createTestTablet("cell1"),
	}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	b := newRandomBalancer("cell1", []string{})

	// Pick multiple times, should always return the same tablet
	for i := 0; i < 100; i++ {
		th := b.Pick(target, tablets, PickOpts{})
		require.NotNil(t, th, "Pick should not return nil")
		assert.Equal(t, tablets[0].Tablet.Alias.Uid, th.Tablet.Alias.Uid,
			"Pick should return the only available tablet")
	}
}

func TestRandomBalancerFactory(t *testing.T) {
	// Test that the factory creates a random balancer correctly
	b, err := NewTabletBalancer(ModeRandom, "cell1", []string{"cell1", "cell2"})
	require.NoError(t, err)
	require.NotNil(t, b)

	// Verify it's actually a randomBalancer
	_, ok := b.(*randomBalancer)
	assert.True(t, ok, "factory should create a randomBalancer")
}

func TestBalancerFactoryInvalidModes(t *testing.T) {
	// Test that "cell" mode returns an error (should be handled by gateway)
	b, err := NewTabletBalancer(ModeCell, "cell1", []string{})
	assert.Error(t, err)
	assert.Nil(t, b)
	assert.ErrorContains(t, err, "cell mode should be handled by the gateway")

	// Test that an invalid mode returns an error
	b, err = NewTabletBalancer(ModeInvalid, "cell1", []string{})
	assert.Error(t, err)
	assert.Nil(t, b)
	assert.ErrorContains(t, err, "unsupported balancer mode")
}

func TestRandomBalancerCellFiltering(t *testing.T) {
	// Create tablets in multiple cells
	tablets := []*discovery.TabletHealth{
		createTestTablet("cell1"),
		createTestTablet("cell1"),
		createTestTablet("cell2"),
		createTestTablet("cell2"),
		createTestTablet("cell3"),
		createTestTablet("cell3"),
	}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}

	// Create balancer that only considers cell1 and cell2
	b := newRandomBalancer("cell1", []string{"cell1", "cell2"})
	require.NotNil(t, b)

	const numPicks = 10000
	pickCounts := make(map[string]int)

	for i := 0; i < numPicks; i++ {
		th := b.Pick(target, tablets, PickOpts{})
		require.NotNil(t, th)
		pickCounts[th.Tablet.Alias.Cell]++
	}

	// Should only pick from cell1 and cell2, never cell3
	assert.Greater(t, pickCounts["cell1"], 0, "should pick from cell1")
	assert.Greater(t, pickCounts["cell2"], 0, "should pick from cell2")
	assert.Equal(t, 0, pickCounts["cell3"], "should never pick from cell3")

	// Each filtered cell should get approximately half the picks
	expectedPerCell := numPicks / 2
	assert.InEpsilon(t, expectedPerCell, pickCounts["cell1"], 0.1,
		"cell1 should get ~50%% of picks")
	assert.InEpsilon(t, expectedPerCell, pickCounts["cell2"], 0.1,
		"cell2 should get ~50%% of picks")
}
