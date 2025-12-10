/*
Copyright 2024 The Vitess Authors.

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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func createTestTabletWithStartTime(cell string, startTime time.Time) *discovery.TabletHealth {
	th := createTestTablet(cell)
	th.TabletStartTime = startTime.Unix()
	return th
}

func TestWarmingBalancerAllOldTablets(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-1 * time.Hour) // 1 hour ago, well past warming period

	tablets := []*discovery.TabletHealth{
		createTestTabletWithStartTime("a", oldTime),
		createTestTabletWithStartTime("b", oldTime),
		createTestTabletWithStartTime("c", oldTime),
	}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	config := WarmingConfig{WarmingPeriod: 30 * time.Minute, WarmingTrafficPercent: 10}
	b := NewWarmingBalancer("a", []string{"a", "b", "c"}, config)

	// All picks should succeed (all old tablets, random distribution)
	const numPicks = 10000
	pickCounts := make(map[uint32]int)

	for i := 0; i < numPicks; i++ {
		th := b.Pick(target, tablets)
		require.NotNil(t, th, "Pick should not return nil")
		pickCounts[th.Tablet.Alias.Uid]++
	}

	// Should be roughly uniform distribution among the 3 old tablets
	expectedPerTablet := numPicks / len(tablets)
	for _, tablet := range tablets {
		count := pickCounts[tablet.Tablet.Alias.Uid]
		assert.InEpsilon(t, expectedPerTablet, count, 0.10,
			"tablet %d should receive roughly equal picks", tablet.Tablet.Alias.Uid)
	}
}

func TestWarmingBalancerAllNewTablets(t *testing.T) {
	now := time.Now()
	newTime := now.Add(-5 * time.Minute) // 5 minutes ago, within warming period

	tablets := []*discovery.TabletHealth{
		createTestTabletWithStartTime("a", newTime),
		createTestTabletWithStartTime("b", newTime),
	}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	config := WarmingConfig{WarmingPeriod: 30 * time.Minute, WarmingTrafficPercent: 10}
	b := NewWarmingBalancer("a", []string{"a", "b"}, config)

	// All picks should succeed - when all tablets are new, we serve normally
	const numPicks = 10000
	pickCounts := make(map[uint32]int)

	for i := 0; i < numPicks; i++ {
		th := b.Pick(target, tablets)
		require.NotNil(t, th, "Pick should not return nil")
		pickCounts[th.Tablet.Alias.Uid]++
	}

	// Should be roughly uniform distribution
	expectedPerTablet := numPicks / len(tablets)
	for _, tablet := range tablets {
		count := pickCounts[tablet.Tablet.Alias.Uid]
		assert.InEpsilon(t, expectedPerTablet, count, 0.10,
			"tablet %d should receive roughly equal picks in all-new mode", tablet.Tablet.Alias.Uid)
	}
}

func TestWarmingBalancerMixedTablets(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-1 * time.Hour)
	newTime := now.Add(-5 * time.Minute)

	oldTablet := createTestTabletWithStartTime("a", oldTime)
	newTablet := createTestTabletWithStartTime("b", newTime)
	tablets := []*discovery.TabletHealth{oldTablet, newTablet}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	config := WarmingConfig{WarmingPeriod: 30 * time.Minute, WarmingTrafficPercent: 10}
	b := NewWarmingBalancer("a", []string{"a", "b"}, config)

	oldPicks := 0
	newPicks := 0
	const N = 10000

	for i := 0; i < N; i++ {
		th := b.Pick(target, tablets)
		require.NotNil(t, th)
		if th.TabletStartTime == oldTablet.TabletStartTime {
			oldPicks++
		} else {
			newPicks++
		}
	}

	// With 10% warming traffic, expect ~90% old, ~10% new
	// Use 10% tolerance for statistical variance
	assert.InEpsilon(t, 0.90, float64(oldPicks)/float64(N), 0.10,
		"old tablet should receive ~90%% of traffic (got %.1f%%)", float64(oldPicks)/float64(N)*100)
	assert.InEpsilon(t, 0.10, float64(newPicks)/float64(N), 0.10,
		"new tablet should receive ~10%% of traffic (got %.1f%%)", float64(newPicks)/float64(N)*100)
}

func TestWarmingBalancerNonReplicaType(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-1 * time.Hour)
	newTime := now.Add(-5 * time.Minute)

	tablets := []*discovery.TabletHealth{
		createTestTabletWithStartTime("a", oldTime),
		createTestTabletWithStartTime("b", newTime),
	}

	// RDONLY target - warming should NOT apply
	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_RDONLY}
	config := WarmingConfig{WarmingPeriod: 30 * time.Minute, WarmingTrafficPercent: 10}
	b := NewWarmingBalancer("a", []string{"a", "b"}, config)

	pickCounts := make(map[uint32]int)
	const N = 10000

	for i := 0; i < N; i++ {
		th := b.Pick(target, tablets)
		require.NotNil(t, th)
		pickCounts[th.Tablet.Alias.Uid]++
	}

	// Should be ~50/50 random distribution (warming bypassed for RDONLY)
	for _, tablet := range tablets {
		count := pickCounts[tablet.Tablet.Alias.Uid]
		assert.InEpsilon(t, 0.50, float64(count)/float64(N), 0.10,
			"RDONLY should use random distribution")
	}
}

func TestWarmingBalancerZeroStartTime(t *testing.T) {
	now := time.Now()
	newTime := now.Add(-5 * time.Minute)

	oldTablet := createTestTablet("a") // No start time set (zero)
	newTablet := createTestTabletWithStartTime("b", newTime)
	tablets := []*discovery.TabletHealth{oldTablet, newTablet}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	config := WarmingConfig{WarmingPeriod: 30 * time.Minute, WarmingTrafficPercent: 10}
	b := NewWarmingBalancer("a", []string{"a", "b"}, config)

	oldPicks := 0
	const N = 10000

	for i := 0; i < N; i++ {
		th := b.Pick(target, tablets)
		require.NotNil(t, th)
		if th.TabletStartTime == 0 {
			oldPicks++
		}
	}

	// Zero start time tablet should be treated as old (~90% of traffic)
	assert.InEpsilon(t, 0.90, float64(oldPicks)/float64(N), 0.05,
		"zero start time tablet should be treated as old")
}

func TestWarmingBalancerPickEmpty(t *testing.T) {
	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	config := WarmingConfig{WarmingPeriod: 30 * time.Minute, WarmingTrafficPercent: 10}
	b := NewWarmingBalancer("a", []string{"a"}, config)

	th := b.Pick(target, []*discovery.TabletHealth{})
	assert.Nil(t, th, "Pick should return nil for empty tablet list")
}

func TestWarmingBalancerPickSingle(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-1 * time.Hour)

	tablets := []*discovery.TabletHealth{
		createTestTabletWithStartTime("a", oldTime),
	}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	config := WarmingConfig{WarmingPeriod: 30 * time.Minute, WarmingTrafficPercent: 10}
	b := NewWarmingBalancer("a", []string{"a"}, config)

	// Pick multiple times, should always return the same tablet
	for i := 0; i < 100; i++ {
		th := b.Pick(target, tablets)
		require.NotNil(t, th, "Pick should not return nil")
		assert.Equal(t, tablets[0].Tablet.Alias.Uid, th.Tablet.Alias.Uid,
			"Pick should return the only available tablet")
	}
}

func TestWarmingBalancerMultipleOldAndNew(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-1 * time.Hour)
	newTime := now.Add(-5 * time.Minute)

	// 2 old tablets and 2 new tablets
	tablets := []*discovery.TabletHealth{
		createTestTabletWithStartTime("a", oldTime),
		createTestTabletWithStartTime("b", oldTime),
		createTestTabletWithStartTime("c", newTime),
		createTestTabletWithStartTime("d", newTime),
	}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	config := WarmingConfig{WarmingPeriod: 30 * time.Minute, WarmingTrafficPercent: 10}
	b := NewWarmingBalancer("a", []string{"a", "b", "c", "d"}, config)

	oldPicks := 0
	newPicks := 0
	warmingCutoff := now.Add(-30 * time.Minute)
	const N = 10000

	for i := 0; i < N; i++ {
		th := b.Pick(target, tablets)
		require.NotNil(t, th)
		tabletStartTime := time.Unix(th.TabletStartTime, 0)
		if tabletStartTime.Before(warmingCutoff) {
			oldPicks++
		} else {
			newPicks++
		}
	}

	// With 10% warming traffic, expect ~90% to old pool, ~10% to new pool
	assert.InEpsilon(t, 0.90, float64(oldPicks)/float64(N), 0.05,
		"old tablets should receive ~90%% of traffic")
	assert.InEpsilon(t, 0.10, float64(newPicks)/float64(N), 0.05,
		"new tablets should receive ~10%% of traffic")
}

func TestWarmingBalancerFactoryError(t *testing.T) {
	// Test that the factory returns an error for warming mode
	// (warming mode requires custom configuration via NewWarmingBalancer)
	b, err := NewTabletBalancer(ModeWarming, "cell1", []string{})
	assert.Error(t, err)
	assert.Nil(t, b)
	assert.ErrorContains(t, err, "warming mode requires additional configuration")
}

func TestWarmingBalancerDefaultConfig(t *testing.T) {
	config := DefaultWarmingConfig()
	assert.Equal(t, 30*time.Minute, config.WarmingPeriod)
	assert.Equal(t, 10, config.WarmingTrafficPercent)
}
