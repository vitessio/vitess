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

	// All tablets in local cell "a"
	t1 := createTestTabletWithStartTime("a", oldTime)
	t2 := createTestTabletWithStartTime("a", oldTime)
	t2.Tablet.Alias.Uid = 2
	t3 := createTestTabletWithStartTime("a", oldTime)
	t3.Tablet.Alias.Uid = 3
	tablets := []*discovery.TabletHealth{t1, t2, t3}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	b := newWarmingBalancer("a", []string{"a"}, 30*time.Minute, 10)

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

	// All tablets in local cell "a"
	t1 := createTestTabletWithStartTime("a", newTime)
	t2 := createTestTabletWithStartTime("a", newTime)
	t2.Tablet.Alias.Uid = 2
	tablets := []*discovery.TabletHealth{t1, t2}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	b := newWarmingBalancer("a", []string{"a"}, 30*time.Minute, 10)

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

	// Both tablets in local cell "a"
	oldTablet := createTestTabletWithStartTime("a", oldTime)
	newTablet := createTestTabletWithStartTime("a", newTime)
	newTablet.Tablet.Alias.Uid = 2 // Different UID
	tablets := []*discovery.TabletHealth{oldTablet, newTablet}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	b := newWarmingBalancer("a", []string{"a"}, 30*time.Minute, 10)

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

func TestWarmingBalancerAppliesToAllTabletTypes(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-1 * time.Hour)
	newTime := now.Add(-5 * time.Minute)

	// Both tablets in local cell "a"
	oldTablet := createTestTabletWithStartTime("a", oldTime)
	newTablet := createTestTabletWithStartTime("a", newTime)
	newTablet.Tablet.Alias.Uid = 2 // Different UID
	tablets := []*discovery.TabletHealth{oldTablet, newTablet}

	// RDONLY target - warming SHOULD apply (same as REPLICA)
	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_RDONLY}
	b := newWarmingBalancer("a", []string{"a"}, 30*time.Minute, 10)

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

	// Should be ~90/10 distribution (warming applies to RDONLY too)
	assert.InEpsilon(t, 0.90, float64(oldPicks)/float64(N), 0.10,
		"old tablet should receive ~90%% of RDONLY traffic")
	assert.InEpsilon(t, 0.10, float64(newPicks)/float64(N), 0.10,
		"new tablet should receive ~10%% of RDONLY traffic")
}

func TestWarmingBalancerZeroStartTime(t *testing.T) {
	now := time.Now()
	newTime := now.Add(-5 * time.Minute)

	// Both tablets in local cell "a"
	oldTablet := createTestTablet("a") // No start time set (zero)
	newTablet := createTestTabletWithStartTime("a", newTime)
	newTablet.Tablet.Alias.Uid = 2 // Different UID
	tablets := []*discovery.TabletHealth{oldTablet, newTablet}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	b := newWarmingBalancer("a", []string{"a"}, 30*time.Minute, 10)

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
	b := newWarmingBalancer("a", []string{"a"}, 30*time.Minute, 10)

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
	b := newWarmingBalancer("a", []string{"a"}, 30*time.Minute, 10)

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

	// 2 old tablets and 2 new tablets, all in local cell "a"
	old1 := createTestTabletWithStartTime("a", oldTime)
	old2 := createTestTabletWithStartTime("a", oldTime)
	old2.Tablet.Alias.Uid = 2
	new1 := createTestTabletWithStartTime("a", newTime)
	new1.Tablet.Alias.Uid = 3
	new2 := createTestTabletWithStartTime("a", newTime)
	new2.Tablet.Alias.Uid = 4
	tablets := []*discovery.TabletHealth{old1, old2, new1, new2}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	b := newWarmingBalancer("a", []string{"a"}, 30*time.Minute, 10)

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
	assert.InEpsilon(t, 0.90, float64(oldPicks)/float64(N), 0.10,
		"old tablets should receive ~90%% of traffic")
	assert.InEpsilon(t, 0.10, float64(newPicks)/float64(N), 0.10,
		"new tablets should receive ~10%% of traffic")
}

func TestWarmingBalancerViaFactory(t *testing.T) {
	// Test that the factory correctly creates a warming balancer
	config := TabletBalancerConfig{
		Mode:                  ModeWarming,
		LocalCell:             "a",
		VTGateCells:           []string{"a"},
		WarmingPeriod:         30 * time.Minute,
		WarmingTrafficPercent: 10,
	}
	b, err := NewTabletBalancer(config)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	// Verify it works
	now := time.Now()
	oldTablet := createTestTabletWithStartTime("a", now.Add(-1*time.Hour))
	tablets := []*discovery.TabletHealth{oldTablet}
	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}

	th := b.Pick(target, tablets)
	assert.NotNil(t, th)
}

func TestDefaultTabletBalancerConfig(t *testing.T) {
	config := DefaultTabletBalancerConfig()
	assert.Equal(t, ModeCell, config.Mode)
	assert.Equal(t, 30*time.Minute, config.WarmingPeriod)
	assert.Equal(t, 10, config.WarmingTrafficPercent)
}

func TestWarmingBalancerPrefersLocalCell(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-1 * time.Hour)

	// Local cell tablet and remote cell tablet
	localTablet := createTestTabletWithStartTime("local", oldTime)
	remoteTablet := createTestTabletWithStartTime("remote", oldTime)
	remoteTablet.Tablet.Alias.Uid = 2
	tablets := []*discovery.TabletHealth{localTablet, remoteTablet}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	b := newWarmingBalancer("local", []string{"local", "remote"}, 30*time.Minute, 10)

	// All picks should go to local cell tablet
	for i := 0; i < 100; i++ {
		th := b.Pick(target, tablets)
		require.NotNil(t, th)
		assert.Equal(t, "local", th.Tablet.Alias.Cell, "Should only pick from local cell")
	}
}

func TestWarmingBalancerFallsBackToRemoteCell(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-1 * time.Hour)

	// Only remote cell tablets, no local cell tablets
	remote1 := createTestTabletWithStartTime("remote", oldTime)
	remote2 := createTestTabletWithStartTime("remote", oldTime)
	remote2.Tablet.Alias.Uid = 2
	tablets := []*discovery.TabletHealth{remote1, remote2}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	b := newWarmingBalancer("local", []string{"local", "remote"}, 30*time.Minute, 10)

	// Should fall back to remote tablets when local cell has none
	for i := 0; i < 100; i++ {
		th := b.Pick(target, tablets)
		require.NotNil(t, th, "Should fall back to remote cell tablets")
		assert.Equal(t, "remote", th.Tablet.Alias.Cell)
	}
}

func TestWarmingBalancerLocalCellWithWarming(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-1 * time.Hour)
	newTime := now.Add(-5 * time.Minute)

	// Local cell: 1 old + 1 new tablet
	// Remote cell: 1 old tablet (should be ignored)
	localOld := createTestTabletWithStartTime("local", oldTime)
	localNew := createTestTabletWithStartTime("local", newTime)
	localNew.Tablet.Alias.Uid = 2
	remoteOld := createTestTabletWithStartTime("remote", oldTime)
	remoteOld.Tablet.Alias.Uid = 3
	tablets := []*discovery.TabletHealth{localOld, localNew, remoteOld}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	b := newWarmingBalancer("local", []string{"local", "remote"}, 30*time.Minute, 10)

	localOldPicks := 0
	localNewPicks := 0
	remotePicks := 0
	const N = 10000

	for i := 0; i < N; i++ {
		th := b.Pick(target, tablets)
		require.NotNil(t, th)
		switch th.Tablet.Alias.Uid {
		case localOld.Tablet.Alias.Uid:
			localOldPicks++
		case localNew.Tablet.Alias.Uid:
			localNewPicks++
		case remoteOld.Tablet.Alias.Uid:
			remotePicks++
		}
	}

	// Remote tablet should never be picked
	assert.Equal(t, 0, remotePicks, "Remote tablet should never be picked when local cell has tablets")

	// Local tablets should follow warming distribution (~90/10)
	assert.InEpsilon(t, 0.90, float64(localOldPicks)/float64(N), 0.10,
		"Local old tablet should receive ~90%% of traffic")
	assert.InEpsilon(t, 0.10, float64(localNewPicks)/float64(N), 0.10,
		"Local new tablet should receive ~10%% of traffic")
}
