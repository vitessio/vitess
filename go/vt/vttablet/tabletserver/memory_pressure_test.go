/*
Copyright 2026 The Vitess Authors.

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

package tabletserver

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestMemoryPressureControllerHysteresis(t *testing.T) {
	cfg := tabletenv.MemoryPressureConfig{
		Enable:          true,
		SoftThreshold:   0.80,
		HardThreshold:   0.90,
		ResumeThreshold: 0.70,
	}
	usage := 0.25
	controller := newMemoryPressureController(&cfg, servenv.NewExporter("MemoryPressureControllerTest", "Tablet"), func() float64 {
		return usage
	})
	controller.refreshInterval = 0

	require.Equal(t, memoryPressureStateNormal, controller.state())
	require.NoError(t, controller.rejectIfAtLeast("Execute", memoryPressureStateHard))

	usage = 0.85
	err := controller.rejectIfAtLeast("VStream", memoryPressureStateSoft)
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.Equal(t, memoryPressureStateSoft, controller.state())

	usage = 0.75
	err = controller.rejectIfAtLeast("VStream", memoryPressureStateSoft)
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.Equal(t, memoryPressureStateSoft, controller.state())

	usage = 0.65
	require.NoError(t, controller.rejectIfAtLeast("Execute", memoryPressureStateHard))
	require.Equal(t, memoryPressureStateNormal, controller.state())

	usage = 0.95
	err = controller.rejectIfAtLeast("Execute", memoryPressureStateHard)
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.Equal(t, memoryPressureStateHard, controller.state())

	usage = 0.85
	err = controller.rejectIfAtLeast("Execute", memoryPressureStateHard)
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.Equal(t, memoryPressureStateHard, controller.state())

	usage = 0.65
	require.NoError(t, controller.rejectIfAtLeast("Execute", memoryPressureStateHard))
	require.Equal(t, memoryPressureStateNormal, controller.state())
}

func TestMemoryPressureControllerCachesUsageSamples(t *testing.T) {
	cfg := tabletenv.MemoryPressureConfig{
		Enable:          true,
		SoftThreshold:   0.80,
		HardThreshold:   0.90,
		ResumeThreshold: 0.70,
	}
	calls := 0
	controller := newMemoryPressureController(&cfg, nil, func() float64 {
		calls++
		return 0.65
	})
	controller.refreshInterval = time.Hour

	require.NoError(t, controller.rejectIfAtLeast("Execute", memoryPressureStateHard))

	require.NoError(t, controller.rejectIfAtLeast("Execute", memoryPressureStateHard))
	require.Equal(t, 1, calls)

	controller.refreshInterval = 0
	require.NoError(t, controller.rejectIfAtLeast("Execute", memoryPressureStateHard))
	require.Equal(t, 2, calls)
}

func TestMemoryPressureControllerRefreshesEveryRequestUnderPressure(t *testing.T) {
	cfg := tabletenv.MemoryPressureConfig{
		Enable:          true,
		SoftThreshold:   0.80,
		HardThreshold:   0.90,
		ResumeThreshold: 0.70,
	}
	calls := 0
	controller := newMemoryPressureController(&cfg, nil, func() float64 {
		calls++
		if calls == 1 {
			return 0.95
		}
		return 0.65
	})
	controller.refreshInterval = time.Hour

	err := controller.rejectIfAtLeast("Execute", memoryPressureStateHard)
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.Equal(t, memoryPressureStateHard, controller.state())

	require.NoError(t, controller.rejectIfAtLeast("Execute", memoryPressureStateHard))
	require.Equal(t, 2, calls)
	require.Equal(t, memoryPressureStateNormal, controller.state())
}

func TestMemoryPressureControllerObserveUsesCachedNormalStateWithoutLock(t *testing.T) {
	cfg := tabletenv.MemoryPressureConfig{
		Enable:          true,
		SoftThreshold:   0.80,
		HardThreshold:   0.90,
		ResumeThreshold: 0.70,
	}
	controller := newMemoryPressureController(&cfg, nil, func() float64 {
		return 0.95
	})
	controller.refreshInterval = time.Hour
	controller.lastState.Store(int32(memoryPressureStateNormal))
	controller.lastUsageBits.Store(math.Float64bits(0.65))
	controller.lastSampleUnixNano.Store(time.Now().UnixNano())

	controller.mu.Lock()
	defer controller.mu.Unlock()

	results := make(chan struct {
		state memoryPressureState
		usage float64
	}, 1)
	go func() {
		state, usage := controller.observe()
		results <- struct {
			state memoryPressureState
			usage float64
		}{
			state: state,
			usage: usage,
		}
	}()

	select {
	case result := <-results:
		require.Equal(t, memoryPressureStateNormal, result.state)
		require.Equal(t, 0.65, result.usage)
	case <-time.After(30 * time.Second):
		t.Fatal("observe blocked on the mutex despite a fresh cached normal-state sample")
	}
}

func TestMemoryPressureControllerAllowsTransactionFinalizationAtHardThreshold(t *testing.T) {
	cfg := tabletenv.MemoryPressureConfig{
		Enable:          true,
		SoftThreshold:   0.80,
		HardThreshold:   0.90,
		ResumeThreshold: 0.70,
	}
	controller := newMemoryPressureController(&cfg, nil, func() float64 {
		return 0.95
	})

	for _, requestName := range []string{
		"Commit",
		"Rollback",
		"Release",
		"CommitPrepared",
		"RollbackPrepared",
		"StartCommit",
		"SetRollback",
		"ConcludeTransaction",
	} {
		require.NoError(t, controller.reject(requestName), requestName)
	}

	err := controller.reject("Prepare")
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
}

func TestTabletServerMemoryPressureRejectsNewWorkAtHardThresholdButAllowsRelease(t *testing.T) {
	ctx := t.Context()
	cfg := tabletenv.NewDefaultConfig()
	cfg.MemoryPressure.Enable = true
	db, tsv := setupTabletServerTestCustom(t, ctx, cfg, "", vtenv.NewTestEnv())
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	usage := 0.25
	tsv.memoryPressure.getMemoryUsage = func() float64 {
		return usage
	}
	tsv.memoryPressure.refreshInterval = 0

	state, err := tsv.Begin(ctx, nil, &target, nil)
	require.NoError(t, err)

	usage = 0.95
	_, err = tsv.Begin(ctx, nil, &target, nil)
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.ErrorContains(t, err, "memory pressure")

	err = tsv.Release(ctx, &target, state.TransactionID, 0)
	require.NoError(t, err)
}

func TestTabletServerMemoryPressureRejectsVStreamAtSoftThreshold(t *testing.T) {
	ctx := t.Context()
	cfg := tabletenv.NewDefaultConfig()
	cfg.MemoryPressure.Enable = true
	db, tsv := setupTabletServerTestCustom(t, ctx, cfg, "", vtenv.NewTestEnv())
	defer tsv.StopService()
	defer db.Close()

	usage := 0.85
	tsv.memoryPressure.getMemoryUsage = func() float64 {
		return usage
	}
	tsv.memoryPressure.refreshInterval = 0

	err := tsv.VStream(ctx, &binlogdatapb.VStreamRequest{
		Target: &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY},
	}, func([]*binlogdatapb.VEvent) error {
		return nil
	})
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.ErrorContains(t, err, "memory pressure")
}

func TestTabletServerMemoryPressureRejectionsIncrementErrorCountersAndLogStats(t *testing.T) {
	ctx := t.Context()
	cfg := tabletenv.NewDefaultConfig()
	cfg.MemoryPressure.Enable = true
	db, tsv := setupTabletServerTestCustom(t, ctx, cfg, "", vtenv.NewTestEnv())
	defer tsv.StopService()
	defer db.Close()

	ch := tabletenv.StatsLogger.Subscribe("test memory pressure logging")
	defer tabletenv.StatsLogger.Unsubscribe(ch)

	usage := 0.95
	tsv.memoryPressure.getMemoryUsage = func() float64 {
		return usage
	}
	tsv.memoryPressure.refreshInterval = 0

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	initialResourceExhausted := tsv.stats.ErrorCounters.Counts()[vtrpcpb.Code_RESOURCE_EXHAUSTED.String()]
	_, err := tsv.Begin(ctx, nil, &target, nil)
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.EqualValues(t, initialResourceExhausted+1, tsv.stats.ErrorCounters.Counts()[vtrpcpb.Code_RESOURCE_EXHAUSTED.String()])

	select {
	case logStats := <-ch:
		require.Equal(t, "Begin", logStats.Method)
		require.Equal(t, "begin", logStats.OriginalSQL)
		require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(logStats.Error))
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for log stats after memory-pressure rejection")
	}
}
