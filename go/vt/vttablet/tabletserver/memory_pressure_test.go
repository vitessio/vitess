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
	"testing"

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

	err := tsv.VStream(ctx, &binlogdatapb.VStreamRequest{
		Target: &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY},
	}, func([]*binlogdatapb.VEvent) error {
		return nil
	})
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.ErrorContains(t, err, "memory pressure")
}
