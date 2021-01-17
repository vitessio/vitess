/*
Copyright 2020 The Vitess Authors.

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

package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsGCTableName(t *testing.T) {
	tm := time.Now()
	states := []TableGCState{HoldTableGCState, PurgeTableGCState, EvacTableGCState, DropTableGCState}
	for _, state := range states {
		for i := 0; i < 10; i++ {
			tableName, err := generateGCTableName(state, "", tm)
			assert.NoError(t, err)
			assert.True(t, IsGCTableName(tableName))
		}
	}
	names := []string{
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_202009151204100",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410 ",
		"__vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d2_20200915120410",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915",
		"_vt_OTHER_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_OTHER_6ace8bcef73211ea87e9f875a4d24e90_zz20200915120410",
	}
	for _, tableName := range names {
		assert.False(t, IsGCTableName(tableName))
	}
}

func TestAnalyzeGCTableName(t *testing.T) {
	baseTime, err := time.Parse(time.RFC1123, "Tue, 15 Sep 2020 12:04:10 UTC")
	assert.NoError(t, err)
	tt := []struct {
		tableName string
		state     TableGCState
		t         time.Time
	}{
		{
			tableName: "_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			state:     DropTableGCState,
			t:         baseTime,
		},
		{
			tableName: "_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			state:     HoldTableGCState,
			t:         baseTime,
		},
		{
			tableName: "_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			state:     EvacTableGCState,
			t:         baseTime,
		},
		{
			tableName: "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			state:     PurgeTableGCState,
			t:         baseTime,
		},
	}
	for _, ts := range tt {
		isGC, state, uuid, tm, err := AnalyzeGCTableName(ts.tableName)
		assert.NoError(t, err)
		assert.True(t, isGC)
		assert.True(t, IsGCUUID(uuid))
		assert.Equal(t, ts.state, state)
		assert.Equal(t, ts.t, tm)
	}
}

func TestParseGCLifecycle(t *testing.T) {
	tt := []struct {
		lifecycle string
		states    map[TableGCState]bool
		expectErr bool
	}{
		{
			lifecycle: "",
			states: map[TableGCState]bool{
				DropTableGCState: true,
			},
		},
		{
			lifecycle: "drop",
			states: map[TableGCState]bool{
				DropTableGCState: true,
			},
		},
		{
			lifecycle: "   drop, ",
			states: map[TableGCState]bool{
				DropTableGCState: true,
			},
		},
		{
			lifecycle: "hold, drop",
			states: map[TableGCState]bool{
				HoldTableGCState: true,
				DropTableGCState: true,
			},
		},
		{
			lifecycle: "hold,purge, evac;drop",
			states: map[TableGCState]bool{
				HoldTableGCState:  true,
				PurgeTableGCState: true,
				EvacTableGCState:  true,
				DropTableGCState:  true,
			},
		},
		{
			lifecycle: "hold,purge,evac,drop",
			states: map[TableGCState]bool{
				HoldTableGCState:  true,
				PurgeTableGCState: true,
				EvacTableGCState:  true,
				DropTableGCState:  true,
			},
		},
		{
			lifecycle: "hold, other, drop",
			expectErr: true,
		},
	}
	for _, ts := range tt {
		t.Run(ts.lifecycle, func(*testing.T) {
			states, err := ParseGCLifecycle(ts.lifecycle)
			if ts.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, ts.states, states)
			}
		})
	}
}
