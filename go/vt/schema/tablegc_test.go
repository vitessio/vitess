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
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGCStates(t *testing.T) {
	// These are all hard coded
	require.Equal(t, HoldTableGCState, gcStates["hld"])
	require.Equal(t, HoldTableGCState, gcStates["HOLD"])
	require.Equal(t, PurgeTableGCState, gcStates["prg"])
	require.Equal(t, PurgeTableGCState, gcStates["PURGE"])
	require.Equal(t, EvacTableGCState, gcStates["evc"])
	require.Equal(t, EvacTableGCState, gcStates["EVAC"])
	require.Equal(t, DropTableGCState, gcStates["drp"])
	require.Equal(t, DropTableGCState, gcStates["DROP"])
	_, ok := gcStates["purge"]
	require.False(t, ok)
	_, ok = gcStates["vrp"]
	require.False(t, ok)
	require.Equal(t, 2*4, len(gcStates)) // 4 states, 2 forms each
}

func TestIsGCTableName(t *testing.T) {
	tm := time.Now()
	states := []TableGCState{HoldTableGCState, PurgeTableGCState, EvacTableGCState, DropTableGCState}
	for _, state := range states {
		for i := 0; i < 10; i++ {
			tableName, err := generateGCTableNameOldFormat(state, "", tm)
			assert.NoError(t, err)
			assert.Truef(t, IsGCTableName(tableName), "table name: %s", tableName)

			tableName, err = generateGCTableName(state, "6ace8bcef73211ea87e9f875a4d24e90", tm)
			assert.NoError(t, err)
			assert.Truef(t, IsGCTableName(tableName), "table name: %s", tableName)

			tableName, err = GenerateGCTableName(state, tm)
			assert.NoError(t, err)
			assert.Truef(t, IsGCTableName(tableName), "table name: %s", tableName)
		}
	}
	t.Run("accept", func(t *testing.T) {
		names := []string{
			"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			"_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			"_vt_drp_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		}
		for _, tableName := range names {
			t.Run(tableName, func(t *testing.T) {
				assert.True(t, IsGCTableName(tableName))
			})
		}
	})
	t.Run("reject", func(t *testing.T) {
		names := []string{
			"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_202009151204100",
			"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410 ",
			"__vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			"_vt_DROP_6ace8bcef73211ea87e9f875a4d2_20200915120410",
			"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915",
			"_vt_OTHER_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			"_vt_OTHER_6ace8bcef73211ea87e9f875a4d24e90_zz20200915120410",
			"_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915999999",
			"_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915999999_",
		}
		for _, tableName := range names {
			t.Run(tableName, func(t *testing.T) {
				assert.False(t, IsGCTableName(tableName))
			})
		}
	})

	t.Run("explicit regexp", func(t *testing.T) {
		// NewGCTableNameExpression regexp is used externally by vreplication. Its a redundant form of
		// InternalTableNameExpression, but is nonetheless required. We verify it works correctly
		re := regexp.MustCompile(GCTableNameExpression)
		t.Run("accept", func(t *testing.T) {
			names := []string{
				"_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
				"_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
				"_vt_evc_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
				"_vt_drp_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			}
			for _, tableName := range names {
				t.Run(tableName, func(t *testing.T) {
					assert.True(t, IsGCTableName(tableName))
					assert.True(t, re.MatchString(tableName))
				})
			}
		})
		t.Run("reject", func(t *testing.T) {
			names := []string{
				"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
				"_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
				"_vt_vrp_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
				"_vt_gho_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			}
			for _, tableName := range names {
				t.Run(tableName, func(t *testing.T) {
					assert.False(t, re.MatchString(tableName))
				})
			}
		})
	})

}

func TestAnalyzeGCTableName(t *testing.T) {
	baseTime, err := time.Parse(time.RFC1123, "Tue, 15 Sep 2020 12:04:10 UTC")
	assert.NoError(t, err)
	tt := []struct {
		tableName string
		state     TableGCState
		t         time.Time
		isGC      bool
	}{
		{
			tableName: "_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			state:     DropTableGCState,
			t:         baseTime,
			isGC:      true,
		},
		{
			tableName: "_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			state:     HoldTableGCState,
			t:         baseTime,
			isGC:      true,
		},
		{
			tableName: "_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			state:     EvacTableGCState,
			t:         baseTime,
			isGC:      true,
		},
		{
			tableName: "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			state:     PurgeTableGCState,
			t:         baseTime,
			isGC:      true,
		},
		{
			tableName: "_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915999999", // time error
			isGC:      false,
		},
		{
			tableName: "_vt_drp_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			state:     DropTableGCState,
			t:         baseTime,
			isGC:      true,
		},
		{
			tableName: "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			state:     HoldTableGCState,
			t:         baseTime,
			isGC:      true,
		},
		{
			tableName: "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915999999_", // time error
			isGC:      false,
		},
		{
			tableName: "_vt_xyz_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			isGC:      false,
		},
	}
	for _, ts := range tt {
		t.Run(ts.tableName, func(t *testing.T) {
			isGC, state, uuid, tm, err := AnalyzeGCTableName(ts.tableName)
			assert.Equal(t, ts.isGC, isGC)
			if ts.isGC {
				assert.NoError(t, err)
				assert.True(t, isCondensedUUID(uuid))
				assert.Equal(t, ts.state, state)
				assert.Equal(t, ts.t, tm)
			}
		})
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

func TestGenerateRenameStatementWithUUID(t *testing.T) {
	uuid := "997342e3_e91d_11eb_aaae_0a43f95f28a3"
	tableName := "mytbl"
	countIterations := 5
	toTableNames := map[string]bool{}
	for i := 0; i < countIterations; i++ {
		_, toTableName, err := GenerateRenameStatementWithUUID(tableName, PurgeTableGCState, OnlineDDLToGCUUID(uuid), time.Now().Add(time.Duration(i)*time.Second).UTC())
		require.NoError(t, err)
		toTableNames[toTableName] = true
	}
	assert.Equal(t, countIterations, len(toTableNames))
}
