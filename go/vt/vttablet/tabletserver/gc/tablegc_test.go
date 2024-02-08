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

package gc

import (
	"context"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/schema"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextTableToPurge(t *testing.T) {
	tt := []struct {
		name   string
		tables []string
		next   string
		ok     bool
	}{
		{
			name:   "empty",
			tables: []string{},
			ok:     false,
		},
		{
			name: "first",
			tables: []string{
				"_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
				"_vt_PURGE_2ace8bcef73211ea87e9f875a4d24e90_20200915120411",
				"_vt_PURGE_3ace8bcef73211ea87e9f875a4d24e90_20200915120412",
				"_vt_PURGE_4ace8bcef73211ea87e9f875a4d24e90_20200915120413",
			},
			next: "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			ok:   true,
		},
		{
			name: "mid",
			tables: []string{
				"_vt_PURGE_2ace8bcef73211ea87e9f875a4d24e90_20200915120411",
				"_vt_PURGE_3ace8bcef73211ea87e9f875a4d24e90_20200915120412",
				"_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
				"_vt_PURGE_4ace8bcef73211ea87e9f875a4d24e90_20200915120413",
			},
			next: "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			ok:   true,
		},
		{
			name: "none",
			tables: []string{
				"_vt_HOLD_2ace8bcef73211ea87e9f875a4d24e90_20200915120411",
				"_vt_EVAC_3ace8bcef73211ea87e9f875a4d24e90_20200915120412",
				"_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
				"_vt_DROP_4ace8bcef73211ea87e9f875a4d24e90_20200915120413",
			},
			next: "",
			ok:   false,
		},
		{
			name: "first, new format",
			tables: []string{
				"_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
				"_vt_prg_2ace8bcef73211ea87e9f875a4d24e90_20200915120411_",
				"_vt_prg_3ace8bcef73211ea87e9f875a4d24e90_20200915120412_",
				"_vt_prg_4ace8bcef73211ea87e9f875a4d24e90_20200915120413_",
			},
			next: "_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			ok:   true,
		},
		{
			name: "mid, new format",
			tables: []string{
				"_vt_prg_2ace8bcef73211ea87e9f875a4d24e90_20200915120411_",
				"_vt_prg_3ace8bcef73211ea87e9f875a4d24e90_20200915120412_",
				"_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
				"_vt_prg_4ace8bcef73211ea87e9f875a4d24e90_20200915120413_",
			},
			next: "_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			ok:   true,
		},
		{
			name: "none, new format",
			tables: []string{
				"_vt_hld_2ace8bcef73211ea87e9f875a4d24e90_20200915120411_",
				"_vt_evc_3ace8bcef73211ea87e9f875a4d24e90_20200915120412_",
				"_vt_evc_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
				"_vt_drp_4ace8bcef73211ea87e9f875a4d24e90_20200915120413_",
				"_vt_prg_4ace8bcef73211ea87e9f875a4d24e90_20200915999999_",
			},
			next: "",
			ok:   false,
		},
	}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			collector := &TableGC{
				purgingTables:    make(map[string]bool),
				checkRequestChan: make(chan bool),
			}
			var err error
			collector.lifecycleStates, err = schema.ParseGCLifecycle("hold,purge,evac,drop")
			assert.NoError(t, err)
			for _, table := range ts.tables {
				collector.addPurgingTable(table)
			}

			next, ok := collector.nextTableToPurge()
			assert.Equal(t, ts.ok, ok)
			if ok {
				assert.Equal(t, ts.next, next)
			}
		})
	}
}

func TestNextState(t *testing.T) {
	tt := []struct {
		lifecycle string
		state     schema.TableGCState
		next      schema.TableGCState
	}{
		{
			lifecycle: "hold,purge,evac,drop",
			state:     schema.HoldTableGCState,
			next:      schema.PurgeTableGCState,
		},
		{
			lifecycle: "hold,purge,evac,drop",
			state:     schema.PurgeTableGCState,
			next:      schema.EvacTableGCState,
		},
		{
			lifecycle: "hold,purge,evac,drop",
			state:     schema.EvacTableGCState,
			next:      schema.DropTableGCState,
		},
		{
			lifecycle: "hold,purge,evac",
			state:     schema.EvacTableGCState,
			next:      schema.DropTableGCState,
		},
		{
			lifecycle: "hold,purge",
			state:     schema.HoldTableGCState,
			next:      schema.PurgeTableGCState,
		},
		{
			lifecycle: "hold,purge",
			state:     schema.PurgeTableGCState,
			next:      schema.DropTableGCState,
		},
		{
			lifecycle: "hold",
			state:     schema.HoldTableGCState,
			next:      schema.DropTableGCState,
		},
		{
			lifecycle: "evac,drop",
			state:     schema.HoldTableGCState,
			next:      schema.EvacTableGCState,
		},
		{
			lifecycle: "evac,drop",
			state:     schema.EvacTableGCState,
			next:      schema.DropTableGCState,
		},
		{
			lifecycle: "drop",
			state:     schema.HoldTableGCState,
			next:      schema.DropTableGCState,
		},
		{
			lifecycle: "drop",
			state:     schema.EvacTableGCState,
			next:      schema.DropTableGCState,
		},
		{
			lifecycle: "",
			state:     schema.HoldTableGCState,
			next:      schema.DropTableGCState,
		},
	}
	for _, ts := range tt {
		collector := &TableGC{}
		var err error
		collector.lifecycleStates, err = schema.ParseGCLifecycle(ts.lifecycle)
		assert.NoError(t, err)
		next := collector.nextState(ts.state)
		assert.NotNil(t, next)
		assert.Equal(t, ts.next, *next)

		postDrop := collector.nextState(schema.DropTableGCState)
		assert.Nil(t, postDrop)
	}
}

func TestShouldTransitionTable(t *testing.T) {
	tt := []struct {
		name             string
		table            string
		state            schema.TableGCState
		handledStates    string
		uuid             string
		shouldTransition bool
		isError          bool
	}{
		{
			name:             "purge, old timestamp",
			table:            "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			state:            schema.PurgeTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: true,
		},
		{
			name:             "purge, old timestamp, new format",
			table:            "_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			state:            schema.PurgeTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: true,
		},
		{
			name:             "no purge, future timestamp",
			table:            "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            schema.PurgeTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: false,
		},
		{
			name:             "no purge, future timestamp, new format",
			table:            "_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_29990915120410_",
			state:            schema.PurgeTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: false,
		},
		{
			name:             "no purge, PURGE not handled state",
			table:            "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            schema.PurgeTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			handledStates:    "hold,evac", // no PURGE
			shouldTransition: true,
		},
		{
			name:             "no purge, PURGE not handled state, new format",
			table:            "_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_29990915120410_",
			state:            schema.PurgeTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			handledStates:    "hold,evac", // no PURGE
			shouldTransition: true,
		},
		{
			name:             "no drop, future timestamp",
			table:            "_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            schema.DropTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: false,
		},
		{
			name:             "no drop, future timestamp, new format",
			table:            "_vt_drp_6ace8bcef73211ea87e9f875a4d24e90_29990915120410_",
			state:            schema.DropTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: false,
		},
		{
			name:             "drop, old timestamp",
			table:            "_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20090915120410",
			state:            schema.DropTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: true,
		},
		{
			name:             "drop, old timestamp, new format",
			table:            "_vt_drp_6ace8bcef73211ea87e9f875a4d24e90_20090915120410_",
			state:            schema.DropTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: true,
		},
		{
			name:             "no evac, future timestamp",
			table:            "_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            schema.EvacTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: false,
		},
		{
			name:             "no evac, future timestamp, new format",
			table:            "_vt_evc_6ace8bcef73211ea87e9f875a4d24e90_29990915120410_",
			state:            schema.EvacTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: false,
		},
		{
			name:             "no hold, HOLD not handled state",
			table:            "_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            schema.HoldTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: true,
		},
		{
			name:             "no hold, HOLD not handled state, new format",
			table:            "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_29990915120410_",
			state:            schema.HoldTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: true,
		},
		{
			name:             "hold, future timestamp",
			table:            "_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            schema.HoldTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			handledStates:    "hold,purge,evac,drop",
			shouldTransition: false,
		},
		{
			name:             "hold, future timestamp, new format",
			table:            "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_29990915120410_",
			state:            schema.HoldTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			handledStates:    "hold,purge,evac,drop",
			shouldTransition: false,
		},
		{
			name:             "not a GC table",
			table:            "_vt_SOMETHING_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            "",
			uuid:             "",
			shouldTransition: false,
		},
		{
			name:             "invalid new format",
			table:            "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_29990915999999_",
			state:            "",
			uuid:             "",
			shouldTransition: false,
		},
	}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			if ts.handledStates == "" {
				ts.handledStates = "purge,evac,drop"
			}
			lifecycleStates, err := schema.ParseGCLifecycle(ts.handledStates)
			assert.NoError(t, err)
			collector := &TableGC{
				lifecycleStates: lifecycleStates,
			}

			shouldTransition, state, uuid, err := collector.shouldTransitionTable(ts.table)
			if ts.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, ts.shouldTransition, shouldTransition)
				assert.Equal(t, ts.state, state)
				assert.Equal(t, ts.uuid, uuid)
			}
		})
	}
}

func TestCheckTables(t *testing.T) {
	collector := &TableGC{
		isOpen:           0,
		purgingTables:    map[string]bool{},
		checkRequestChan: make(chan bool),
	}
	var err error
	collector.lifecycleStates, err = schema.ParseGCLifecycle("hold,purge,evac,drop")
	require.NoError(t, err)

	gcTables := []*gcTable{
		{
			tableName:   "_vt_something_that_isnt_a_gc_table",
			isBaseTable: true,
		},
		{
			tableName:   "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_29990915999999_",
			isBaseTable: true,
		},
		{
			tableName:   "_vt_HOLD_11111111111111111111111111111111_20990920093324", // 2099 is in the far future
			isBaseTable: true,
		},
		{
			tableName:   "_vt_hld_11111111111111111111111111111111_20990920093324_", // 2099 is in the far future
			isBaseTable: true,
		},
		{
			tableName:   "_vt_HOLD_22222222222222222222222222222222_20200920093324",
			isBaseTable: true,
		},
		{
			tableName:   "_vt_hld_22222222222222222222222222222222_20200920093324_",
			isBaseTable: true,
		},
		{
			tableName:   "_vt_DROP_33333333333333333333333333333333_20200919083451",
			isBaseTable: true,
		},
		{
			tableName:   "_vt_drp_33333333333333333333333333333333_20200919083451_",
			isBaseTable: true,
		},
		{
			tableName:   "_vt_DROP_44444444444444444444444444444444_20200919083451",
			isBaseTable: false,
		},
		{
			tableName:   "_vt_drp_44444444444444444444444444444444_20200919083451_",
			isBaseTable: false,
		},
	}
	expectResponses := len(gcTables)
	// one gcTable above is irrelevant: it does not have a GC table name
	expectResponses = expectResponses - 1
	// one will not transition: its date is 2099
	expectResponses = expectResponses - 1
	// one gcTable above is irrelevant: it has an invalid new format timestamp
	expectResponses = expectResponses - 1
	// one will not transition: its date is 2099 in new format
	expectResponses = expectResponses - 1

	expectDropTables := []*gcTable{
		{
			tableName:   "_vt_DROP_33333333333333333333333333333333_20200919083451",
			isBaseTable: true,
		},
		{
			tableName:   "_vt_drp_33333333333333333333333333333333_20200919083451_",
			isBaseTable: true,
		},
		{
			tableName:   "_vt_DROP_44444444444444444444444444444444_20200919083451",
			isBaseTable: false,
		},
		{
			tableName:   "_vt_drp_44444444444444444444444444444444_20200919083451_",
			isBaseTable: false,
		},
	}
	expectTransitionRequests := []*transitionRequest{
		{
			fromTableName: "_vt_HOLD_22222222222222222222222222222222_20200920093324",
			isBaseTable:   true,
			toGCState:     schema.PurgeTableGCState,
			uuid:          "22222222222222222222222222222222",
		},
		{
			fromTableName: "_vt_hld_22222222222222222222222222222222_20200920093324_",
			isBaseTable:   true,
			toGCState:     schema.PurgeTableGCState,
			uuid:          "22222222222222222222222222222222",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	dropTablesChan := make(chan *gcTable)
	transitionRequestsChan := make(chan *transitionRequest)

	err = collector.checkTables(ctx, gcTables, dropTablesChan, transitionRequestsChan)
	assert.NoError(t, err)

	var responses int
	var foundDropTables []*gcTable
	var foundTransitionRequests []*transitionRequest
	for {
		if responses == expectResponses {
			break
		}
		select {
		case <-ctx.Done():
			assert.FailNow(t, "timeout")
			return
		case gcTable := <-dropTablesChan:
			responses++
			foundDropTables = append(foundDropTables, gcTable)
		case request := <-transitionRequestsChan:
			responses++
			foundTransitionRequests = append(foundTransitionRequests, request)
		}
	}
	assert.ElementsMatch(t, expectDropTables, foundDropTables)
	assert.ElementsMatch(t, expectTransitionRequests, foundTransitionRequests)
}
