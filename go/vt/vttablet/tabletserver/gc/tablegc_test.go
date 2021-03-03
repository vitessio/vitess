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
	"testing"

	"vitess.io/vitess/go/vt/schema"

	"github.com/stretchr/testify/assert"
)

func TestNextTableToPurge(t *testing.T) {
	tt := []struct {
		tables []string
		next   string
		ok     bool
	}{
		{
			tables: []string{},
			ok:     false,
		},
		{
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
			tables: []string{
				"_vt_PURGE_2ace8bcef73211ea87e9f875a4d24e90_20200915120411",
				"_vt_PURGE_3ace8bcef73211ea87e9f875a4d24e90_20200915120412",
				"_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
				"_vt_PURGE_4ace8bcef73211ea87e9f875a4d24e90_20200915120413",
			},
			next: "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			ok:   true,
		},
	}
	for _, ts := range tt {
		collector := &TableGC{
			purgingTables: make(map[string]bool),
		}
		for _, table := range ts.tables {
			collector.purgingTables[table] = true
		}
		next, ok := collector.nextTableToPurge()
		assert.Equal(t, ts.ok, ok)
		if ok {
			assert.Equal(t, ts.next, next)
		}
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
		table            string
		state            schema.TableGCState
		uuid             string
		shouldTransition bool
		isError          bool
	}{
		{
			table:            "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			state:            schema.PurgeTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: true,
		},
		{
			table:            "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            schema.PurgeTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: false,
		},
		{
			table:            "_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            schema.DropTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: false,
		},
		{
			table:            "_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20090915120410",
			state:            schema.DropTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: true,
		},
		{
			table:            "_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            schema.EvacTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: false,
		},
		{
			table:            "_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            schema.HoldTableGCState,
			uuid:             "6ace8bcef73211ea87e9f875a4d24e90",
			shouldTransition: true,
		},
		{
			table:            "_vt_SOMETHING_6ace8bcef73211ea87e9f875a4d24e90_29990915120410",
			state:            "",
			uuid:             "",
			shouldTransition: false,
		},
	}
	lifecycleStates, err := schema.ParseGCLifecycle("purge,evac,drop")
	assert.NoError(t, err)
	collector := &TableGC{
		lifecycleStates: lifecycleStates,
	}
	for _, ts := range tt {
		shouldTransition, state, uuid, err := collector.shouldTransitionTable(ts.table)
		if ts.isError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, ts.shouldTransition, shouldTransition)
			assert.Equal(t, ts.state, state)
			assert.Equal(t, ts.uuid, uuid)
		}
	}
}
