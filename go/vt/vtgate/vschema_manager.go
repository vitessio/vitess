/*
Copyright 2017 Google Inc.

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

package vtgate

import (
	"context"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// VSchemaManager is used to watch for updates to the vschema and to implement
// the DDL commands to add / remove vindexes
type VSchemaManager struct {
	e                 *Executor
	mu                sync.Mutex
	currentSrvVschema *vschemapb.SrvVSchema
}

// GetCurrentSrvVschema returns a copy of the latest SrvVschema from the
// topo watch
func (vm *VSchemaManager) GetCurrentSrvVschema() *vschemapb.SrvVSchema {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return proto.Clone(vm.currentSrvVschema).(*vschemapb.SrvVSchema)
}

// watchSrvVSchema watches the SrvVSchema from the topo. The function does
// not return an error. It instead logs warnings on failure.
// The SrvVSchema object is roll-up of all the Keyspace information,
// so when a keyspace is added or removed, it will be properly updated.
//
// This function will wait until the first value has either been processed
// or triggered an error before returning.
func (vm *VSchemaManager) watchSrvVSchema(ctx context.Context, cell string) {
	vm.e.serv.WatchSrvVSchema(ctx, cell, func(v *vschemapb.SrvVSchema, err error) {
		// Create a closure to save the vschema. If the value
		// passed is nil, it means we encountered an error and
		// we don't know the real value. In this case, we want
		// to use the previous value if it was set, or an
		// empty vschema if it wasn't.
		switch {
		case err == nil:
			// Good case, we can try to save that value.
		case topo.IsErrType(err, topo.NoNode):
			// If the SrvVschema disappears, we need to clear our record.
			// Otherwise, keep what we already had before.
			v = nil
		default:
			// Watch error, increment our counters.
			if vschemaCounters != nil {
				vschemaCounters.Add("WatchError", 1)
			}
		}

		// keep a copy of the latest SrvVschema
		vm.mu.Lock()
		vm.currentSrvVschema = v
		vm.mu.Unlock()

		// Transform the provided SrvVSchema into a VSchema.
		var vschema *vindexes.VSchema
		if v != nil {
			vschema, err = vindexes.BuildVSchema(v)
			if err != nil {
				log.Warningf("Error creating VSchema for cell %v (will try again next update): %v", cell, err)
				err = fmt.Errorf("error creating VSchema for cell %v: %v", cell, err)
				if vschemaCounters != nil {
					vschemaCounters.Add("Parsing", 1)
				}
			}
		}
		if v == nil {
			// We encountered an error, build an empty vschema.
			vschema, _ = vindexes.BuildVSchema(&vschemapb.SrvVSchema{})
		}

		// Build the display version. At this point, three cases:
		// - v is nil, vschema is empty, and err is set:
		//     1. when the watch returned an error.
		//     2. when BuildVSchema failed.
		// - v is set, vschema is full, and err is nil:
		//     3. when everything worked.
		errorMessage := ""
		if err != nil {
			errorMessage = err.Error()
		}
		stats := NewVSchemaStats(vschema, errorMessage)

		// save our value. if there was an error, then keep the
		// existing vschema instead of overwriting it.
		if v == nil && vm.e.vschema != nil {
			vschema = vm.e.vschema
		}

		vm.e.SaveVSchema(vschema, stats)
	})
}

// UpdateVSchema propagates the updated vschema to the topo. The entry for
// the given keyspace is updated in the global topo, and the full SrvVSchema
// is updated in all known cells.
func (vm *VSchemaManager) UpdateVSchema(ctx context.Context, ksName string, vschema *vschemapb.SrvVSchema) error {
	topoServer, err := vm.e.serv.GetTopoServer()
	if err != nil {
		return err
	}

	ks := vschema.Keyspaces[ksName]
	err = topoServer.SaveVSchema(ctx, ksName, ks)
	if err != nil {
		return err
	}

	cells, err := topoServer.GetKnownCells(ctx)
	if err != nil {
		return err
	}

	// even if one cell fails, continue to try the others
	for _, cell := range cells {
		cellErr := topoServer.UpdateSrvVSchema(ctx, cell, vschema)
		if cellErr != nil {
			err = cellErr
			log.Errorf("error updating vschema in cell %s: %v", cell, cellErr)
		}
	}

	return err
}
