/*
Copyright 2019 The Vitess Authors.

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
	"sync"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

var _ VSchemaOperator = (*VSchemaManager)(nil)

// VSchemaManager is used to watch for updates to the vschema and to implement
// the DDL commands to add / remove vindexes
type VSchemaManager struct {
	mu                sync.Mutex
	currentSrvVschema *vschemapb.SrvVSchema
	currentVschema    *vindexes.VSchema
	serv              srvtopo.Server
	cell              string
	subscriber        func(vschema *vindexes.VSchema, stats *VSchemaStats)
	schema            SchemaInfo
}

// SchemaInfo is an interface to schema tracker.
type SchemaInfo interface {
	Tables(ks string) map[string][]vindexes.Column
	Views(ks string) map[string]sqlparser.SelectStatement
}

// GetCurrentSrvVschema returns a copy of the latest SrvVschema from the
// topo watch
func (vm *VSchemaManager) GetCurrentSrvVschema() *vschemapb.SrvVSchema {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return proto.Clone(vm.currentSrvVschema).(*vschemapb.SrvVSchema)
}

// UpdateVSchema propagates the updated vschema to the topo. The entry for
// the given keyspace is updated in the global topo, and the full SrvVSchema
// is updated in all known cells.
func (vm *VSchemaManager) UpdateVSchema(ctx context.Context, ksName string, vschema *vschemapb.SrvVSchema) error {
	topoServer, err := vm.serv.GetTopoServer()
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
	if err != nil {
		return err
	}

	// Update all the local copy of VSchema if the topo update is successful.
	vm.VSchemaUpdate(vschema, err)

	return nil
}

// VSchemaUpdate builds the VSchema from SrvVschema and call subscribers.
func (vm *VSchemaManager) VSchemaUpdate(v *vschemapb.SrvVSchema, err error) bool {
	log.Infof("Received vschema update")
	switch {
	case err == nil:
		// Good case, we can try to save that value.
	case topo.IsErrType(err, topo.NoNode):
		// If the SrvVschema disappears, we need to clear our record.
		// Otherwise, keep what we already had before.
		v = nil
	default:
		log.Errorf("SrvVschema watch error: %v", err)
		// Watch error, increment our counters.
		if vschemaCounters != nil {
			vschemaCounters.Add("WatchError", 1)
		}
	}

	vm.mu.Lock()
	defer vm.mu.Unlock()

	// keep a copy of the latest SrvVschema and Vschema
	vm.currentSrvVschema = v // TODO: should we do this locking?
	vschema := vm.currentVschema

	if v == nil {
		// We encountered an error, build an empty vschema.
		if vm.currentVschema == nil {
			vschema = vindexes.BuildVSchema(&vschemapb.SrvVSchema{})
		}
	} else {
		vschema = vm.buildAndEnhanceVSchema(v)
		vm.currentVschema = vschema
	}

	if vm.subscriber != nil {
		vm.subscriber(vschema, vSchemaStats(err, vschema))
	}
	return true
}

func vSchemaStats(err error, vschema *vindexes.VSchema) *VSchemaStats {
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
	return stats
}

// Rebuild will rebuild and publish the new vschema.
// This method should be called when the underlying schema has changed.
func (vm *VSchemaManager) Rebuild() {
	vm.mu.Lock()
	v := vm.currentSrvVschema
	vm.mu.Unlock()

	log.Infof("Received schema update")
	if v == nil {
		log.Infof("No vschema to enhance")
		return
	}

	vschema := vm.buildAndEnhanceVSchema(v)
	vm.mu.Lock()
	vm.currentVschema = vschema
	vm.mu.Unlock()

	if vm.subscriber != nil {
		vm.subscriber(vschema, vSchemaStats(nil, vschema))
		log.Infof("Sent vschema to subscriber")
	}
}

// buildAndEnhanceVSchema builds a new VSchema and uses information from the schema tracker to update it
func (vm *VSchemaManager) buildAndEnhanceVSchema(v *vschemapb.SrvVSchema) *vindexes.VSchema {
	vschema := vindexes.BuildVSchema(v)
	if vm.schema != nil {
		vm.updateFromSchema(vschema)
	}
	return vschema
}

func (vm *VSchemaManager) updateFromSchema(vschema *vindexes.VSchema) {
	for ksName, ks := range vschema.Keyspaces {
		m := vm.schema.Tables(ksName)

		for tblName, columns := range m {
			vTbl := ks.Tables[tblName]
			if vTbl == nil {
				// a table that is unknown by the vschema. we add it as a normal table
				ks.Tables[tblName] = &vindexes.Table{
					Name:                    sqlparser.NewIdentifierCS(tblName),
					Keyspace:                ks.Keyspace,
					Columns:                 columns,
					ColumnListAuthoritative: true,
				}
				continue
			}
			if !vTbl.ColumnListAuthoritative {
				// if we found the matching table and the vschema view of it is not authoritative, then we just update the columns of the table
				vTbl.Columns = columns
				vTbl.ColumnListAuthoritative = true
			}
		}

		views := vm.schema.Views(ksName)
		if views != nil {
			ks.Views = make(map[string]sqlparser.SelectStatement, len(views))
			for name, def := range views {
				ks.Views[name] = sqlparser.CloneSelectStatement(def)
			}
		}
	}
}
