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
	"fmt"
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
	serv              srvtopo.Server
	cell              string
	subscriber        func(vschema *vindexes.VSchema, stats *VSchemaStats)
	schema            SchemaInfo
}

// SchemaTable contains the table name, columns and the accuracy of the information.
type SchemaTable struct {
	Name    string
	Columns []vindexes.Column
	Unknown bool
}

// SchemaInfo is an interface to schema tracker.
type SchemaInfo interface {
	Tables(ks string) []SchemaTable
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

	return err
}

// VSchemaUpdate builds the VSchema from SrvVschema and call subscribers.
func (vm *VSchemaManager) VSchemaUpdate(v *vschemapb.SrvVSchema, err error) {
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

	// keep a copy of the latest SrvVschema
	vm.mu.Lock()
	vm.currentSrvVschema = v
	vm.mu.Unlock()

	var vschema *vindexes.VSchema
	if v != nil {
		vschema, err = vindexes.BuildVSchema(v)
		if err == nil {
			if vm.schema != nil {
				vm.updateFromSchema(vschema)
			}
		} else {
			log.Warningf("Error creating VSchema for cell %v (will try again next update): %v", vm.cell, err)
			err = fmt.Errorf("error creating VSchema for cell %v: %v", vm.cell, err)
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
	if vm.subscriber != nil {
		vm.subscriber(vschema, stats)
	}
}

func (vm *VSchemaManager) updateFromSchema(vschema *vindexes.VSchema) {
	for ksName, ks := range vschema.Keyspaces {
		for _, tbl := range vm.schema.Tables(ksName) {
			if tbl.Unknown {
				continue
			}
			vTbl := ks.Tables[tbl.Name]
			if vTbl == nil {
				// a table that is unknown by the vschema. we add it as a normal table
				ks.Tables[tbl.Name] = &vindexes.Table{
					Name:                    sqlparser.NewTableIdent(tbl.Name),
					Keyspace:                ks.Keyspace,
					Columns:                 tbl.Columns,
					ColumnListAuthoritative: true,
				}
				continue
			}
			if !vTbl.ColumnListAuthoritative {
				// if we found the matching table and the vschema view of it is not authoritative, then we just update the columns of the table
				vTbl.Columns = tbl.Columns
				vTbl.ColumnListAuthoritative = true
			}
		}
	}
}
