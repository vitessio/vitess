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

	"vitess.io/vitess/go/vt/graph"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
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
	parser            *sqlparser.Parser
}

// SchemaInfo is an interface to schema tracker.
type SchemaInfo interface {
	Tables(ks string) map[string]*vindexes.TableInfo
	Views(ks string) map[string]sqlparser.SelectStatement
}

// GetCurrentSrvVschema returns a copy of the latest SrvVschema from the
// topo watch
func (vm *VSchemaManager) GetCurrentSrvVschema() *vschemapb.SrvVSchema {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return vm.currentSrvVschema.CloneVT()
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

	_, err = vindexes.BuildKeyspace(ks, vm.parser)
	if err != nil {
		return err
	}

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
			vschema = vindexes.BuildVSchema(&vschemapb.SrvVSchema{}, vm.parser)
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
	vschema := vindexes.BuildVSchema(v, vm.parser)
	if vm.schema != nil {
		vm.updateFromSchema(vschema)
		// We mark the keyspaces that have foreign key management in Vitess and have cyclic foreign keys
		// to have an error. This makes all queries against them to fail.
		markErrorIfCyclesInFk(vschema)
	}
	return vschema
}

func (vm *VSchemaManager) updateFromSchema(vschema *vindexes.VSchema) {
	for ksName, ks := range vschema.Keyspaces {
		m := vm.schema.Tables(ksName)
		// Before we add the foreign key definitions in the tables, we need to make sure that all the tables
		// are created in the Vschema, so that later when we try to find the routed tables, we don't end up
		// getting dummy tables.
		for tblName, tblInfo := range m {
			setColumns(ks, tblName, tblInfo.Columns)
		}

		// Now that we have ensured that all the tables are created, we can start populating the foreign keys
		// in the tables.
		for tblName, tblInfo := range m {
			rTbl, err := vschema.FindRoutedTable(ksName, tblName, topodatapb.TabletType_PRIMARY)
			if err != nil {
				log.Errorf("error finding routed table %s: %v", tblName, err)
				continue
			}
			for _, fkDef := range tblInfo.ForeignKeys {
				// Ignore internal tables as part of foreign key references.
				if schema.IsInternalOperationTableName(fkDef.ReferenceDefinition.ReferencedTable.Name.String()) {
					continue
				}
				parentTbl, err := vschema.FindRoutedTable(ksName, fkDef.ReferenceDefinition.ReferencedTable.Name.String(), topodatapb.TabletType_PRIMARY)
				if err != nil {
					log.Errorf("error finding parent table %s: %v", fkDef.ReferenceDefinition.ReferencedTable.Name.String(), err)
					continue
				}
				rTbl.ParentForeignKeys = append(rTbl.ParentForeignKeys, vindexes.NewParentFkInfo(parentTbl, fkDef))
				parentTbl.ChildForeignKeys = append(parentTbl.ChildForeignKeys, vindexes.NewChildFkInfo(rTbl, fkDef))
			}
			for _, idxDef := range tblInfo.Indexes {
				switch idxDef.Info.Type {
				case sqlparser.IndexTypePrimary:
					for _, idxCol := range idxDef.Columns {
						rTbl.PrimaryKey = append(rTbl.PrimaryKey, idxCol.Column)
					}
				case sqlparser.IndexTypeUnique:
					var uniqueKey sqlparser.Exprs
					for _, idxCol := range idxDef.Columns {
						if idxCol.Expression == nil {
							uniqueKey = append(uniqueKey, sqlparser.NewColName(idxCol.Column.String()))
						} else {
							uniqueKey = append(uniqueKey, idxCol.Expression)
						}
					}
					rTbl.UniqueKeys = append(rTbl.UniqueKeys, uniqueKey)
				}
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

func markErrorIfCyclesInFk(vschema *vindexes.VSchema) {
	for ksName, ks := range vschema.Keyspaces {
		// Only check cyclic foreign keys for keyspaces that have
		// foreign keys managed in Vitess.
		if ks.ForeignKeyMode != vschemapb.Keyspace_managed {
			continue
		}
		/*
			3 cases for creating the graph for cycle detection:
			1. ON DELETE RESTRICT ON UPDATE RESTRICT: This is the simplest case where no update/delete is required on the child table, we only need to verify whether a value exists or not. So we don't need to add any edge for this case.
			2. ON DELETE SET NULL, ON UPDATE SET NULL, ON UPDATE CASCADE: In this case having any update/delete on any of the columns in the parent side of the foreign key will make a corresponding delete/update on all the column in the child side of the foreign key. So we will add an edge from all the columns in the parent side to all the columns in the child side.
			3. ON DELETE CASCADE: This is a special case wherein a deletion on the parent table will affect all the columns in the child table irrespective of the columns involved in the foreign key! So, we'll add an edge from all the columns in the parent side of the foreign key to all the columns of the child table.
		*/
		g := graph.NewGraph[string]()
		for _, table := range ks.Tables {
			for _, cfk := range table.ChildForeignKeys {
				// Check for case 1.
				if cfk.OnUpdate.IsRestrict() && cfk.OnDelete.IsRestrict() {
					continue
				}

				childTable := cfk.Table
				var parentVertices []string
				var childVertices []string
				for _, column := range cfk.ParentColumns {
					parentVertices = append(parentVertices, sqlparser.String(sqlparser.NewColNameWithQualifier(column.String(), table.GetTableName())))
				}

				// Check for case 3.
				if cfk.OnDelete.IsCascade() {
					for _, column := range childTable.Columns {
						childVertices = append(childVertices, sqlparser.String(sqlparser.NewColNameWithQualifier(column.Name.String(), childTable.GetTableName())))
					}
				} else {
					// Case 2.
					for _, column := range cfk.ChildColumns {
						childVertices = append(childVertices, sqlparser.String(sqlparser.NewColNameWithQualifier(column.String(), childTable.GetTableName())))
					}
				}
				addCrossEdges(g, parentVertices, childVertices)
			}
		}
		hasCycle, cycle := g.HasCycles()
		if hasCycle {
			ks.Error = vterrors.VT09019(ksName, cycle)
		}
	}
}

// addCrossEdges adds the edges from all the vertices in the first list to all the vertices in the second list.
func addCrossEdges(g *graph.Graph[string], from []string, to []string) {
	for _, fromStr := range from {
		for _, toStr := range to {
			g.AddEdge(fromStr, toStr)
		}
	}
}

func setColumns(ks *vindexes.KeyspaceSchema, tblName string, columns []vindexes.Column) *vindexes.Table {
	vTbl := ks.Tables[tblName]
	if vTbl == nil {
		// a table that is unknown by the vschema. we add it as a normal table
		ks.Tables[tblName] = &vindexes.Table{
			Name:                    sqlparser.NewIdentifierCS(tblName),
			Keyspace:                ks.Keyspace,
			Columns:                 columns,
			ColumnListAuthoritative: true,
		}
		return ks.Tables[tblName]
	}
	// if we found the matching table and the vschema view of it is not authoritative, then we just update the columns of the table
	if !vTbl.ColumnListAuthoritative {
		vTbl.Columns = columns
		vTbl.ColumnListAuthoritative = true
	}
	return ks.Tables[tblName]
}
