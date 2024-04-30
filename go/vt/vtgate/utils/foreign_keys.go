/*
Copyright 2024 The Vitess Authors.

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

package utils

import (
	"vitess.io/vitess/go/vt/graph"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func MarkErrorIfCyclesInFkAndOrderTables(vschema *vindexes.VSchema) {
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
		vertexToTable := make(map[string]*vindexes.Table)
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
					vertex := sqlparser.String(sqlparser.NewColNameWithQualifier(column.String(), table.GetTableName()))
					vertexToTable[vertex] = table
					parentVertices = append(parentVertices, vertex)
				}

				// Check for case 3.
				if cfk.OnDelete.IsCascade() {
					for _, column := range childTable.Columns {
						vertex := sqlparser.String(sqlparser.NewColNameWithQualifier(column.Name.String(), childTable.GetTableName()))
						vertexToTable[vertex] = childTable
						childVertices = append(childVertices, vertex)
					}
				} else {
					// Case 2.
					for _, column := range cfk.ChildColumns {
						vertex := sqlparser.String(sqlparser.NewColNameWithQualifier(column.String(), childTable.GetTableName()))
						vertexToTable[vertex] = childTable
						childVertices = append(childVertices, vertex)
					}
				}
				addCrossEdges(g, parentVertices, childVertices)
			}
		}
		hasCycle, cycle := g.HasCycles()
		if hasCycle {
			ks.Error = vterrors.VT09019(ksName, cycle)
			continue
		}
		idx := 1
		for _, vertex := range g.TopologicalSorting() {
			tbl := vertexToTable[vertex]
			if tbl.FkOrder == 0 {
				tbl.FkOrder = idx
				idx++
			}
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
