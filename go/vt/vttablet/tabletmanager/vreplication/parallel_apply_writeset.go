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

package vreplication

import (
	"fmt"
	"maps"
	"strings"
	"sync"

	"vitess.io/vitess/go/sqltypes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// fkConstraintRef represents one foreign key constraint on a table.
// It maps one or more child columns to a parent table, allowing the
// parallel apply writeset to include FK reference keys that conflict
// with the parent table's PK keys.
type fkConstraintRef struct {
	ParentTable      string   // referenced parent table name
	ChildColumnNames []string // child column names, in FK ordinal order
}

// writesetKeysForFKRef generates writeset keys based on a foreign key constraint.
// For each row (before and after), it looks up the child column values and produces
// a key in the format "parentTable:val1,val2,..." which will conflict with the
// parent table's PK-based writeset key, forcing serialization of dependent txns.
//
// beforeVals/afterVals are pre-decoded row values (decoded once per change in
// buildTxnWriteset). fieldIdx maps field name to index in the values slice.
func writesetKeysForFKRef(ref *fkConstraintRef, fieldIdx map[string]int, beforeVals, afterVals []sqltypes.Value, keySet map[string]struct{}, buf *strings.Builder) {
	if ref == nil {
		return
	}
	appendFKKey := func(vals []sqltypes.Value) {
		if len(vals) == 0 {
			return
		}
		buf.Reset()
		buf.WriteString(ref.ParentTable)
		buf.WriteByte(':')
		first := true
		for _, colName := range ref.ChildColumnNames {
			idx, ok := fieldIdx[colName]
			if !ok {
				// Column not in fields — can't build FK key, skip silently.
				return
			}
			if idx >= len(vals) {
				return
			}
			if !first {
				buf.WriteByte(',')
			}
			first = false
			buf.WriteString(vals[idx].String())
		}
		keySet[buf.String()] = struct{}{}
	}
	appendFKKey(beforeVals)
	appendFKKey(afterVals)
}

func buildTxnWriteset(tablePlans map[string]*TablePlan, fkRefs map[string][]fkConstraintRef, events []*binlogdatapb.VEvent) ([]string, error) {
	keySet := map[string]struct{}{}
	var buf strings.Builder
	// Cache fieldIdx per table to avoid rebuilding on every row change.
	fieldIdxCache := map[string]map[string]int{}
	for _, event := range events {
		if event.Type != binlogdatapb.VEventType_ROW {
			continue
		}
		rowEvent := event.RowEvent
		if rowEvent == nil {
			continue
		}
		plan := tablePlans[rowEvent.TableName]
		if plan == nil {
			return nil, fmt.Errorf("missing table plan for %s", rowEvent.TableName)
		}
		refs := fkRefs[rowEvent.TableName]
		// Build fieldIdx once per table for FK ref lookups.
		var fieldIdx map[string]int
		if len(refs) > 0 {
			var ok bool
			fieldIdx, ok = fieldIdxCache[rowEvent.TableName]
			if !ok {
				fieldIdx = make(map[string]int, len(plan.Fields))
				for i, f := range plan.Fields {
					fieldIdx[f.Name] = i
				}
				fieldIdxCache[rowEvent.TableName] = fieldIdx
			}
		}
		for _, change := range rowEvent.RowChanges {
			// Decode Before/After row values once per change.
			var beforeVals, afterVals []sqltypes.Value
			if change.Before != nil && plan.Fields != nil {
				beforeVals = sqltypes.MakeRowTrusted(plan.Fields, change.Before)
			}
			if change.After != nil && plan.Fields != nil {
				afterVals = sqltypes.MakeRowTrusted(plan.Fields, change.After)
			}
			if err := writesetKeysForChange(plan, rowEvent.TableName, beforeVals, afterVals, keySet, &buf); err != nil {
				return nil, err
			}
			for i := range refs {
				writesetKeysForFKRef(&refs[i], fieldIdx, beforeVals, afterVals, keySet, &buf)
			}
		}
	}
	if len(keySet) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	return keys, nil
}

func snapshotTablePlans(mu *sync.RWMutex, tablePlans map[string]*TablePlan, version *int64, cachedVersion *int64, cached map[string]*TablePlan) map[string]*TablePlan {
	if tablePlans == nil {
		return nil
	}
	mu.RLock()
	defer mu.RUnlock()
	if cached != nil && *version == *cachedVersion {
		return cached
	}
	copy := make(map[string]*TablePlan, len(tablePlans))
	maps.Copy(copy, tablePlans)
	*cachedVersion = *version
	return copy
}

// writesetKeysForChange extracts PK-based writeset keys from pre-decoded row
// values and inserts them directly into the caller's keySet map. The buf
// argument is a reusable strings.Builder to avoid per-call allocations.
// beforeVals/afterVals are decoded once per change in buildTxnWriteset.
func writesetKeysForChange(plan *TablePlan, tableName string, beforeVals, afterVals []sqltypes.Value, keySet map[string]struct{}, buf *strings.Builder) error {
	if plan == nil {
		return nil
	}
	if len(plan.PKIndices) == 0 {
		return nil
	}
	appendKey := func(vals []sqltypes.Value) error {
		if len(vals) == 0 {
			return nil
		}
		buf.Reset()
		buf.WriteString(tableName)
		buf.WriteByte(':')
		first := true
		hasPK := false
		for i, isPK := range plan.PKIndices {
			if !isPK {
				continue
			}
			if i >= len(vals) {
				return fmt.Errorf("pk index out of range for %s", tableName)
			}
			hasPK = true
			if !first {
				buf.WriteByte(',')
			}
			first = false
			buf.WriteString(vals[i].String())
		}
		if !hasPK {
			return nil
		}
		keySet[buf.String()] = struct{}{}
		return nil
	}
	if err := appendKey(beforeVals); err != nil {
		return err
	}
	if err := appendKey(afterVals); err != nil {
		return err
	}
	return nil
}

// queryFKRefs queries information_schema.KEY_COLUMN_USAGE to discover all
// foreign key constraints in the given database. It returns a map from
// child table name to a list of FK constraints. Each constraint includes
// the referenced (parent) table name and the child column names in ordinal
// order, so that writeset keys generated for child rows will match the
// parent table's PK-based writeset keys.
func queryFKRefs(dbClient *vdbClient, dbName string) (map[string][]fkConstraintRef, error) {
	query := fmt.Sprintf(
		"SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, ORDINAL_POSITION "+
			"FROM information_schema.KEY_COLUMN_USAGE "+
			"WHERE TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME IS NOT NULL "+
			"ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION",
		encodeString(dbName),
	)
	qr, err := dbClient.ExecuteFetch(query, 10000)
	if err != nil {
		return nil, fmt.Errorf("queryFKRefs: %w", err)
	}
	if len(qr.Rows) == 0 {
		return nil, nil
	}

	// Group by (childTable, parentTable) constraint — each row is one column
	// of a potentially multi-column FK.
	type constraintKey struct {
		childTable  string
		parentTable string
	}
	type constraintEntry struct {
		key  constraintKey
		cols []string // child column names in ordinal order
	}

	// Use a slice to preserve order; there are typically very few FK constraints.
	var constraints []constraintEntry
	idx := map[constraintKey]int{}

	for _, row := range qr.Rows {
		childTable := row[0].ToString()
		colName := row[1].ToString()
		parentTable := row[2].ToString()

		k := constraintKey{childTable: childTable, parentTable: parentTable}
		if i, ok := idx[k]; ok {
			constraints[i].cols = append(constraints[i].cols, colName)
		} else {
			idx[k] = len(constraints)
			constraints = append(constraints, constraintEntry{
				key:  k,
				cols: []string{colName},
			})
		}
	}

	result := make(map[string][]fkConstraintRef, len(constraints))
	for _, c := range constraints {
		result[c.key.childTable] = append(result[c.key.childTable], fkConstraintRef{
			ParentTable:      c.key.parentTable,
			ChildColumnNames: c.cols,
		})
	}
	return result, nil
}
