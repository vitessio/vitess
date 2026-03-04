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
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// FNV-1a constants for uint64.
// We use inline FNV-1a hashing to convert writeset keys (previously
// heap-allocated strings like "table:pk1,pk2") into uint64 values.
// This eliminates per-transaction string allocations in the scheduler
// hot path, which were the dominant allocation source at high TPS.
const (
	fnvOffset uint64 = 14695981039346656037
	fnvPrime  uint64 = 1099511628211
)

// writesetHash returns a new FNV-1a hash seeded with the table name.
func writesetHash(tableName string) uint64 {
	h := fnvOffset
	for i := range len(tableName) {
		h ^= uint64(tableName[i])
		h *= fnvPrime
	}
	// Separator between table name and values.
	h ^= uint64(':')
	h *= fnvPrime
	return h
}

// writesetHashAddByte folds a single byte into an FNV-1a hash.
func writesetHashAddByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= fnvPrime
	return h
}

// writesetHashAddValue folds a sqltypes.Value into the hash by writing
// its type discriminator followed by its raw bytes.
func writesetHashAddValue(h uint64, v sqltypes.Value) uint64 {
	// Type discriminator (1 byte) to distinguish e.g. INT64(1) from VARCHAR("1").
	h = writesetHashAddByte(h, byte(v.Type()))
	raw := v.Raw()
	for _, b := range raw {
		h ^= uint64(b)
		h *= fnvPrime
	}
	return h
}

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
// a hash keyed on the parent table name and FK column values, which will conflict
// with the parent table's PK-based writeset key, forcing serialization of
// dependent txns.
func writesetKeysForFKRef(ref *fkConstraintRef, fieldIdx map[string]int, beforeVals, afterVals []sqltypes.Value, keySet map[uint64]struct{}) {
	if ref == nil {
		return
	}
	appendFKKey := func(vals []sqltypes.Value) {
		if len(vals) == 0 {
			return
		}
		h := writesetHash(ref.ParentTable)
		first := true
		for _, colName := range ref.ChildColumnNames {
			idx, ok := fieldIdx[colName]
			if !ok {
				return
			}
			if idx >= len(vals) {
				return
			}
			if !first {
				h = writesetHashAddByte(h, ',')
			}
			first = false
			h = writesetHashAddValue(h, vals[idx])
		}
		keySet[h] = struct{}{}
	}
	appendFKKey(beforeVals)
	appendFKKey(afterVals)
}

// buildTxnWriteset builds writeset keys for the given events.
// fieldIdxCache is an optional cache of field-name→index maps, shared
// across transactions on the same scheduler goroutine. Pass nil to
// use a local cache (e.g. in tests).
func buildTxnWriteset(tablePlans map[string]*TablePlan, fkRefs map[string][]fkConstraintRef, events []*binlogdatapb.VEvent, fieldIdxCaches ...map[string]map[string]int) ([]uint64, error) {
	// Pre-estimate capacity to avoid map rehashing during key insertion.
	// Each row change can produce ~2 keys (before + after).
	estimated := 0
	for _, event := range events {
		if event.Type == binlogdatapb.VEventType_ROW && event.RowEvent != nil {
			estimated += 2 * len(event.RowEvent.RowChanges)
		}
	}
	keySet := make(map[uint64]struct{}, estimated)
	var fieldIdxCache map[string]map[string]int
	if len(fieldIdxCaches) > 0 && fieldIdxCaches[0] != nil {
		fieldIdxCache = fieldIdxCaches[0]
	} else {
		fieldIdxCache = map[string]map[string]int{}
	}
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
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "missing table plan for %s", rowEvent.TableName)
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
			if err := writesetKeysForChange(plan, rowEvent.TableName, beforeVals, afterVals, keySet); err != nil {
				return nil, err
			}
			for i := range refs {
				writesetKeysForFKRef(&refs[i], fieldIdx, beforeVals, afterVals, keySet)
			}
		}
	}
	if len(keySet) == 0 {
		return nil, nil
	}
	keys := make([]uint64, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	return keys, nil
}

// snapshotTablePlans returns a copy-on-write snapshot of tablePlans. It only
// copies the map when the version has changed since the last snapshot, avoiding
// the read-lock hold time of building writesets directly against the live map.
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
// values and inserts them directly into the caller's keySet map as uint64 hashes.
func writesetKeysForChange(plan *TablePlan, tableName string, beforeVals, afterVals []sqltypes.Value, keySet map[uint64]struct{}) error {
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
		h := writesetHash(tableName)
		first := true
		hasPK := false
		for i, isPK := range plan.PKIndices {
			if !isPK {
				continue
			}
			if i >= len(vals) {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "pk index out of range for %s", tableName)
			}
			hasPK = true
			if !first {
				h = writesetHashAddByte(h, ',')
			}
			first = false
			h = writesetHashAddValue(h, vals[i])
		}
		if !hasPK {
			return nil
		}
		keySet[h] = struct{}{}
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
		return nil, vterrors.Wrapf(err, "queryFKRefs")
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
