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
	"sync/atomic"

	"github.com/cespare/xxhash/v2"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// writesetDigestInit initializes an xxhash digest with the table name
// followed by a ':' separator. Callers declare a stack-local xxhash.Digest
// and pass its address to avoid heap allocation. xxhash provides better
// throughput than FNV-1a for writeset keys with multiple PK columns.
func writesetDigestInit(d *xxhash.Digest, tableName string) {
	d.Reset()
	d.WriteString(tableName)
	d.Write([]byte{':'})
}

// writesetDigestAddValue folds a sqltypes.Value into the digest by writing
// its type discriminator (1 byte) followed by its raw bytes.
func writesetDigestAddValue(d *xxhash.Digest, v sqltypes.Value) {
	d.Write([]byte{byte(v.Type())})
	d.Write(v.Raw())
}

// fkConstraintRef represents one foreign key constraint on a table.
// It maps one or more child columns to a parent table, allowing the
// parallel apply writeset to include FK reference keys that conflict
// with the parent table's writeset keys.
type fkConstraintRef struct {
	ParentTable           string   // referenced parent table name
	ChildColumnNames      []string // child column names, in FK ordinal order
	ReferencedColumnNames []string // parent column names, in FK ordinal order
}

// parentFKRef represents a foreign key constraint from the parent table's
// perspective. When a parent row changes, we generate writeset keys using the
// referenced column values so they conflict with child-side FK keys.
type parentFKRef struct {
	ParentTable           string   // the parent table name (same as the table being modified)
	ReferencedColumnNames []string // parent column names referenced by the FK
}

// buildParentFKRefs builds a reverse map from parent table name to the FK
// constraints that reference it. This allows parent-side rows to generate
// writeset keys that match child-side FK keys.
func buildParentFKRefs(fkRefs map[string][]fkConstraintRef) map[string][]parentFKRef {
	if len(fkRefs) == 0 {
		return nil
	}
	result := make(map[string][]parentFKRef)
	for _, refs := range fkRefs {
		for _, ref := range refs {
			result[ref.ParentTable] = append(result[ref.ParentTable], parentFKRef{
				ParentTable:           ref.ParentTable,
				ReferencedColumnNames: ref.ReferencedColumnNames,
			})
		}
	}
	return result
}

// writesetKeysForParentFKRef generates writeset keys for a parent table row
// based on foreign key constraints that reference this table. The hash uses
// parentTable:referencedColValues, matching the child-side FK key hash.
// Returns an error if the FK columns are missing from the streamed field list,
// so the caller can force serialization instead of silently dropping the edge.
func writesetKeysForParentFKRef(ref *parentFKRef, fieldIdx map[string]int, beforeVals, afterVals []sqltypes.Value, keySet map[uint64]struct{}) error {
	appendKey := func(vals []sqltypes.Value) error {
		if len(vals) == 0 {
			return nil
		}
		var d xxhash.Digest
		writesetDigestInit(&d, ref.ParentTable)
		first := true
		for _, colName := range ref.ReferencedColumnNames {
			idx, ok := fieldIdx[colName]
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "FK referenced column %q not in streamed fields for parent table %s", colName, ref.ParentTable)
			}
			if idx >= len(vals) {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "FK referenced column %q index %d out of range for parent table %s", colName, idx, ref.ParentTable)
			}
			val := vals[idx]
			if val.IsNull() {
				return nil
			}
			if !first {
				d.Write([]byte{','})
			}
			first = false
			writesetDigestAddValue(&d, val)
		}
		keySet[d.Sum64()] = struct{}{}
		return nil
	}
	if err := appendKey(beforeVals); err != nil {
		return err
	}
	return appendKey(afterVals)
}

// writesetKeysForFKRef generates writeset keys based on a foreign key constraint.
// For each row (before and after), it looks up the child column values and produces
// a hash keyed on the parent table name and FK column values, which will conflict
// with the parent table's PK-based writeset key, forcing serialization of
// dependent txns.
// Returns an error if FK columns are missing from the streamed field list.
func writesetKeysForFKRef(ref *fkConstraintRef, fieldIdx map[string]int, beforeVals, afterVals []sqltypes.Value, keySet map[uint64]struct{}) error {
	if ref == nil {
		return nil
	}
	appendFKKey := func(vals []sqltypes.Value) error {
		if len(vals) == 0 {
			return nil
		}
		var d xxhash.Digest
		writesetDigestInit(&d, ref.ParentTable)
		first := true
		for _, colName := range ref.ChildColumnNames {
			idx, ok := fieldIdx[colName]
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "FK child column %q not in streamed fields for table referencing %s", colName, ref.ParentTable)
			}
			if idx >= len(vals) {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "FK child column %q index %d out of range for table referencing %s", colName, idx, ref.ParentTable)
			}
			val := vals[idx]
			// In MySQL, if any referencing column in an FK is NULL, the FK
			// constraint is not enforced for that row. Skip generating a
			// writeset key in that case to avoid artificial conflicts.
			if val.IsNull() {
				return nil
			}
			if !first {
				d.Write([]byte{','})
			}
			first = false
			writesetDigestAddValue(&d, val)
		}
		keySet[d.Sum64()] = struct{}{}
		return nil
	}
	if err := appendFKKey(beforeVals); err != nil {
		return err
	}
	return appendFKKey(afterVals)
}

// buildTxnWriteset builds writeset keys for the given events.
// fieldIdxCache is an optional cache of field-name→index maps, shared
// across transactions on the same scheduler goroutine. Pass nil to
// use a local cache (e.g. in tests).
func buildTxnWriteset(tablePlans map[string]*TablePlan, fkRefs map[string][]fkConstraintRef, parentRefs map[string][]parentFKRef, events []*binlogdatapb.VEvent, fieldIdxCaches ...map[string]map[string]int) ([]uint64, error) {
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
		targetTableName := plan.TargetName
		refs := fkRefs[targetTableName]
		pRefs := parentRefs[targetTableName]
		// Build fieldIdx once per table for FK ref lookups (child or parent side).
		var fieldIdx map[string]int
		if len(refs) > 0 || len(pRefs) > 0 {
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
			// Partial row images (DataColumns/JsonPartialValues) omit columns
			// from the binlog payload. buildTxnWriteset decodes rows with
			// sqltypes.MakeRowTrusted(plan.Fields, change.Before/After), which
			// treats the streamed values as positional and ignores the bitmaps.
			// That makes both PK and FK hashing unsafe: omitted columns can
			// shift later values into the wrong field slots. BEFORE images are
			// ambiguous too: vstreamer can encode omitted columns as -1 lengths,
			// but it only publishes DataColumns for AFTER rows.
			// Fail closed until writeset reconstruction becomes bitmap-aware.
			isPartialRow := change.DataColumns != nil || change.JsonPartialValues != nil
			if !isPartialRow && plan.Fields != nil {
				isPartialRow = (change.Before != nil && len(change.Before.Lengths) < len(plan.Fields)) ||
					(change.After != nil && len(change.After.Lengths) < len(plan.Fields))
			}
			if !isPartialRow {
				isPartialRow = beforeRowHasNegativeRelevantLengths(change.Before, plan, fieldIdx, refs, pRefs)
			}
			if isPartialRow {
				return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "partial row image on table %s: forcing serialization", rowEvent.TableName)
			}
			// Decode Before/After row values once per change.
			var beforeVals, afterVals []sqltypes.Value
			if change.Before != nil && plan.Fields != nil {
				beforeVals = sqltypes.MakeRowTrusted(plan.Fields, change.Before)
			}
			if change.After != nil && plan.Fields != nil {
				afterVals = sqltypes.MakeRowTrusted(plan.Fields, change.After)
			}
			if err := writesetKeysForChange(plan, targetTableName, beforeVals, afterVals, keySet); err != nil {
				return nil, err
			}
			for i := range refs {
				if err := writesetKeysForFKRef(&refs[i], fieldIdx, beforeVals, afterVals, keySet); err != nil {
					return nil, err
				}
			}
			// Parent-side: generate FK-aware keys using the referenced columns
			// so parent row changes conflict with child FK keys.
			for i := range pRefs {
				if err := writesetKeysForParentFKRef(&pRefs[i], fieldIdx, beforeVals, afterVals, keySet); err != nil {
					return nil, err
				}
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

func beforeRowHasNegativeRelevantLengths(row *querypb.Row, plan *TablePlan, fieldIdx map[string]int, refs []fkConstraintRef, pRefs []parentFKRef) bool {
	if row == nil {
		return false
	}
	relevantColumns := make(map[int]struct{})
	for i, isPK := range plan.PKIndices {
		if isPK {
			relevantColumns[i] = struct{}{}
		}
	}
	for _, ref := range refs {
		for _, colName := range ref.ChildColumnNames {
			if idx, ok := fieldIdx[colName]; ok {
				relevantColumns[idx] = struct{}{}
			}
		}
	}
	for _, ref := range pRefs {
		for _, colName := range ref.ReferencedColumnNames {
			if idx, ok := fieldIdx[colName]; ok {
				relevantColumns[idx] = struct{}{}
			}
		}
	}
	for i, length := range row.Lengths {
		if length < 0 {
			if _, ok := relevantColumns[i]; ok {
				return true
			}
		}
	}
	return false
}

// snapshotTablePlans returns a copy-on-write snapshot of tablePlans. It only
// copies the map when the version has changed since the last snapshot, avoiding
// the read-lock hold time of building writesets directly against the live map.
func snapshotTablePlans(mu *sync.RWMutex, tablePlans map[string]*TablePlan, version *atomic.Int64, cachedVersion *int64, cached map[string]*TablePlan) map[string]*TablePlan {
	if tablePlans == nil {
		return nil
	}
	mu.RLock()
	defer mu.RUnlock()
	v := version.Load()
	if cached != nil && v == *cachedVersion {
		return cached
	}
	cp := make(map[string]*TablePlan, len(tablePlans))
	maps.Copy(cp, tablePlans)
	*cachedVersion = v
	return cp
}

func txnTouchesExtraUniqueSecondary(events []*binlogdatapb.VEvent, tablePlans map[string]*TablePlan) bool {
	for _, event := range events {
		if event.Type != binlogdatapb.VEventType_ROW || event.RowEvent == nil {
			continue
		}
		plan := tablePlans[event.RowEvent.TableName]
		if plan != nil && plan.HasExtraUniqueSecondary {
			return true
		}
	}
	return false
}

func txnTouchesUnsupportedWritesetMapping(events []*binlogdatapb.VEvent, tablePlans map[string]*TablePlan) bool {
	for _, event := range events {
		if event.Type != binlogdatapb.VEventType_ROW || event.RowEvent == nil {
			continue
		}
		plan := tablePlans[event.RowEvent.TableName]
		if plan != nil && plan.HasUnsupportedWritesetMapping {
			return true
		}
	}
	return false
}

// writesetKeysForChange extracts PK-based writeset keys from pre-decoded row
// values and inserts them directly into the caller's keySet map as uint64 hashes.
func writesetKeysForChange(plan *TablePlan, tableName string, beforeVals, afterVals []sqltypes.Value, keySet map[uint64]struct{}) error {
	if plan == nil {
		return nil
	}
	if len(plan.PKIndices) == 0 {
		if len(plan.IdentityColumns) != 0 {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no usable writeset identity for %s", tableName)
		}
		return nil
	}
	appendKey := func(vals []sqltypes.Value) error {
		if len(vals) == 0 {
			return nil
		}
		var d xxhash.Digest
		writesetDigestInit(&d, tableName)
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
				d.Write([]byte{','})
			}
			first = false
			writesetDigestAddValue(&d, vals[i])
		}
		if !hasPK {
			if len(plan.IdentityColumns) != 0 {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no usable writeset identity for %s", tableName)
			}
			return nil
		}
		keySet[d.Sum64()] = struct{}{}
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
		"SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "+
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

	// Group by (childTable, constraintName) — each row is one column
	// of a potentially multi-column FK. We group by constraint name
	// rather than parent table because a child table can have multiple
	// FK constraints referencing the same parent table with different
	// column sets.
	type constraintKey struct {
		childTable     string
		constraintName string
	}
	type constraintEntry struct {
		key            constraintKey
		parentTable    string
		cols           []string // child column names in ordinal order
		referencedCols []string // parent column names in ordinal order
	}

	// Use a slice to preserve order; there are typically very few FK constraints.
	var constraints []constraintEntry
	idx := map[constraintKey]int{}

	for _, row := range qr.Rows {
		childTable := row[0].ToString()
		constraintName := row[1].ToString()
		colName := row[2].ToString()
		parentTable := row[3].ToString()
		referencedColName := row[4].ToString()

		k := constraintKey{childTable: childTable, constraintName: constraintName}
		if i, ok := idx[k]; ok {
			constraints[i].cols = append(constraints[i].cols, colName)
			constraints[i].referencedCols = append(constraints[i].referencedCols, referencedColName)
		} else {
			idx[k] = len(constraints)
			constraints = append(constraints, constraintEntry{
				key:            k,
				parentTable:    parentTable,
				cols:           []string{colName},
				referencedCols: []string{referencedColName},
			})
		}
	}

	result := make(map[string][]fkConstraintRef, len(constraints))
	for _, c := range constraints {
		result[c.key.childTable] = append(result[c.key.childTable], fkConstraintRef{
			ParentTable:           c.parentTable,
			ChildColumnNames:      c.cols,
			ReferencedColumnNames: c.referencedCols,
		})
	}
	return result, nil
}
