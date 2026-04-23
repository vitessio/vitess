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
	"encoding/binary"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var writesetTextValueMarker = [2]byte{0xFF, 0x00}

// fieldIndexForName resolves a column-name lookup in a field-index map by
// trying the exact spelling first and falling back to lowercase. The maps are
// populated with both variants to bridge the case-sensitivity gap between
// sqlparser output and raw binlog field names.
func fieldIndexForName(fieldIdx map[string]int, colName string) (int, bool) {
	if idx, ok := fieldIdx[colName]; ok {
		return idx, true
	}
	idx, ok := fieldIdx[strings.ToLower(colName)]
	return idx, ok
}

// writesetDigestAddPayload writes a length-prefixed payload into the digest.
// The length prefix keeps concatenated payloads unambiguous so two different
// byte sequences cannot hash to the same digest by coincidental boundary
// alignment.
func writesetDigestAddPayload(d *xxhash.Digest, payload []byte) {
	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], uint64(len(payload)))
	d.Write(scratch[:])
	d.Write(payload)
}

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
	var scratch [8]byte
	raw := v.Raw()
	binary.LittleEndian.PutUint64(scratch[:], uint64(1+len(raw)))
	d.Write(scratch[:])
	d.Write([]byte{byte(v.Type())})
	d.Write(raw)
}

// writesetDigestAddFieldValue folds a column value into the digest using
// collation-aware hashing for text columns. Two rows that MySQL considers
// equal (trailing spaces under PAD SPACE, equivalent forms under *_ci
// collations) must hash to the same writeset key or conflict detection
// would let truly-conflicting txns run in parallel.
func writesetDigestAddFieldValue(d *xxhash.Digest, field *querypb.Field, v sqltypes.Value) error {
	if field == nil || !sqltypes.IsText(field.Type) || field.Charset == 0 {
		writesetDigestAddValue(d, v)
		return nil
	}

	collation := colldata.Lookup(collations.ID(field.Charset))
	if collation == nil {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "unknown collation %d for field %s", field.Charset, field.Name)
	}

	raw := v.Raw()
	if collationUsesPadSpace(collation) {
		raw = trimTrailingPadSpaceCodepoints(collation.Charset(), raw)
	}

	var semanticHash vthash.Hasher
	semanticHash.Reset()
	collation.Hash(&semanticHash, raw, 0)

	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], semanticHash.Sum64())
	payload := make([]byte, len(writesetTextValueMarker)+len(scratch))
	copy(payload, writesetTextValueMarker[:])
	copy(payload[len(writesetTextValueMarker):], scratch[:])
	writesetDigestAddPayload(d, payload)
	return nil
}

// collationUsesPadSpace reports whether the given collation compares strings
// as if right-padded with spaces. Values under such collations have trailing
// pad codepoints stripped before hashing so that e.g. 'abc' and 'abc   '
// hash to the same writeset key.
func collationUsesPadSpace(collation colldata.Collation) bool {
	switch collation.(type) {
	case *colldata.Collation_utf8mb4_uca_0900, *colldata.Collation_utf8mb4_0900_bin:
		return false
	default:
		return true
	}
}

// trimTrailingPadSpaceCodepoints strips trailing space codepoints from raw
// bytes using the column's charset decoder. Used by PAD SPACE collations so
// values that compare equal in MySQL also hash equal in the writeset digest.
func trimTrailingPadSpaceCodepoints(cs charset.Charset, raw []byte) []byte {
	trimmedEnd := 0
	for i := 0; i < len(raw); {
		r, size := cs.DecodeRune(raw[i:])
		if size <= 0 {
			return raw
		}
		i += size
		if r != ' ' {
			trimmedEnd = i
		}
	}
	return raw[:trimmedEnd]
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

// buildCanonicalTargetTableNames builds a lowercase→original-case map of
// target table names so canonicalTargetTableName can line up FK-graph lookups
// with the various case variants that arrive from DDL, binlog events, and
// replicator plans. Entries with ambiguous casing (two different target
// names sharing the same lowercase key) are dropped rather than silently
// picking one.
func buildCanonicalTargetTableNames(tablePlans map[string]*TablePlan) map[string]string {
	if len(tablePlans) == 0 {
		return nil
	}
	canonical := make(map[string]string, len(tablePlans))
	ambiguous := make(map[string]struct{})
	for _, plan := range tablePlans {
		if plan == nil || plan.TargetName == "" {
			continue
		}
		key := strings.ToLower(plan.TargetName)
		if _, ok := ambiguous[key]; ok {
			continue
		}
		if existing, ok := canonical[key]; ok {
			if existing != plan.TargetName {
				delete(canonical, key)
				ambiguous[key] = struct{}{}
			}
			continue
		}
		canonical[key] = plan.TargetName
	}
	if len(canonical) == 0 {
		return nil
	}
	return canonical
}

// canonicalTargetTableName resolves a possibly case-varying name to the exact
// target-table key used in tablePlans. Returns the input unchanged when no
// canonical match exists so lookups miss cleanly rather than silently hitting
// a sibling table.
func canonicalTargetTableName(name string, canonical map[string]string) string {
	if name == "" || len(canonical) == 0 {
		return name
	}
	if resolved, ok := canonical[strings.ToLower(name)]; ok {
		return resolved
	}
	return name
}

// resolveFKRefsForTable collects FK constraints whose child table matches the
// given name (compared canonically). Returned refs have their ParentTable
// canonicalized so the writeset digest for a child row hashes under the same
// table-name key as the parent's writeset, which is what makes the two sides
// conflict.
func resolveFKRefsForTable(tableName string, refs map[string][]fkConstraintRef, canonical map[string]string) []fkConstraintRef {
	if tableName == "" || len(refs) == 0 {
		return nil
	}
	resolvedTableName := canonicalTargetTableName(tableName, canonical)
	var resolved []fkConstraintRef
	for name, tableRefs := range refs {
		if canonicalTargetTableName(name, canonical) != resolvedTableName {
			continue
		}
		start := len(resolved)
		resolved = append(resolved, tableRefs...)
		for i := start; i < len(resolved); i++ {
			resolved[i].ParentTable = canonicalTargetTableName(resolved[i].ParentTable, canonical)
		}
	}
	return resolved
}

// resolveParentFKRefsForTable is the parent-side counterpart to
// resolveFKRefsForTable: when a parent row changes, we need FK-style writeset
// keys keyed on the parent's referenced columns so the change conflicts with
// the child-side FK keys.
func resolveParentFKRefsForTable(tableName string, refs map[string][]parentFKRef, canonical map[string]string) []parentFKRef {
	if tableName == "" || len(refs) == 0 {
		return nil
	}
	resolvedTableName := canonicalTargetTableName(tableName, canonical)
	var resolved []parentFKRef
	for name, tableRefs := range refs {
		if canonicalTargetTableName(name, canonical) != resolvedTableName {
			continue
		}
		start := len(resolved)
		resolved = append(resolved, tableRefs...)
		for i := start; i < len(resolved); i++ {
			resolved[i].ParentTable = canonicalTargetTableName(resolved[i].ParentTable, canonical)
		}
	}
	return resolved
}

// buildResolvedFKRefTableSet returns the set of canonicalized table names
// that participate in any FK edge, as either child or parent. The scheduler
// uses this set to decide which tables' touched-row bookkeeping must follow
// FK-induced conflicts across the txn graph.
func buildResolvedFKRefTableSet(refs map[string][]fkConstraintRef, parentRefs map[string][]parentFKRef, canonical map[string]string) map[string]struct{} {
	if len(refs) == 0 && len(parentRefs) == 0 {
		return nil
	}
	resolved := make(map[string]struct{}, len(refs)+len(parentRefs))
	for name, tableRefs := range refs {
		if len(tableRefs) == 0 {
			continue
		}
		resolved[canonicalTargetTableName(name, canonical)] = struct{}{}
	}
	for name, tableRefs := range parentRefs {
		if len(tableRefs) == 0 {
			continue
		}
		resolved[canonicalTargetTableName(name, canonical)] = struct{}{}
	}
	if len(resolved) == 0 {
		return nil
	}
	return resolved
}

type txnWritesetCache struct {
	fieldIdxCache        map[string]map[string]int
	canonicalTargetNames map[string]string
	resolvedFKRefs       map[string][]fkConstraintRef
	resolvedParentRefs   map[string][]parentFKRef
}

// writesetKeysForParentFKRef generates writeset keys for a parent table row
// based on foreign key constraints that reference this table. The hash uses
// parentTable:referencedColValues, matching the child-side FK key hash.
// Returns an error if the FK columns are missing from the streamed field list,
// so the caller can force serialization instead of silently dropping the edge.
func writesetKeysForParentFKRef(ref *parentFKRef, fields []*querypb.Field, fieldIdx map[string]int, beforeVals, afterVals []sqltypes.Value, keySet map[uint64]struct{}) error {
	appendKey := func(vals []sqltypes.Value) error {
		if len(vals) == 0 {
			return nil
		}
		var d xxhash.Digest
		writesetDigestInit(&d, ref.ParentTable)
		for _, colName := range ref.ReferencedColumnNames {
			idx, ok := fieldIndexForName(fieldIdx, colName)
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "FK referenced column %q not in streamed fields for parent table %s", colName, ref.ParentTable)
			}
			if idx >= len(fields) {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "FK referenced column %q index %d out of range for parent table fields %s", colName, idx, ref.ParentTable)
			}
			if idx >= len(vals) {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "FK referenced column %q index %d out of range for parent table %s", colName, idx, ref.ParentTable)
			}
			val := vals[idx]
			if val.IsNull() {
				return nil
			}
			if err := writesetDigestAddFieldValue(&d, fields[idx], val); err != nil {
				return err
			}
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
func writesetKeysForFKRef(ref *fkConstraintRef, fields []*querypb.Field, fieldIdx map[string]int, beforeVals, afterVals []sqltypes.Value, keySet map[uint64]struct{}) error {
	if ref == nil {
		return nil
	}
	appendFKKey := func(vals []sqltypes.Value) error {
		if len(vals) == 0 {
			return nil
		}
		var d xxhash.Digest
		writesetDigestInit(&d, ref.ParentTable)
		for _, colName := range ref.ChildColumnNames {
			idx, ok := fieldIndexForName(fieldIdx, colName)
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "FK child column %q not in streamed fields for table referencing %s", colName, ref.ParentTable)
			}
			if idx >= len(fields) {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "FK child column %q index %d out of range for table fields referencing %s", colName, idx, ref.ParentTable)
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
			if err := writesetDigestAddFieldValue(&d, fields[idx], val); err != nil {
				return err
			}
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
	var cache *txnWritesetCache
	if len(fieldIdxCaches) > 0 && fieldIdxCaches[0] != nil {
		cache = &txnWritesetCache{fieldIdxCache: fieldIdxCaches[0]}
	}
	return buildTxnWritesetWithCache(tablePlans, fkRefs, parentRefs, events, cache)
}

// buildTxnWritesetWithCache is the cache-aware core of buildTxnWriteset.
// canonical-name, FK-resolution, and fieldIdx maps are shared across txns
// on the same scheduler goroutine to avoid rebuilding them per txn. Fails
// closed (returns an error) on partial row images or missing FK columns so
// the caller can route the txn through the serial path instead of producing
// a writeset that misses conflict-determining columns.
func buildTxnWritesetWithCache(tablePlans map[string]*TablePlan, fkRefs map[string][]fkConstraintRef, parentRefs map[string][]parentFKRef, events []*binlogdatapb.VEvent, cache *txnWritesetCache) ([]uint64, error) {
	// Pre-estimate capacity to avoid map rehashing during key insertion.
	// Each row change can produce ~2 keys (before + after).
	estimated := 0
	for _, event := range events {
		if event.Type == binlogdatapb.VEventType_ROW && event.RowEvent != nil {
			estimated += 2 * len(event.RowEvent.RowChanges)
		}
	}
	keySet := make(map[uint64]struct{}, estimated)
	needResolvedFKRefs := len(fkRefs) > 0 || len(parentRefs) > 0
	var canonicalTargetNames map[string]string
	var resolvedFKRefs map[string][]fkConstraintRef
	var resolvedParentRefs map[string][]parentFKRef
	if needResolvedFKRefs {
		if cache != nil {
			canonicalTargetNames = cache.canonicalTargetNames
			resolvedFKRefs = cache.resolvedFKRefs
			resolvedParentRefs = cache.resolvedParentRefs
		}
		if canonicalTargetNames == nil {
			canonicalTargetNames = buildCanonicalTargetTableNames(tablePlans)
			if cache != nil {
				cache.canonicalTargetNames = canonicalTargetNames
			}
		}
		if resolvedFKRefs == nil {
			resolvedFKRefs = make(map[string][]fkConstraintRef)
			if cache != nil {
				cache.resolvedFKRefs = resolvedFKRefs
			}
		}
		if resolvedParentRefs == nil {
			resolvedParentRefs = make(map[string][]parentFKRef)
			if cache != nil {
				cache.resolvedParentRefs = resolvedParentRefs
			}
		}
	}
	var fieldIdxCache map[string]map[string]int
	if cache != nil && cache.fieldIdxCache != nil {
		fieldIdxCache = cache.fieldIdxCache
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
		var refs []fkConstraintRef
		var pRefs []parentFKRef
		if needResolvedFKRefs {
			var ok bool
			refs, ok = resolvedFKRefs[targetTableName]
			if !ok {
				refs = resolveFKRefsForTable(targetTableName, fkRefs, canonicalTargetNames)
				resolvedFKRefs[targetTableName] = refs
			}
			pRefs, ok = resolvedParentRefs[targetTableName]
			if !ok {
				pRefs = resolveParentFKRefsForTable(targetTableName, parentRefs, canonicalTargetNames)
				resolvedParentRefs[targetTableName] = pRefs
			}
		}
		// Build fieldIdx once per table for FK and composite identity lookups.
		var fieldIdx map[string]int
		if len(refs) > 0 || len(pRefs) > 0 || len(plan.IdentityColumns) > 1 {
			var ok bool
			fieldIdx, ok = fieldIdxCache[rowEvent.TableName]
			if !ok {
				fieldIdx = make(map[string]int, len(plan.Fields))
				for i, f := range plan.Fields {
					fieldIdx[f.Name] = i
					fieldIdx[strings.ToLower(f.Name)] = i
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
			if err := writesetKeysForChangeWithFieldIdx(plan, targetTableName, fieldIdx, beforeVals, afterVals, keySet); err != nil {
				return nil, err
			}
			for i := range refs {
				if err := writesetKeysForFKRef(&refs[i], plan.Fields, fieldIdx, beforeVals, afterVals, keySet); err != nil {
					return nil, err
				}
			}
			// Parent-side: generate FK-aware keys using the referenced columns
			// so parent row changes conflict with child FK keys.
			for i := range pRefs {
				if err := writesetKeysForParentFKRef(&pRefs[i], plan.Fields, fieldIdx, beforeVals, afterVals, keySet); err != nil {
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

// beforeRowHasNegativeRelevantLengths returns true when a BEFORE image has
// -1 (omitted) lengths for any column the writeset depends on (PK or
// FK-joined column). vstreamer encodes omitted columns as -1 length without
// publishing a DataColumns bitmap on BEFORE rows, so the sentinel is the
// only signal we have. Treating those as partial images lets us fail closed
// and serialize instead of hashing against wrong-slot values.
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
			if idx, ok := fieldIndexForName(fieldIdx, colName); ok {
				relevantColumns[idx] = struct{}{}
			}
		}
	}
	for _, ref := range pRefs {
		for _, colName := range ref.ReferencedColumnNames {
			if idx, ok := fieldIndexForName(fieldIdx, colName); ok {
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

// txnTouchesExtraUniqueSecondary reports whether the txn writes any table
// whose plan carries an extra unique secondary index. Those tables have to
// serialize: writeset keys built from PK alone can miss conflicts that the
// secondary unique index would otherwise enforce.
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

// txnTouchesUnsupportedWritesetMapping reports whether any ROW event in the
// txn targets a table whose plan uses a mapping the writeset builder can't
// reason about (expressions, generated columns, lossy casts, etc). The
// scheduler must force serialization so those txns do not slip past conflict
// detection.
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
	return writesetKeysForChangeWithFieldIdx(plan, tableName, nil, beforeVals, afterVals, keySet)
}

// writesetIdentityFieldIndexes resolves a plan's declared identity column
// names to positional indexes into the streamed fields. Multi-column
// identities go through this path; single-column identity plans use a
// simpler fast path elsewhere. Returns an error if any declared column is
// missing from the streamed fields so the caller can serialize the txn.
func writesetIdentityFieldIndexes(plan *TablePlan, tableName string, fieldIdx map[string]int) ([]int, error) {
	if plan == nil || len(plan.IdentityColumns) <= 1 {
		return nil, nil
	}
	if fieldIdx == nil {
		fieldIdx = make(map[string]int, len(plan.Fields))
		for i, f := range plan.Fields {
			if f == nil {
				continue
			}
			fieldIdx[f.Name] = i
			fieldIdx[strings.ToLower(f.Name)] = i
		}
	}
	indexes := make([]int, 0, len(plan.IdentityColumns))
	for _, colName := range plan.IdentityColumns {
		idx, ok := fieldIndexForName(fieldIdx, colName)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "writeset identity column %q not in streamed fields for %s", colName, tableName)
		}
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

// writesetKeysForChangeWithFieldIdx is the indexed variant of
// writesetKeysForChange: it takes a pre-built field-index map so multi-row
// txns do not rebuild the lookup per change. The keys it inserts into
// keySet are what the scheduler compares to decide which concurrent txns
// conflict.
func writesetKeysForChangeWithFieldIdx(plan *TablePlan, tableName string, fieldIdx map[string]int, beforeVals, afterVals []sqltypes.Value, keySet map[uint64]struct{}) error {
	if plan == nil {
		return nil
	}
	if len(plan.PKIndices) == 0 {
		if len(plan.IdentityColumns) != 0 {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no usable writeset identity for %s", tableName)
		}
		return nil
	}
	identityIndexes, err := writesetIdentityFieldIndexes(plan, tableName, fieldIdx)
	if err != nil {
		return err
	}
	appendKey := func(vals []sqltypes.Value) error {
		if len(vals) == 0 {
			return nil
		}
		var d xxhash.Digest
		writesetDigestInit(&d, tableName)
		hasPK := false
		if len(identityIndexes) > 0 {
			for _, idx := range identityIndexes {
				if idx >= len(vals) {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "pk index out of range for %s", tableName)
				}
				hasPK = true
				var field *querypb.Field
				if idx < len(plan.Fields) {
					field = plan.Fields[idx]
				}
				if err := writesetDigestAddFieldValue(&d, field, vals[idx]); err != nil {
					return err
				}
			}
		} else {
			for i, isPK := range plan.PKIndices {
				if !isPK {
					continue
				}
				if i >= len(vals) {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "pk index out of range for %s", tableName)
				}
				hasPK = true
				var field *querypb.Field
				if i < len(plan.Fields) {
					field = plan.Fields[i]
				}
				if err := writesetDigestAddFieldValue(&d, field, vals[i]); err != nil {
					return err
				}
			}
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
		"SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME, "+
			"child_cols.DATA_TYPE, COALESCE(child_cols.CHARACTER_SET_NAME, ''), COALESCE(child_cols.COLLATION_NAME, ''), COALESCE(child_cols.COLUMN_TYPE, ''), "+
			"parent_cols.DATA_TYPE, COALESCE(parent_cols.CHARACTER_SET_NAME, ''), COALESCE(parent_cols.COLLATION_NAME, ''), COALESCE(parent_cols.COLUMN_TYPE, '') "+
			"FROM information_schema.KEY_COLUMN_USAGE kcu "+
			"JOIN information_schema.COLUMNS child_cols "+
			"ON child_cols.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND child_cols.TABLE_NAME = kcu.TABLE_NAME AND child_cols.COLUMN_NAME = kcu.COLUMN_NAME "+
			"JOIN information_schema.COLUMNS parent_cols "+
			"ON parent_cols.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND parent_cols.TABLE_NAME = kcu.REFERENCED_TABLE_NAME AND parent_cols.COLUMN_NAME = kcu.REFERENCED_COLUMN_NAME "+
			"WHERE kcu.TABLE_SCHEMA = %s AND kcu.REFERENCED_TABLE_NAME IS NOT NULL "+
			"ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
		encodeString(dbName),
	)
	qr, err := dbClient.ExecuteFetch(query, -1)
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

	type fkColumnDigestMeta struct {
		dataType   string
		charset    string
		collation  string
		columnType string
	}
	parseDigestMeta := func(offset int, row []sqltypes.Value) fkColumnDigestMeta {
		return fkColumnDigestMeta{
			dataType:   strings.ToLower(row[offset].ToString()),
			charset:    row[offset+1].ToString(),
			collation:  row[offset+2].ToString(),
			columnType: strings.ToLower(row[offset+3].ToString()),
		}
	}
	usesTextDigest := func(meta fkColumnDigestMeta) bool {
		return meta.charset != "" || meta.collation != ""
	}
	columnsShareWritesetEncoding := func(child, parent fkColumnDigestMeta) bool {
		if usesTextDigest(child) || usesTextDigest(parent) {
			return usesTextDigest(child) && usesTextDigest(parent) &&
				child.charset == parent.charset &&
				child.collation == parent.collation
		}
		return child.columnType == parent.columnType
	}

	for _, row := range qr.Rows {
		childTable := row[0].ToString()
		constraintName := row[1].ToString()
		colName := row[2].ToString()
		parentTable := row[3].ToString()
		referencedColName := row[4].ToString()
		childMeta := parseDigestMeta(5, row)
		parentMeta := parseDigestMeta(9, row)
		if !columnsShareWritesetEncoding(childMeta, parentMeta) {
			return nil, vterrors.Errorf(
				vtrpcpb.Code_FAILED_PRECONDITION,
				"incompatible FK column definitions for %s.%s referencing %s.%s: child=%s/%s parent=%s/%s; align the definitions or disable parallel apply for this workflow",
				childTable,
				colName,
				parentTable,
				referencedColName,
				childMeta.columnType,
				childMeta.collation,
				parentMeta.columnType,
				parentMeta.collation,
			)
		}

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
