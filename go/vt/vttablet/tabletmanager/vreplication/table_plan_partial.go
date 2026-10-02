/*
Copyright 2023 The Vitess Authors.

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
	"bytes"
	"encoding/hex"
	"strings"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// isBitSet returns true if the bit at index is set
func isBitSet(data []byte, index int) bool {
	byteIndex := index / 8
	bitMask := byte(1 << (uint(index) & 0x7))
	return data[byteIndex]&bitMask > 0
}

func setBit(data []byte, index int, value bool) {
	byteIndex := index / 8
	bitMask := byte(1 << (uint(index) & 0x7))
	if value {
		data[byteIndex] |= bitMask
	} else {
		data[byteIndex] &= 0xff - bitMask
	}
}

func (tp *TablePlan) isPartial(rowChange *binlogdatapb.RowChange) bool {
	if rowChange == nil {
		return false
	}
	return (rowChange.DataColumns != nil && rowChange.DataColumns.Count > 0) ||
		(rowChange.AfterDataColumns != nil && rowChange.AfterDataColumns.Count > 0) ||
		(rowChange.JsonPartialValues != nil && rowChange.JsonPartialValues.Count > 0)
}

// streamedDataColumns returns the after image column presence bitmap of a row
// event, indexed by the columns streamed from the source (tp.Fields). It
// prefers AfterDataColumns, which the vstreamer projects through the filter so
// that bit i always describes tp.Fields[i]. When an older source only sends the
// legacy DataColumns, that is returned instead; its bits are in the source
// table's column order and only line up with tp.Fields when the filter neither
// reorders nor drops columns, which is the behavior we have always had.
func (tp *TablePlan) streamedDataColumns(rowChange *binlogdatapb.RowChange) *binlogdatapb.RowChange_Bitmap {
	if rowChange == nil {
		return nil
	}
	if rowChange.AfterDataColumns != nil && rowChange.AfterDataColumns.Count > 0 {
		return rowChange.AfterDataColumns
	}
	return rowChange.DataColumns
}

// mappedDataColumns is a streamed after-image bitmap projected onto the target
// table's column expressions. It only depends on the bitmap, so it is cached
// per distinct bitmap in TablePlan.PartialBitmaps.
type mappedDataColumns struct {
	// target has bit i set when colExprs[i] can be generated from the image.
	target *binlogdatapb.RowChange_Bitmap
	// mixed lists the target expressions that use both present and absent
	// streamed columns, with the indexes (into tp.Fields) of their present
	// inputs. Such an expression cannot be computed from the image. It is left
	// out of the partial query when none of its present inputs changed, since
	// its value cannot have changed either, and rejected otherwise.
	mixed []mixedColExpr
}

type mixedColExpr struct {
	colExpr int
	present []int
}

// targetDataColumns maps a bitmap indexed by the columns streamed from the
// source (tp.Fields) onto the target table's column expressions
// (tpb.colExprs), so that bit i of the result says whether colExprs[i] can be
// generated from the row event. A target column is present when every streamed
// column that its expression uses is present, and absent when none is (nothing
// it reads changed). Expressions with a mix of present and absent inputs are
// recorded in mixed and left absent; see partialQueryDataColumns. The column
// names are resolved by walking the expression rather than using
// colExpr.references: for a rename like "convert(c1 using utf8mb4) as c2" the
// references map holds the alias c2 while the stream carries the source column
// c1. Expressions that use no streamed column, such as constants in a
// Materialize filter, always have a value and are marked as present.
func (tp *TablePlan) targetDataColumns(streamed *binlogdatapb.RowChange_Bitmap) (*mappedDataColumns, error) {
	fieldIndexes := make(map[string]int, len(tp.Fields))
	for i, field := range tp.Fields {
		fieldIndexes[strings.ToLower(field.Name)] = i
	}
	colExprs := tp.TablePlanBuilder.colExprs
	mapped := &mappedDataColumns{
		target: &binlogdatapb.RowChange_Bitmap{
			Count: int64(len(colExprs)),
			Cols:  make([]byte, (len(colExprs)+7)/8),
		},
	}
	for i, cexpr := range colExprs {
		var presentInputs, absentInputs []int
		if cexpr.expr != nil {
			err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				col, ok := node.(*sqlparser.ColName)
				if !ok {
					return true, nil
				}
				idx, ok := fieldIndexes[col.Name.Lowered()]
				if !ok {
					return false, vterrors.Errorf(vtrpcpb.Code_INTERNAL,
						"column %s used by target column %s of table %s is not among the streamed fields",
						col.Name.String(), cexpr.colName.String(), tp.TargetName)
				}
				if int64(idx) >= streamed.Count || !isBitSet(streamed.Cols, idx) {
					absentInputs = append(absentInputs, idx)
				} else {
					presentInputs = append(presentInputs, idx)
				}
				return true, nil
			}, cexpr.expr)
			if err != nil {
				return nil, err
			}
		}
		switch {
		case len(absentInputs) == 0:
			setBit(mapped.target.Cols, i, true)
		case len(presentInputs) > 0:
			mapped.mixed = append(mapped.mixed, mixedColExpr{colExpr: i, present: presentInputs})
		}
	}
	return mapped, nil
}

// mappedDataColumnsFor returns the cached projection of the streamed bitmap
// onto the target expressions, computing it on first use.
func (tp *TablePlan) mappedDataColumnsFor(streamed *binlogdatapb.RowChange_Bitmap) (*mappedDataColumns, error) {
	key := hex.EncodeToString(streamed.Cols)
	if mapped, ok := tp.PartialBitmaps[key]; ok {
		return mapped, nil
	}
	mapped, err := tp.targetDataColumns(streamed)
	if err != nil {
		return nil, err
	}
	tp.PartialBitmaps[key] = mapped
	return mapped, nil
}

// checkMixedColExprs rejects a row change when a target expression that uses
// both present and absent streamed columns has a present input whose value
// differs between the before and after images, or whose before-image value is
// unknown: the expression's new value cannot be computed without the absent
// column. Under NOBLOB the before image omits BLOB/TEXT columns whether or not
// they changed, and an omitted value is indistinguishable from a real NULL in
// the row bytes, so BeforeDataColumns must be consulted before treating a
// present input as unchanged. When every present input is known in the before
// image and none of them changed, the expression's value did not change either,
// so leaving it out of the partial query is correct. As with every column that
// a partial image leaves out, the expression is then not re-evaluated on the
// target: a filter using a non-deterministic function (uuid(), now(), ...) is
// not reproducible under any row image mode and is not made so here.
func (tp *TablePlan) checkMixedColExprs(mapped *mappedDataColumns, rowChange *binlogdatapb.RowChange) error {
	if len(mapped.mixed) == 0 {
		return nil
	}
	colExprs := tp.TablePlanBuilder.colExprs
	if rowChange.Before == nil {
		return tp.missingValueError(colExprs[mapped.mixed[0].colExpr])
	}
	before := sqltypes.MakeRowTrusted(tp.Fields, rowChange.Before)
	after := sqltypes.MakeRowTrusted(tp.Fields, rowChange.After)
	for _, m := range mapped.mixed {
		for _, idx := range m.present {
			// BeforeDataColumns is projected into tp.Fields order (see #21067).
			// When it is absent we keep the byte comparison below, which is the
			// pre-#21067 behavior against an older source.
			if beforeCols := rowChange.BeforeDataColumns; beforeCols != nil && beforeCols.Count > 0 {
				if int64(idx) >= beforeCols.Count || !isBitSet(beforeCols.Cols, idx) {
					return tp.missingValueError(colExprs[m.colExpr])
				}
			}
			if idx >= len(before) || idx >= len(after) ||
				before[idx].IsNull() != after[idx].IsNull() ||
				!bytes.Equal(before[idx].Raw(), after[idx].Raw()) {
				return tp.missingValueError(colExprs[m.colExpr])
			}
		}
	}
	return nil
}

func (tp *TablePlan) missingValueError(cexpr *colExpr) error {
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
		"binary log event missing a needed value for %s.%s due to not using binlog-row-image=FULL; you will need to re-run the workflow with binlog-row-image=FULL",
		tp.TargetName, cexpr.colName.String())
}

// supportsPartialImages reports whether partial insert/update queries can be
// generated for the plan. The partial generators only know how to emit plain
// column expressions; count(*) and sum() need the aggregate update forms that
// the full statements carry. Such plans have never worked with partial row
// images (the generated SQL was invalid), so we reject them with a clear error
// instead. Grouped plans without aggregates, such as the Materialize filter of
// an owned lookup vindex backfill, only use plain expressions and are fine.
func (tp *TablePlan) supportsPartialImages() bool {
	tpb := tp.TablePlanBuilder
	if tpb == nil {
		return true
	}
	for _, cexpr := range tpb.colExprs {
		if cexpr.operation != opExpr {
			return false
		}
	}
	return true
}

// partialQueryDataColumns returns the after image column presence bitmap to
// generate partial insert/update queries from, indexed by the target table's
// column expressions. When the source sends the projected AfterDataColumns it
// is mapped onto the target expressions; otherwise the legacy DataColumns is
// used as-is, which is only exact for identity projections (see #21075).
func (tp *TablePlan) partialQueryDataColumns(rowChange *binlogdatapb.RowChange) (*binlogdatapb.RowChange_Bitmap, error) {
	if !tp.supportsPartialImages() {
		// Not recoverable by retrying: the workflow has to be recreated with full row images.
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"partial row image received for %s, whose filter uses aggregate expressions, which is not supported; you will need to re-run the workflow with binlog-row-image=FULL and without binlog-row-value-options=PARTIAL_JSON",
			tp.TargetName)
	}
	if rowChange.AfterDataColumns != nil && rowChange.AfterDataColumns.Count > 0 {
		mapped, err := tp.mappedDataColumnsFor(rowChange.AfterDataColumns)
		if err != nil {
			return nil, err
		}
		if err := tp.checkMixedColExprs(mapped, rowChange); err != nil {
			return nil, err
		}
		return mapped.target, nil
	}
	if rowChange.DataColumns == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "partial row event for %s has no data columns bitmap", tp.TargetName)
	}
	return rowChange.DataColumns, nil
}

func (tpb *tablePlanBuilder) generatePartialValuesPart(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter, dataColumns *binlogdatapb.RowChange_Bitmap) (*sqlparser.ParsedQuery, error) {
	bvf.mode = bvAfter
	separator := "("
	for ind, cexpr := range tpb.colExprs {
		if int64(ind) >= dataColumns.Count {
			log.Error("Ran out of columns trying to generate query for " + tpb.name.CompliantName())
			return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "unable to create partial insert query for "+tpb.name.String())
		}
		if cexpr.isGenerated || !isBitSet(dataColumns.Cols, ind) {
			continue
		}
		buf.Myprintf("%s", separator)
		separator = ","
		switch cexpr.operation {
		case opExpr:
			switch cexpr.colType {
			case querypb.Type_JSON:
				buf.Myprintf("%v", cexpr.expr)
			case querypb.Type_DATETIME:
				sourceTZ := tpb.source.SourceTimeZone
				targetTZ := tpb.source.TargetTimeZone
				if sourceTZ != "" && targetTZ != "" {
					buf.Myprintf("convert_tz(%v, '%s', '%s')", cexpr.expr, sourceTZ, targetTZ)
				} else {
					buf.Myprintf("%v", cexpr.expr)
				}
			default:
				buf.Myprintf("%v", cexpr.expr)
			}
		}
	}
	buf.Myprintf(")")
	return buf.ParsedQuery(), nil
}

func (tpb *tablePlanBuilder) generatePartialInsertPart(buf *sqlparser.TrackedBuffer, dataColumns *binlogdatapb.RowChange_Bitmap) (*sqlparser.ParsedQuery, error) {
	// Same statement shape as generateInsertPart: a fully grouped plan keeps
	// the first value it sees.
	if tpb.onInsert == insertIgnore {
		buf.Myprintf("insert ignore into %v(", tpb.name)
	} else {
		buf.Myprintf("insert into %v(", tpb.name)
	}
	separator := ""
	for ind, cexpr := range tpb.colExprs {
		if int64(ind) >= dataColumns.Count {
			log.Error("Ran out of columns trying to generate query for " + tpb.name.CompliantName())
			return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "unable to create partial insert query for "+tpb.name.String())
		}
		if cexpr.isGenerated {
			continue
		}
		if !isBitSet(dataColumns.Cols, ind) {
			continue
		}
		buf.Myprintf("%s%v", separator, cexpr.colName)
		separator = ","
	}
	buf.Myprintf(")", tpb.name)
	return buf.ParsedQuery(), nil
}

func (tpb *tablePlanBuilder) generatePartialSelectPart(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter, dataColumns *binlogdatapb.RowChange_Bitmap) (*sqlparser.ParsedQuery, error) {
	bvf.mode = bvAfter
	buf.WriteString(" select ")
	separator := ""
	for ind, cexpr := range tpb.colExprs {
		if int64(ind) >= dataColumns.Count {
			log.Error("Ran out of columns trying to generate query for " + tpb.name.CompliantName())
			return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "unable to create partial insert query for "+tpb.name.String())
		}
		if cexpr.isGenerated {
			continue
		}
		if !isBitSet(dataColumns.Cols, ind) {
			continue
		}
		buf.Myprintf("%s", separator)
		separator = ", "
		buf.Myprintf("%v", cexpr.expr)
	}
	buf.WriteString(" from dual where ")
	tpb.generatePKConstraint(buf, bvf)
	return buf.ParsedQuery(), nil
}

func (tpb *tablePlanBuilder) createPartialInsertQuery(dataColumns *binlogdatapb.RowChange_Bitmap) (*sqlparser.ParsedQuery, error) {
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)

	if _, err := tpb.generatePartialInsertPart(buf, dataColumns); err != nil {
		return nil, err
	}
	if tpb.lastpk == nil {
		// If there's no lastpk, generate straight values.
		buf.Myprintf(" values ", tpb.name)
		if _, err := tpb.generatePartialValuesPart(buf, bvf, dataColumns); err != nil {
			return nil, err
		}
	} else {
		// If there is a lastpk, generate values as a select from dual
		// where the pks < lastpk
		if _, err := tpb.generatePartialSelectPart(buf, bvf, dataColumns); err != nil {
			return nil, err
		}
	}
	tpb.generatePartialOnDupPart(buf, dataColumns)
	return buf.ParsedQuery(), nil
}

// generatePartialOnDupPart is generateOnDupPart restricted to the columns
// present in the image, so that a partial INSERT for a grouped plan keeps the
// "last value wins" semantics of the full statement. Aggregate plans never
// reach the partial generators (see supportsPartialImages).
func (tpb *tablePlanBuilder) generatePartialOnDupPart(buf *sqlparser.TrackedBuffer, dataColumns *binlogdatapb.RowChange_Bitmap) {
	if tpb.onInsert != insertOnDup {
		return
	}
	separator := " on duplicate key update "
	for ind, cexpr := range tpb.colExprs {
		if cexpr.isGrouped || cexpr.isPK || cexpr.isGenerated || !isBitSet(dataColumns.Cols, ind) {
			continue
		}
		buf.Myprintf("%s%v=values(%v)", separator, cexpr.colName, cexpr.colName)
		separator = ", "
	}
}

// createPartialUpdateQuery generates the UPDATE for a partial row image. It
// returns a nil query and no error when none of the writable target columns is
// present in the image, e.g. when the only change in the source row was to a
// column that the filter does not select: there is nothing to update on the
// target and the row event should be treated as a no-op. For a fully grouped
// plan the UPDATE is an "insert ignore", as in generateUpdateStatement: the
// first value wins and an existing row is left alone.
func (tpb *tablePlanBuilder) createPartialUpdateQuery(dataColumns *binlogdatapb.RowChange_Bitmap) (*sqlparser.ParsedQuery, error) {
	if tpb.onInsert == insertIgnore {
		return tpb.createPartialInsertQuery(dataColumns)
	}
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)
	buf.Myprintf("update %v set ", tpb.name)
	separator := ""
	for i, cexpr := range tpb.colExprs {
		if int64(i) >= dataColumns.Count {
			log.Error("Ran out of columns trying to generate query for " + tpb.name.CompliantName())
			return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "unable to create partial update query for "+tpb.name.String())
		}
		if cexpr.isPK || cexpr.isGenerated || !isBitSet(dataColumns.Cols, i) {
			continue
		}
		buf.Myprintf("%s%v=", separator, cexpr.colName)
		separator = ", "
		switch cexpr.operation {
		case opExpr:
			bvf.mode = bvAfter
			switch cexpr.colType {
			case querypb.Type_JSON:
				buf.Myprintf("%v", cexpr.expr)
			case querypb.Type_DATETIME:
				sourceTZ := tpb.source.SourceTimeZone
				targetTZ := tpb.source.TargetTimeZone
				if sourceTZ != "" && targetTZ != "" {
					buf.Myprintf("convert_tz(%v, '%s', '%s')", cexpr.expr, sourceTZ, targetTZ)
				} else {
					buf.Myprintf("%v", cexpr.expr)
				}
			default:
				buf.Myprintf("%v", cexpr.expr)
			}
		}
	}
	if separator == "" {
		// No writable target column is present in the image.
		return nil, nil
	}
	tpb.generateWhere(buf, bvf)
	return buf.ParsedQuery(), nil
}

// getPartialInsertQuery returns the INSERT for a partial after image. A column
// that is absent from a true INSERT's image was not set by the source statement
// and took its default there, so leaving it out lets it take the target's
// default too. The other way to receive an after-only partial image, an UPDATE
// whose row moves into the target key range, is rejected by the vstreamer
// (see processRowEvent): there the omitted columns hold values we do not have.
// Expressions that mix present and absent inputs are rejected by
// partialQueryDataColumns since there is no before image to compare against.
func (tp *TablePlan) getPartialInsertQuery(rowChange *binlogdatapb.RowChange) (*sqlparser.ParsedQuery, error) {
	dataColumns, err := tp.partialQueryDataColumns(rowChange)
	if err != nil {
		return nil, err
	}
	key := hex.EncodeToString(dataColumns.Cols)
	ins, ok := tp.PartialInserts[key]
	if ok {
		return ins, nil
	}
	ins, err = tp.TablePlanBuilder.createPartialInsertQuery(dataColumns)
	if err != nil {
		return nil, err
	}
	if ins == nil {
		return ins, vterrors.New(vtrpcpb.Code_INTERNAL, "unable to create partial insert query for "+tp.TargetName)
	}
	tp.PartialInserts[key] = ins
	tp.Stats.PartialQueryCacheSize.Add([]string{"insert"}, 1)
	return ins, nil
}

func (tp *TablePlan) getPartialUpdateQuery(rowChange *binlogdatapb.RowChange) (*sqlparser.ParsedQuery, error) {
	dataColumns, err := tp.partialQueryDataColumns(rowChange)
	if err != nil {
		return nil, err
	}
	key := hex.EncodeToString(dataColumns.Cols)
	upd, ok := tp.PartialUpdates[key]
	if ok {
		return upd, nil
	}
	upd, err = tp.TablePlanBuilder.createPartialUpdateQuery(dataColumns)
	if err != nil {
		return nil, err
	}
	if upd == nil {
		// Nothing to update on the target for this image; not worth caching.
		return nil, nil
	}
	tp.PartialUpdates[key] = upd
	tp.Stats.PartialQueryCacheSize.Add([]string{"update"}, 1)
	return upd, nil
}
