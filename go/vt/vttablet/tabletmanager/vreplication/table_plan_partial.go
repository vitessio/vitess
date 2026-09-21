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
	"encoding/hex"
	"strings"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

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

// targetDataColumns maps a bitmap indexed by the columns streamed from the
// source (tp.Fields) onto the target table's column expressions
// (tpb.colExprs), so that bit i of the result says whether colExprs[i] can be
// generated from the row event. A target column is present when every streamed
// column that its expression uses is present. The column names are resolved by
// walking the expression rather than using colExpr.references: for a rename
// like "convert(c1 using utf8mb4) as c2" the references map holds the alias c2
// while the stream carries the source column c1. Expressions that use no
// streamed column, such as constants in a Materialize filter or count(*),
// always have a value and are marked as present.
func (tp *TablePlan) targetDataColumns(streamed *binlogdatapb.RowChange_Bitmap) (*binlogdatapb.RowChange_Bitmap, error) {
	fieldIndexes := make(map[string]int, len(tp.Fields))
	for i, field := range tp.Fields {
		fieldIndexes[strings.ToLower(field.Name)] = i
	}
	colExprs := tp.TablePlanBuilder.colExprs
	target := &binlogdatapb.RowChange_Bitmap{
		Count: int64(len(colExprs)),
		Cols:  make([]byte, (len(colExprs)+7)/8),
	}
	for i, cexpr := range colExprs {
		present := true
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
					present = false
					return false, nil
				}
				return true, nil
			}, cexpr.expr)
			if err != nil {
				return nil, err
			}
		}
		setBit(target.Cols, i, present)
	}
	return target, nil
}

// partialQueryDataColumns returns the after image column presence bitmap to
// generate partial insert/update queries from, indexed by the target table's
// column expressions. When the source sends the projected AfterDataColumns it
// is mapped onto the target expressions; otherwise the legacy DataColumns is
// used as-is, which is only exact for identity projections (see #21075).
func (tp *TablePlan) partialQueryDataColumns(rowChange *binlogdatapb.RowChange) (*binlogdatapb.RowChange_Bitmap, error) {
	if rowChange.AfterDataColumns != nil && rowChange.AfterDataColumns.Count > 0 {
		return tp.targetDataColumns(rowChange.AfterDataColumns)
	}
	if rowChange.DataColumns == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "partial row event for %s has no data columns bitmap", tp.TargetName)
	}
	return rowChange.DataColumns, nil
}

func (tpb *tablePlanBuilder) generatePartialValuesPart(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter, dataColumns *binlogdatapb.RowChange_Bitmap) *sqlparser.ParsedQuery {
	bvf.mode = bvAfter
	separator := "("
	for ind, cexpr := range tpb.colExprs {
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
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generatePartialInsertPart(buf *sqlparser.TrackedBuffer, dataColumns *binlogdatapb.RowChange_Bitmap) *sqlparser.ParsedQuery {
	buf.Myprintf("insert into %v(", tpb.name)
	separator := ""
	for ind, cexpr := range tpb.colExprs {
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
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generatePartialSelectPart(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter, dataColumns *binlogdatapb.RowChange_Bitmap) *sqlparser.ParsedQuery {
	bvf.mode = bvAfter
	buf.WriteString(" select ")
	separator := ""
	for ind, cexpr := range tpb.colExprs {
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
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) createPartialInsertQuery(dataColumns *binlogdatapb.RowChange_Bitmap) *sqlparser.ParsedQuery {
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)

	tpb.generatePartialInsertPart(buf, dataColumns)
	if tpb.lastpk == nil {
		// If there's no lastpk, generate straight values.
		buf.Myprintf(" values ", tpb.name)
		tpb.generatePartialValuesPart(buf, bvf, dataColumns)
	} else {
		// If there is a lastpk, generate values as a select from dual
		// where the pks < lastpk
		tpb.generatePartialSelectPart(buf, bvf, dataColumns)
	}
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) createPartialUpdateQuery(dataColumns *binlogdatapb.RowChange_Bitmap) *sqlparser.ParsedQuery {
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)
	buf.Myprintf("update %v set ", tpb.name)
	separator := ""
	for i, cexpr := range tpb.colExprs {
		if int64(i) >= dataColumns.Count {
			log.Error("Ran out of columns trying to generate query for " + tpb.name.CompliantName())
			return nil
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
	tpb.generateWhere(buf, bvf)
	return buf.ParsedQuery()
}

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
	ins = tp.TablePlanBuilder.createPartialInsertQuery(dataColumns)
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
	upd = tp.TablePlanBuilder.createPartialUpdateQuery(dataColumns)
	if upd == nil {
		return upd, vterrors.New(vtrpcpb.Code_INTERNAL, "unable to create partial update query for "+tp.TargetName)
	}
	tp.PartialUpdates[key] = upd
	tp.Stats.PartialQueryCacheSize.Add([]string{"update"}, 1)
	return upd, nil
}
