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
	"fmt"

	"vitess.io/vitess/go/vt/vttablet"

	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// isBitSet returns true if the bit at index is set
func isBitSet(data []byte, index int) bool {
	byteIndex := index / 8
	bitMask := byte(1 << (uint(index) & 0x7))
	return data[byteIndex]&bitMask > 0
}

func (tp *TablePlan) isPartial(rowChange *binlogdatapb.RowChange) bool {
	if (vttablet.VReplicationExperimentalFlags /**/ & /**/ vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage) == 0 ||
		rowChange.DataColumns == nil ||
		rowChange.DataColumns.Count == 0 {

		return false
	}
	return true
}

func (tpb *tablePlanBuilder) generatePartialValuesPart(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter, dataColumns *binlogdatapb.RowChange_Bitmap) *sqlparser.ParsedQuery {
	bvf.mode = bvAfter
	separator := "("
	for ind, cexpr := range tpb.colExprs {
		if tpb.isColumnGenerated(cexpr.colName) {
			continue
		}
		if !isBitSet(dataColumns.Cols, ind) {
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
		if tpb.isColumnGenerated(cexpr.colName) {
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
		if tpb.isColumnGenerated(cexpr.colName) {
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
		if cexpr.isPK {
			continue
		}
		if tpb.isColumnGenerated(cexpr.colName) {
			continue
		}
		if int64(i) >= dataColumns.Count {
			log.Errorf("Ran out of columns trying to generate query for %s", tpb.name.CompliantName())
			return nil
		}
		if !isBitSet(dataColumns.Cols, i) {
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
func (tp *TablePlan) getPartialInsertQuery(dataColumns *binlogdatapb.RowChange_Bitmap) (*sqlparser.ParsedQuery, error) {
	key := fmt.Sprintf("%x", dataColumns.Cols)
	ins, ok := tp.PartialInserts[key]
	if ok {
		return ins, nil
	}
	ins = tp.TablePlanBuilder.createPartialInsertQuery(dataColumns)
	if ins == nil {
		return ins, vterrors.New(vtrpcpb.Code_INTERNAL, fmt.Sprintf("unable to create partial insert query for %s", tp.TargetName))
	}
	tp.PartialInserts[key] = ins
	tp.Stats.PartialQueryCacheSize.Add([]string{"insert"}, 1)
	return ins, nil
}

func (tp *TablePlan) getPartialUpdateQuery(dataColumns *binlogdatapb.RowChange_Bitmap) (*sqlparser.ParsedQuery, error) {
	key := fmt.Sprintf("%x", dataColumns.Cols)
	upd, ok := tp.PartialUpdates[key]
	if ok {
		return upd, nil
	}
	upd = tp.TablePlanBuilder.createPartialUpdateQuery(dataColumns)
	if upd == nil {
		return upd, vterrors.New(vtrpcpb.Code_INTERNAL, fmt.Sprintf("unable to create partial update query for %s", tp.TargetName))
	}
	tp.PartialUpdates[key] = upd
	tp.Stats.PartialQueryCacheSize.Add([]string{"update"}, 1)
	return upd, nil
}
