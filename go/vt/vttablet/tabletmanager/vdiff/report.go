/*
Copyright 2022 The Vitess Authors.

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

package vdiff

import (
	"fmt"
	"sort"

	"vitess.io/vitess/go/vt/logutil"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

// At most how many samples we should show for row differences in the final report
const maxVDiffReportSampleRows = 10

// DiffReport is the summary of differences for one table.
type DiffReport struct {
	TableName string

	// counts
	ProcessedRows   int64
	MatchingRows    int64
	MismatchedRows  int64
	ExtraRowsSource int64
	ExtraRowsTarget int64

	// actual data for a few sample rows
	ExtraRowsSourceDiffs []*RowDiff      `json:"ExtraRowsSourceDiffs,omitempty"`
	ExtraRowsTargetDiffs []*RowDiff      `json:"ExtraRowsTargetDiffs,omitempty"`
	MismatchedRowsSample []*DiffMismatch `json:"MismatchedRowsSample,omitempty"`
}

// DiffMismatch is a sample of row diffs between source and target.
type DiffMismatch struct {
	Source *RowDiff
	Target *RowDiff
}

// RowDiff is a row that didn't match as part of the comparison.
type RowDiff struct {
	Row   map[string]sqltypes.Value
	Query string
}

func (td *tableDiffer) genRowDiff(queryStmt string, row []sqltypes.Value, debug, onlyPks bool) (*RowDiff, error) {
	drp := &RowDiff{}
	drp.Row = make(map[string]sqltypes.Value)
	statement, err := sqlparser.Parse(queryStmt)
	if err != nil {
		return nil, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(statement))
	}

	if debug {
		drp.Query = td.genDebugQueryDiff(sel, row, onlyPks)
	}

	setVal := func(index int) {
		buf := sqlparser.NewTrackedBuffer(nil)
		sel.SelectExprs[index].Format(buf)
		col := buf.String()
		drp.Row[col] = row[index]
	}

	if onlyPks {
		for _, pkI := range td.tablePlan.selectPks {
			setVal(pkI)
		}
		return drp, nil
	}

	for i := range sel.SelectExprs {
		setVal(i)
	}

	return drp, nil
}

func (td *tableDiffer) genDebugQueryDiff(sel *sqlparser.Select, row []sqltypes.Value, onlyPks bool) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("select ")

	if onlyPks {
		for i, pkI := range td.tablePlan.selectPks {
			pk := sel.SelectExprs[pkI]
			pk.Format(buf)
			if i != len(td.tablePlan.selectPks)-1 {
				buf.Myprintf(", ")
			}
		}
	} else {
		sel.SelectExprs.Format(buf)
	}
	buf.Myprintf(" from ")
	buf.Myprintf(sqlparser.ToString(sel.From))
	buf.Myprintf(" where ")
	for i, pkI := range td.tablePlan.selectPks {
		sel.SelectExprs[pkI].Format(buf)
		buf.Myprintf("=")
		row[pkI].EncodeSQL(buf)
		if i != len(td.tablePlan.selectPks)-1 {
			buf.Myprintf(" AND ")
		}
	}
	buf.Myprintf(";")
	return buf.String()
}

// formatSampleRow returns a formatted string representing a sample mismatched row
// TODO: we need to figure out a way to leverage this with the JSON formatted
// output as it's currently generated via the stadard JSON package marshalling.
func formatSampleRow(logger logutil.Logger, rd *RowDiff, debug bool) { //nolint:unused,deadcode
	keys := make([]string, 0, len(rd.Row))
	for k := range rd.Row {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		logger.Printf("\t\t\t %s: %s\n", k, formatValue(rd.Row[k]))
	}

	if debug {
		logger.Printf("\t\tDebugQuery: %v\n", rd.Query)
	}
}

// formatValue returns a properly formatted string based on the SQL type
func formatValue(val sqltypes.Value) string {
	if val.Type() == sqltypes.Null {
		return "null (NULL_TYPE)"
	}
	if val.IsQuoted() || val.Type() == sqltypes.Bit {
		if len(val.Raw()) >= 20 {
			rawBytes := val.Raw()[:20]
			rawBytes = append(rawBytes, []byte("...[TRUNCATED]")...)
			return fmt.Sprintf("%q (%v)", rawBytes, val.Type())
		}
		return fmt.Sprintf("%q (%v)", val.Raw(), val.Type())
	}
	return fmt.Sprintf("%s (%v)", val.Raw(), val.Type())
}
