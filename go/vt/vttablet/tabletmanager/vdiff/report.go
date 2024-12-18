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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

const (
	truncatedNotation = "...[TRUNCATED]"
)

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
	ExtraRowsSourceDiffs []*RowDiff      `json:"ExtraRowsSourceSample,omitempty"`
	ExtraRowsTargetDiffs []*RowDiff      `json:"ExtraRowsTargetSample,omitempty"`
	MismatchedRowsDiffs  []*DiffMismatch `json:"MismatchedRowsSample,omitempty"`
}

type ProgressReport struct {
	Percentage float64
	ETA        string `json:"ETA,omitempty"` // a formatted date
}

// DiffMismatch is a sample of row diffs between source and target.
type DiffMismatch struct {
	Source *RowDiff `json:"Source,omitempty"`
	Target *RowDiff `json:"Target,omitempty"`
}

// RowDiff is a row that didn't match as part of the comparison.
type RowDiff struct {
	Row   map[string]string `json:"Row,omitempty"`
	Query string            `json:"Query,omitempty"`
}

func (td *tableDiffer) genRowDiff(queryStmt string, row []sqltypes.Value, opts *tabletmanagerdatapb.VDiffReportOptions) (*RowDiff, error) {
	rd := &RowDiff{}
	rd.Row = make(map[string]string)
	statement, err := td.wd.ct.vde.parser.Parse(queryStmt)
	if err != nil {
		return nil, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("unexpected: %+v", sqlparser.String(statement))
	}

	if opts.GetDebugQuery() {
		rd.Query = td.genDebugQueryDiff(sel, row, opts.GetOnlyPks())
	}

	addVal := func(index int, truncateAt int) {
		buf := sqlparser.NewTrackedBuffer(nil)
		sel.SelectExprs[index].Format(buf)
		col := buf.String()
		// Let's truncate if it's really worth it to avoid losing
		// value for a few chars.
		if truncateAt > 0 && row[index].Len() >= truncateAt+len(truncatedNotation)+20 {
			rd.Row[col] = row[index].ToString()[:truncateAt] + truncatedNotation
		} else {
			rd.Row[col] = row[index].ToString()
		}
	}

	// Include PK columns first and do not truncate them so that
	// the user can always at a minimum identify the row for
	// further investigation.
	pks := make(map[int]struct{}, len(td.tablePlan.selectPks))
	for _, pkI := range td.tablePlan.selectPks {
		addVal(pkI, 0)
		pks[pkI] = struct{}{}
	}

	if opts.GetOnlyPks() {
		return rd, nil
	}

	truncateAt := int(opts.GetRowDiffColumnTruncateAt())
	for i := range sel.SelectExprs {
		if _, pk := pks[i]; !pk {
			addVal(i, truncateAt)
		}
	}

	return rd, nil
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
