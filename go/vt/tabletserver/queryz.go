// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"time"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
)

var (
	queryzHeader = []byte(`<thead>
		<tr>
			<th>Query</th>
			<th>Table</th>
			<th>Plan</th>
			<th>Reason</th>
			<th>Count</th>
			<th>Time</th>
			<th>Rows</th>
			<th>Errors</th>
			<th>Time per query</th>
			<th>Rows per query</th>
			<th>Errors per query</th>
		</tr>
        </thead>
	`)
	queryzTmpl = template.Must(template.New("example").Parse(`
		<tr class="{{.Color}}">
			<td>{{.Query}}</td>
			<td>{{.Table}}</td>
			<td>{{.Plan}}</td>
			<td>{{.Reason}}</td>
			<td>{{.Count}}</td>
			<td>{{.Time}}</td>
			<td>{{.Rows}}</td>
			<td>{{.Errors}}</td>
			<td>{{.TimePQ}}</td>
			<td>{{.RowsPQ}}</td>
			<td>{{.ErrorsPQ}}</td>
		</tr>
	`))
)

// queryzRow is used for rendering query stats
// using go's template.
type queryzRow struct {
	Query  string
	Table  string
	Plan   planbuilder.PlanType
	Reason planbuilder.ReasonType
	Count  int64
	tm     time.Duration
	Rows   int64
	Errors int64
	Color  string
}

// Time returns the total time as a string.
func (qzs *queryzRow) Time() string {
	return fmt.Sprintf("%.6f", float64(qzs.tm)/1e9)
}

func (qzs *queryzRow) timePQ() float64 {
	return float64(qzs.tm) / (1e9 * float64(qzs.Count))
}

// TimePQ returns the time per query as a string.
func (qzs *queryzRow) TimePQ() string {
	return fmt.Sprintf("%.6f", qzs.timePQ())
}

func (qzs *queryzRow) rowsPQ() float64 {
	return float64(qzs.Rows) / float64(qzs.Count)
}

// RowsPQ returns the row count per query as a string.
func (qzs *queryzRow) RowsPQ() string {
	return fmt.Sprintf("%.6f", qzs.rowsPQ())
}

// ErrorsPQ returns the error count per query as a string.
func (qzs *queryzRow) ErrorsPQ() string {
	return fmt.Sprintf("%.6f", float64(qzs.Errors)/float64(qzs.Count))
}

type queryzSorter struct {
	rows []*queryzRow
	less func(row1, row2 *queryzRow) bool
}

func (sorter *queryzSorter) Len() int {
	return len(sorter.rows)
}

func (sorter *queryzSorter) Swap(i, j int) {
	sorter.rows[i], sorter.rows[j] = sorter.rows[j], sorter.rows[i]
}

func (sorter *queryzSorter) Less(i, j int) bool {
	return sorter.less(sorter.rows[i], sorter.rows[j])
}

func init() {
	http.HandleFunc("/queryz", queryzHandler)
}

// queryzHandler displays the query stats.
func queryzHandler(w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	startHTMLTable(w)
	defer endHTMLTable(w)
	w.Write(queryzHeader)

	si := SqlQueryRpcService.qe.schemaInfo
	keys := si.queries.Keys()
	sorter := queryzSorter{
		rows: make([]*queryzRow, 0, len(keys)),
		less: func(row1, row2 *queryzRow) bool {
			return row1.timePQ() > row2.timePQ()
		},
	}
	for _, v := range si.queries.Keys() {
		plan := si.getQuery(v)
		if plan == nil {
			continue
		}
		Value := &queryzRow{
			Query:  wrappable(v),
			Table:  plan.TableName,
			Plan:   plan.PlanId,
			Reason: plan.Reason,
		}
		Value.Count, Value.tm, Value.Rows, Value.Errors = plan.Stats()
		timepq := time.Duration(int64(Value.tm) / Value.Count)
		if timepq < 10*time.Millisecond {
			Value.Color = "low"
		} else if timepq < 100*time.Millisecond {
			Value.Color = "medium"
		} else {
			Value.Color = "high"
		}
		sorter.rows = append(sorter.rows, Value)
	}
	sort.Sort(&sorter)
	for _, Value := range sorter.rows {
		queryzTmpl.Execute(w, Value)
	}
}
