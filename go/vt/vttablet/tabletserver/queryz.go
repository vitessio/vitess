/*
Copyright 2019 The Vitess Authors.

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

package tabletserver

import (
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"time"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logz"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
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
			<th>MySQL Time</th>
			<th>Rows</th>
			<th>Errors</th>
			<th>Time per query</th>
			<th>MySQL Time per query</th>
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
			<td>{{.MysqlTime}}</td>
			<td>{{.Rows}}</td>
			<td>{{.Errors}}</td>
			<td>{{.TimePQ}}</td>
			<td>{{.MysqlTimePQ}}</td>
			<td>{{.RowsPQ}}</td>
			<td>{{.ErrorsPQ}}</td>
		</tr>
	`))
)

// queryzRow is used for rendering query stats
// using go's template.
type queryzRow struct {
	Query     string
	Table     string
	Plan      planbuilder.PlanType
	Reason    planbuilder.ReasonType
	Count     int64
	tm        time.Duration
	mysqlTime time.Duration
	Rows      int64
	Errors    int64
	Color     string
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

// MysqlTime returns the MySQL time as a string.
func (qzs *queryzRow) MysqlTime() string {
	return fmt.Sprintf("%.6f", float64(qzs.mysqlTime)/1e9)
}

// MysqlTimePQ returns the time per query as a string.
func (qzs *queryzRow) MysqlTimePQ() string {
	val := float64(qzs.mysqlTime) / (1e9 * float64(qzs.Count))
	return fmt.Sprintf("%.6f", val)
}

// RowsPQ returns the row count per query as a string.
func (qzs *queryzRow) RowsPQ() string {
	val := float64(qzs.Rows) / float64(qzs.Count)
	return fmt.Sprintf("%.6f", val)
}

// ErrorsPQ returns the error count per query as a string.
func (qzs *queryzRow) ErrorsPQ() string {
	return fmt.Sprintf("%.6f", float64(qzs.Errors)/float64(qzs.Count))
}

type queryzSorter struct {
	rows []*queryzRow
	less func(row1, row2 *queryzRow) bool
}

func (s *queryzSorter) Len() int           { return len(s.rows) }
func (s *queryzSorter) Swap(i, j int)      { s.rows[i], s.rows[j] = s.rows[j], s.rows[i] }
func (s *queryzSorter) Less(i, j int) bool { return s.less(s.rows[i], s.rows[j]) }

func queryzHandler(qe *QueryEngine, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	logz.StartHTMLTable(w)
	defer logz.EndHTMLTable(w)
	w.Write(queryzHeader)

	keys := qe.plans.Keys()
	sorter := queryzSorter{
		rows: make([]*queryzRow, 0, len(keys)),
		less: func(row1, row2 *queryzRow) bool {
			return row1.timePQ() > row2.timePQ()
		},
	}
	for _, v := range qe.plans.Keys() {
		plan := qe.peekQuery(v)
		if plan == nil {
			continue
		}
		Value := &queryzRow{
			Query:  logz.Wrappable(sqlparser.TruncateForUI(v)),
			Table:  plan.TableName().String(),
			Plan:   plan.PlanID,
			Reason: plan.Reason,
		}
		Value.Count, Value.tm, Value.mysqlTime, Value.Rows, Value.Errors = plan.Stats()
		var timepq time.Duration
		if Value.Count != 0 {
			timepq = time.Duration(int64(Value.tm) / Value.Count)
		}
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
		if err := queryzTmpl.Execute(w, Value); err != nil {
			log.Errorf("queryz: couldn't execute template: %v", err)
		}
	}
}
