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

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/logz"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/utils"
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

	keys := qe.queries.Keys()
	sorter := queryzSorter{
		rows: make([]*queryzRow, 0, len(keys)),
		less: func(row1, row2 *queryzRow) bool {
			return row1.timePQ() > row2.timePQ()
		},
	}
	for _, v := range qe.queries.Keys() {
		plan := qe.peekQuery(v)
		if plan == nil {
			continue
		}
		Value := &queryzRow{
			Query:  logz.Wrappable(utils.TruncateQuery(v)),
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
