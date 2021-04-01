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

package vtgate

import (
	"fmt"
	"html/template"
	"net/http"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/logz"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type statsResults struct {
	TotalReadOnlyTime  time.Duration
	TotalScatterTime   time.Duration
	PercentTimeScatter float64
	Items              []*statsResultItem
}

// using this intermediate data structure to make testing easier
type statsResultItem struct {
	Query                  string
	AvgTimePerQuery        time.Duration
	PercentTimeOfReads     float64
	PercentTimeOfScatters  float64
	PercentCountOfReads    float64
	PercentCountOfScatters float64
	From                   string
	Count                  uint64
}

func (e *Executor) gatherScatterStats() (statsResults, error) {
	scatterExecTime := time.Duration(0)
	readOnlyTime := time.Duration(0)
	scatterCount := uint64(0)
	readOnlyCount := uint64(0)
	totalExecTime := time.Duration(0)
	totalCount := uint64(0)

	var err error
	plans := make([]*engine.Plan, 0)
	routes := make([]*engine.Route, 0)
	// First we go over all plans and collect statistics and all query plans for scatter queries
	e.plans.ForEach(func(value interface{}) bool {
		plan := value.(*engine.Plan)
		scatter := engine.Find(findScatter, plan.Instructions)
		readOnly := !engine.Exists(isUpdating, plan.Instructions)
		isScatter := scatter != nil

		if isScatter {
			route, isRoute := scatter.(*engine.Route)
			if !isRoute {
				err = vterrors.Errorf(vtrpc.Code_INTERNAL, "expected a route, but found a %v", scatter)
				return false
			}
			plans = append(plans, plan)
			routes = append(routes, route)
			scatterExecTime += time.Duration(atomic.LoadUint64(&plan.ExecTime))
			scatterCount += atomic.LoadUint64(&plan.ExecCount)
		}
		if readOnly {
			readOnlyTime += time.Duration(atomic.LoadUint64(&plan.ExecTime))
			readOnlyCount += atomic.LoadUint64(&plan.ExecCount)
		}

		totalExecTime += time.Duration(atomic.LoadUint64(&plan.ExecTime))
		totalCount += atomic.LoadUint64(&plan.ExecCount)
		return true
	})
	if err != nil {
		return statsResults{}, err
	}

	// Now we'll go over all scatter queries we've found and produce result items for each
	resultItems := make([]*statsResultItem, len(plans))
	for i, plan := range plans {
		route := routes[i]
		execCount := atomic.LoadUint64(&plan.ExecCount)
		execTime := time.Duration(atomic.LoadUint64(&plan.ExecTime))

		var avgTimePerQuery int64
		if execCount != 0 {
			avgTimePerQuery = execTime.Nanoseconds() / int64(execCount)
		}
		resultItems[i] = &statsResultItem{
			Query:                  plan.Original,
			AvgTimePerQuery:        time.Duration(avgTimePerQuery),
			PercentTimeOfReads:     100 * float64(execTime) / float64(readOnlyTime),
			PercentTimeOfScatters:  100 * float64(execTime) / float64(scatterExecTime),
			PercentCountOfReads:    100 * float64(execCount) / float64(readOnlyCount),
			PercentCountOfScatters: 100 * float64(execCount) / float64(scatterCount),
			From:                   route.Keyspace.Name + "." + route.TableName,
			Count:                  execCount,
		}
	}
	result := statsResults{
		TotalReadOnlyTime:  readOnlyTime,
		TotalScatterTime:   scatterExecTime,
		PercentTimeScatter: 100 * float64(scatterExecTime) / float64(readOnlyTime),
		Items:              resultItems,
	}
	return result, nil
}

// WriteScatterStats will write an html report to the provided response writer
func (e *Executor) WriteScatterStats(w http.ResponseWriter) {
	logz.StartHTMLTable(w)
	defer logz.EndHTMLTable(w)

	results, err := e.gatherScatterStats()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	t := template.New("template")
	t, err = t.Parse(html)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	err = t.Execute(w, results)
	if err != nil {
		http.Error(w, err.Error(), 500)
	}

	_, err = w.Write([]byte(fmt.Sprintf("Percentage of time spent on scatter queries: %2.2f%%", results.PercentTimeScatter)))

	if err != nil {
		http.Error(w, err.Error(), 500)
	}

}

const html = `
<thead>
	<tr>
		<th>Query</th>
		<th># of executions</th>
		<th>Avg time/query</th>
		<th>% time of reads</th>
		<th>% time of scatters</th>
		<th>% of reads</th>
		<th>% of scatters</th>
	</tr>
</thead>
{{range .Items}}
<tr class="medium">
	<td>{{.Query}}</td>
	<td>{{.Count}}</td>
	<td>{{.AvgTimePerQuery}}</td>
	<td>{{.PercentTimeOfReads}}</td>
	<td>{{.PercentTimeOfScatters}}</td>
	<td>{{.PercentCountOfReads}}</td>
	<td>{{.PercentCountOfScatters}}</td>
</tr>
{{end}}
`

func findScatter(p engine.Primitive) bool {
	switch v := p.(type) {
	case *engine.Route:
		return v.Opcode == engine.SelectScatter
	default:
		return false
	}
}

func isUpdating(p engine.Primitive) bool {
	switch p.(type) {
	case *engine.Update, *engine.Delete, *engine.Insert:
		return true
	default:
		return false
	}
}
