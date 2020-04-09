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

	plans := make([]*engine.Plan, 0)
	routes := make([]*engine.Route, 0)
	// First we go over all plans and collect statistics and all query plans for scatter queries
	for _, item := range e.plans.Items() {
		plan := item.Value.(*engine.Plan)

		scatter := engine.Find(findScatter, plan.Instructions)
		readOnly := !engine.Exists(isUpdating, plan.Instructions)
		isScatter := scatter != nil

		if isScatter {
			route, isRoute := scatter.(*engine.Route)
			if !isRoute {
				return statsResults{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "expected a route, but found a %v", scatter)
			}
			plans = append(plans, plan)
			routes = append(routes, route)
			scatterExecTime += plan.ExecTime
			scatterCount += plan.ExecCount
		}
		if readOnly {
			readOnlyTime += plan.ExecTime
			readOnlyCount += plan.ExecCount
		}

		totalExecTime += plan.ExecTime
		totalCount += plan.ExecCount
	}

	// Now we'll go over all scatter queries we've found and produce result items for each
	resultItems := make([]*statsResultItem, len(plans))
	for i, plan := range plans {
		route := routes[i]
		resultItems[i] = &statsResultItem{
			Query:                  plan.Original,
			AvgTimePerQuery:        time.Duration(plan.ExecTime.Nanoseconds() / int64(plan.ExecCount)),
			PercentTimeOfReads:     float64(100 * plan.ExecTime / readOnlyTime),
			PercentTimeOfScatters:  float64(100 * plan.ExecTime / scatterExecTime),
			PercentCountOfReads:    float64(100 * plan.ExecCount / readOnlyCount),
			PercentCountOfScatters: float64(100 * plan.ExecCount / scatterCount),
			From:                   route.Keyspace.Name + "." + route.TableName,
			Count:                  plan.ExecCount,
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
