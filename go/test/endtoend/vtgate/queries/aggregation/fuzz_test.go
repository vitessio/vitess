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

package aggregation

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"golang.org/x/exp/maps"
)

type (
	tableT struct {
		name    string
		columns []string
	}
)

func TestFuzzAggregations(t *testing.T) {
	// This test randomizes values and queries, and checks that mysql returns the same values that Vitess does
	mcmp, closer := start(t)
	defer closer()

	noOfRows := rand.Intn(300)
	var values []string
	for i := 0; i < noOfRows; i++ {
		values = append(values, fmt.Sprintf("(%d, 'name%d', 'value%d', %d)", i, i, i, i))
	}
	t1Insert := fmt.Sprintf("insert into t1 (t1_id, name, value, shardKey) values %s;", strings.Join(values, ","))
	values = nil
	noOfRows = rand.Intn(300)
	for i := 0; i < noOfRows; i++ {
		values = append(values, fmt.Sprintf("(%d, %d)", i, i))
	}
	t2Insert := fmt.Sprintf("insert into t2 (id, shardKey) values %s;", strings.Join(values, ","))

	mcmp.Exec(t1Insert)
	mcmp.Exec(t2Insert)

	t.Cleanup(func() {
		if t.Failed() {
			fmt.Println(t1Insert)
			fmt.Println(t2Insert)
		}
	})

	schema := map[string]tableT{
		"t1": {name: "t1", columns: []string{"t1_id", "name", "value", "shardKey"}},
		"t2": {name: "t2", columns: []string{"id", "shardKey"}},
	}

	endBy := time.Now().Add(1 * time.Second)
	schemaTables := maps.Values(schema)

	for time.Now().Before(endBy) || t.Failed() {
		tables := createTables(schemaTables)
		query := randomQuery(tables, 3, 3)
		fmt.Println(query)
		mcmp.Exec(query)
	}
}

func randomQuery(tables []tableT, maxAggrs, maxGroupBy int) string {
	randomCol := func(tblIdx int) string {
		tbl := tables[tblIdx]
		col := randomEl(tbl.columns)
		return fmt.Sprintf("tbl%d.%s", tblIdx, col)
	}
	predicates := createPredicates(tables, randomCol)
	aggregates := createAggregations(tables, maxAggrs, randomCol)
	grouping := createGroupBy(tables, maxGroupBy, randomCol)
	sel := "select /*vt+ PLANNER=Gen4 */ " + strings.Join(aggregates, ", ") + " from "

	var tbls []string
	for i, s := range tables {
		tbls = append(tbls, fmt.Sprintf("%s as tbl%d", s.name, i))
	}
	sel += strings.Join(tbls, ", ")

	if len(predicates) > 0 {
		sel += " where "
		sel += strings.Join(predicates, " and ")
	}
	if len(grouping) > 0 {
		sel += " group by "
		sel += strings.Join(grouping, ", ")
	}
	return sel
}

func createGroupBy(tables []tableT, maxGB int, randomCol func(tblIdx int) string) (grouping []string) {
	noOfGBs := rand.Intn(maxGB)
	for i := 0; i < noOfGBs; i++ {
		tblIdx := rand.Intn(len(tables))
		grouping = append(grouping, randomCol(tblIdx))
	}
	return
}

func createAggregations(tables []tableT, maxAggrs int, randomCol func(tblIdx int) string) (aggregates []string) {
	noOfAggrs := rand.Intn(maxAggrs) + 1
	for i := 0; i < noOfAggrs; i++ {
		tblIdx := rand.Intn(len(tables))
		e := randomCol(tblIdx)

		switch rand.Intn(5) {
		case 0:
			aggregates = append(aggregates, "count(*)")
		case 1:
			aggregates = append(aggregates, fmt.Sprintf("max(%s)", e))
		case 2:
			aggregates = append(aggregates, fmt.Sprintf("min(%s)", e))
		case 3:
			aggregates = append(aggregates, fmt.Sprintf("sum(%s)", e))
		case 4:
			aggregates = append(aggregates, fmt.Sprintf("count(%s)", e))
		case 5:
			aggregates = append(aggregates, e)
		}
	}
	return aggregates
}

func createTables(schemaTables []tableT) []tableT {
	noOfTables := rand.Intn(2) + 1
	var tables []tableT

	for i := 0; i < noOfTables; i++ {
		tables = append(tables, randomEl(schemaTables))
	}
	return tables
}

func createPredicates(tables []tableT, randomCol func(tblIdx int) string) (predicates []string) {
	for idx1 := range tables {
		for idx2 := range tables {
			if idx1 == idx2 {
				continue
			}
			noOfPredicates := rand.Intn(2)
			for i := 0; i < noOfPredicates; i++ {
				predicates = append(predicates, fmt.Sprintf("%s = %s", randomCol(idx1), randomCol(idx2)))
			}
		}
	}
	return predicates
}

func randomEl[K any](in []K) K {
	return in[rand.Intn(len(in))]
}
