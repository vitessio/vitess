//go:build gofuzz
// +build gofuzz

/*
Copyright 2021 The Vitess Authors.
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
	"context"
	"sync"
	"testing"

	fuzz "github.com/AdaLogics/go-fuzz-headers"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var initter sync.Once

func initForFuzzing() {
	testing.Init()
}

// FuzzGetPlan implements a fuzzer that tests GetPlan.
func FuzzGetPlan(data []byte) int {
	initter.Do(initForFuzzing)
	t := &testing.T{}
	f := fuzz.NewConsumer(data)
	query1, err := f.GetSQLString()
	if err != nil {
		return 0
	}
	query2, err := f.GetString()
	if err != nil {
		return 0
	}
	db := fakesqldb.New(t)
	defer db.Close()

	// Add a query
	db.AddQuery(query1, &sqltypes.Result{})

	// Set up the environment
	config := tabletenv.NewDefaultConfig()
	config.DB = newDBConfigs(db)
	env := tabletenv.NewEnv(config, "TabletServerTest")
	se := schema.NewEngine(env)
	qe := NewQueryEngine(env, se)
	defer qe.Close()
	qe.Open()

	logStats := tabletenv.NewLogStats(context.Background(), "GetPlanStats")
	qe.SetQueryPlanCacheCap(1024)

	// Call target
	_, _ = qe.GetPlan(context.Background(), logStats, query2, true)
	return 1
}
