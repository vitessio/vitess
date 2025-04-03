/*
Copyright 2025 The Vitess Authors.

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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestShouldOptimizePlan tests the shouldOptimizePlan function
func TestShouldOptimizePlan(t *testing.T) {
	tcases := []struct {
		planType                    engine.PlanType
		preparedPlan, isExecutePath bool
		expected                    bool
	}{{
		planType:      engine.PlanPassthrough,
		preparedPlan:  true,
		isExecutePath: true,
		expected:      false,
	}, {
		planType:      engine.PlanScatter,
		preparedPlan:  true,
		isExecutePath: true,
		expected:      false,
	}, {
		planType:      engine.PlanOnlineDDL,
		preparedPlan:  true,
		isExecutePath: true,
		expected:      false,
	}, {
		planType:      engine.PlanJoinOp,
		preparedPlan:  true,
		isExecutePath: true,
		expected:      true,
	}, {
		planType:      engine.PlanComplex,
		preparedPlan:  true,
		isExecutePath: true,
		expected:      true,
	}, {
		planType:      engine.PlanJoinOp,
		preparedPlan:  false,
		isExecutePath: true,
		expected:      false,
	}, {
		planType:      engine.PlanComplex,
		preparedPlan:  true,
		isExecutePath: false,
		expected:      false,
	}}
	for _, tcase := range tcases {
		t.Run(fmt.Sprintf("%v %v %v", tcase.planType, tcase.preparedPlan, tcase.isExecutePath), func(t *testing.T) {
			plan := &engine.Plan{
				Type: tcase.planType,
			}
			shouldOptimize := shouldOptimizePlan(tcase.preparedPlan, tcase.isExecutePath, plan)
			require.Equal(t, tcase.expected, shouldOptimize)
			// Checking again should always return false for all cases
			shouldOptimize = shouldOptimizePlan(tcase.preparedPlan, tcase.isExecutePath, plan)
			require.False(t, shouldOptimize)
		})
	}
}

// TestDeferredOptimization tests plan output with deferred optimization.
func TestDeferredOptimization(t *testing.T) {
	ctx := context.Background()
	env := vtenv.NewTestEnv()
	cfg := &evalengine.Config{
		Environment: env,
		Collation:   collations.MySQL8().DefaultConnectionCharset(),
	}
	parser := sqlparser.NewTestParser()
	exprEnv := evalengine.EmptyExpressionEnv(env)
	queryLogger := streamlog.New[*logstats.LogStats]("VTGate", queryLogBufferSize)
	result := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			KsTestSharded:   loadKeyspace(executorVSchema),
			KsTestUnsharded: loadKeyspace(unshardedVSchema),
		},
	}

	resolver := newTestResolver(ctx, nil, nil, "")
	executor := Executor{
		config:      createExecutorConfig(),
		env:         env,
		resolver:    resolver,
		vschema:     vindexes.BuildVSchema(result, parser),
		queryLogger: queryLogger,
		plans:       DefaultPlanCache(),
	}
	defer executor.plans.Close()
	opts := jsondiff.DefaultConsoleOptions()
	for _, tcase := range readJSONTests("prepared_statements.json") {
		testName := tcase.Comment
		if testName == "" {
			testName = tcase.Query
		}
		executor.ClearPlans()
		t.Run(testName, func(t *testing.T) {
			sess := executorcontext.NewSafeSession(&vtgatepb.Session{})
			ls := logstats.NewLogStats(ctx, "test", tcase.Query, "", nil, streamlog.GetQueryLogConfig())
			_, _, _, _ = executor.fetchOrCreatePlan(ctx, sess, tcase.Query, nil, false, true, ls, false)

			bv := make(map[string]*querypb.BindVariable)
			for i, from := range tcase.BindVars {
				key := fmt.Sprintf("v%d", i+1)
				bv[key] = makeBindVar(t, from, cfg, exprEnv)
			}

			plan, _, _, err := executor.fetchOrCreatePlan(ctx, sess, tcase.Query, bv, false, true, ls, true)
			out := getPlanOrErrorOutput(err, plan)

			compare, s := jsondiff.Compare(tcase.Plan, []byte(out), &opts)
			if compare != jsondiff.FullMatch {
				t.Errorf("Plan does not match for %s\n%s", tcase.Query, s)
				fmt.Println(out)
			}
		})
	}
}

func makeBindVar(t *testing.T, e string, cfg *evalengine.Config, env *evalengine.ExpressionEnv) *querypb.BindVariable {
	parser := sqlparser.NewTestParser()
	expr, err := parser.ParseExpr(e)
	require.NoError(t, err)
	evalExpr, err := evalengine.Translate(expr, cfg)
	require.NoError(t, err)
	evalResult, err := env.Evaluate(evalExpr)
	require.NoError(t, err)
	return sqltypes.ValueBindVariable(evalResult.Value(collations.Unknown))
}

func getPlanOrErrorOutput(err error, plan *engine.Plan) string {
	if err != nil {
		return "\"" + err.Error() + "\""
	}
	b := new(bytes.Buffer)
	enc := json.NewEncoder(b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	err = enc.Encode(plan)
	if err != nil {
		panic(err)
	}
	return b.String()
}

func loadKeyspace(schema string) *vschemapb.Keyspace {
	var vs vschemapb.Keyspace
	if err := json2.UnmarshalPB([]byte(schema), &vs); err != nil {
		panic(err)
	}
	return &vs
}

type PlanTest struct {
	Comment  string          `json:"comment,omitempty"`
	Query    string          `json:"query,omitempty"`
	BindVars []string        `json:"bindvars,omitempty"`
	Plan     json.RawMessage `json:"plan,omitempty"`
	Skip     bool            `json:"skip,omitempty"`
	SkipE2E  bool            `json:"skip_e2e,omitempty"`
}

func locateFile(name string) string {
	return "plantests/" + name
}

func readJSONTests(filename string) []PlanTest {
	var output []PlanTest
	file, err := os.Open(locateFile(filename))
	if err != nil {
		panic(err)
	}
	dec := json.NewDecoder(file)
	dec.DisallowUnknownFields()
	err = dec.Decode(&output)
	if err != nil {
		panic(err)
	}
	return output
}
