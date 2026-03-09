/*
Copyright 2026 The Vitess Authors.

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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

// TestMixedPulloutContextSameSubquery verifies that when the same subquery AST
// appears in two different pullout contexts (scalar value vs IN), the planner
// produces a correct plan with appropriate FilterTypes for each context.
//
// GetReservedArgumentFor returns the same bind variable name for structurally
// identical subqueries. When pullOutValueSubqueries deduplicates by ArgName
// alone, it reuses the first SubQuery operator (and its FilterType) for all
// subsequent occurrences — even when a different FilterType is needed.
func TestMixedPulloutContextSameSubquery(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Version = Gen4

	// Same subquery used as a scalar value AND inside an IN expression.
	query := "SELECT (SELECT count(*) FROM user_extra), 1 IN (SELECT count(*) FROM user_extra) FROM user WHERE id = 1"

	plan, err := TestBuilder(query, vw, "user")
	require.NoError(t, err)
	require.NotNil(t, plan)

	// Collect all UncorrelatedSubquery primitives from the plan tree.
	var subqueries []*engine.UncorrelatedSubquery
	var walk func(p engine.Primitive)
	walk = func(p engine.Primitive) {
		if usq, ok := p.(*engine.UncorrelatedSubquery); ok {
			subqueries = append(subqueries, usq)
		}
		inputs, _ := p.Inputs()
		for _, input := range inputs {
			walk(input)
		}
	}
	walk(plan.Instructions)

	// We need two subquery operators: one PulloutValue for the scalar context,
	// one PulloutIn for the IN context. They must use distinct bind variable
	// names to avoid overwriting each other.
	require.Len(t, subqueries, 2,
		"expected two UncorrelatedSubquery operators for different pullout contexts")

	opcodes := map[opcode.PulloutOpcode]bool{}
	for _, usq := range subqueries {
		opcodes[usq.Opcode] = true
	}
	assert.True(t, opcodes[opcode.PulloutValue],
		"expected a PulloutValue subquery for the scalar context")
	assert.True(t, opcodes[opcode.PulloutIn],
		"expected a PulloutIn subquery for the IN context")

	assert.NotEqual(t, subqueries[0].SubqueryResult, subqueries[1].SubqueryResult,
		"subquery operators sharing the same SubqueryResult name causes bind variable conflicts")
}
