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

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
)

func TestCombineRisk(t *testing.T) {
	tests := []struct {
		a, b, want outputRisk
	}{
		{riskIndependent, riskIndependent, riskIndependent},
		{riskIndependent, riskMergedJSON, riskMergedJSON},
		{riskMergedJSON, riskSubtypeLossyMergedJSON, riskSubtypeLossyMergedJSON},
		{riskSubtypeLossyMergedJSON, riskUnknown, riskUnknown},
		{riskUnknown, riskIndependent, riskUnknown},
		{riskMergedJSON, riskMergedJSON, riskMergedJSON},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, combineRisk(tc.a, tc.b))
		assert.Equal(t, tc.want, combineRisk(tc.b, tc.a))
	}
}

// TestRiskForOutputUnion pins the Union rule: union output columns are
// positionally aligned across every source, so an output offset combines the
// risk of that offset in each source. The source risks are seeded through the
// resolver memo, keeping the test a pure function of the combine rule.
func TestRiskForOutputUnion(t *testing.T) {
	tests := []struct {
		name    string
		sources []outputRisk
		want    outputRisk
	}{
		{"all independent", []outputRisk{riskIndependent, riskIndependent}, riskIndependent},
		{"one merged", []outputRisk{riskIndependent, riskMergedJSON}, riskMergedJSON},
		{"one lossy", []outputRisk{riskMergedJSON, riskSubtypeLossyMergedJSON}, riskSubtypeLossyMergedJSON},
		{"one unknown", []outputRisk{riskIndependent, riskUnknown}, riskUnknown},
		{"lossy and unknown", []outputRisk{riskSubtypeLossyMergedJSON, riskUnknown}, riskUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolver := newOutputRiskResolver(nil)
			union := &operators.Union{}
			for _, risk := range tc.sources {
				src := &operators.Route{}
				union.Sources = append(union.Sources, src)
				resolver.memo[outputRiskKey{op: src, offset: 0}] = risk
			}
			assert.Equal(t, tc.want, resolver.riskForOutput(union, 0))
		})
	}
}

// TestCandidatePredicates pins the risk/type decision tables of the two
// operand policies: the comparison policy rejects subtype-lossy merged JSON
// and unproved provenance, while the GREATEST/LEAST policy also rejects
// every proved merge-derived JSON operand.
func TestCandidatePredicates(t *testing.T) {
	tests := []struct {
		name               string
		c                  candidateAnalysis
		rejectedComparison bool
		rejectedPolicy     bool
	}{
		{"merged JSON", candidateAnalysis{risk: riskMergedJSON, sqlType: querypb.Type_JSON, typeKnown: true}, false, true},
		{"merged unknown type", candidateAnalysis{risk: riskMergedJSON}, false, true},
		{"merged non-JSON", candidateAnalysis{risk: riskMergedJSON, sqlType: querypb.Type_INT64, typeKnown: true}, false, false},
		{"lossy JSON", candidateAnalysis{risk: riskSubtypeLossyMergedJSON, sqlType: querypb.Type_JSON, typeKnown: true}, true, true},
		{"lossy unknown type", candidateAnalysis{risk: riskSubtypeLossyMergedJSON}, true, true},
		{"lossy non-JSON", candidateAnalysis{risk: riskSubtypeLossyMergedJSON, sqlType: querypb.Type_VARCHAR, typeKnown: true}, false, false},
		{"unknown risk JSON", candidateAnalysis{risk: riskUnknown, sqlType: querypb.Type_JSON, typeKnown: true}, true, true},
		{"unknown risk unknown type", candidateAnalysis{risk: riskUnknown}, true, true},
		{"unknown risk non-JSON", candidateAnalysis{risk: riskUnknown, sqlType: querypb.Type_INT64, typeKnown: true}, false, false},
		{"independent JSON", candidateAnalysis{risk: riskIndependent, sqlType: querypb.Type_JSON, typeKnown: true}, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.rejectedComparison, tc.c.lossyJSONOperand() || tc.c.unknownMergeProvenance())
			assert.Equal(t, tc.rejectedPolicy, tc.c.mergedJSONOperand() || tc.c.unknownMergeProvenance())
		})
	}
}

func TestLossyJSONOperand(t *testing.T) {
	assert.True(t, candidateAnalysis{risk: riskSubtypeLossyMergedJSON, sqlType: querypb.Type_JSON, typeKnown: true}.lossyJSONOperand())
	assert.True(t, candidateAnalysis{risk: riskSubtypeLossyMergedJSON}.lossyJSONOperand())
	assert.False(t, candidateAnalysis{risk: riskSubtypeLossyMergedJSON, sqlType: querypb.Type_VARCHAR, typeKnown: true}.lossyJSONOperand())
	assert.False(t, candidateAnalysis{risk: riskMergedJSON, sqlType: querypb.Type_JSON, typeKnown: true}.lossyJSONOperand())
	assert.False(t, candidateAnalysis{risk: riskUnknown}.lossyJSONOperand())
}

func TestClassifyKnownJSONWrapper(t *testing.T) {
	parse := func(sql string) sqlparser.Expr {
		expr, err := sqlparser.NewTestParser().ParseExpr(sql)
		require.NoError(t, err)
		return expr
	}
	tests := []struct {
		sql  string
		want jsonWrapperKind
	}{
		{"json_extract(x, '$')", wrapperReturnsJSON},
		{"json_object('k', x)", wrapperReturnsJSON},
		{"json_array(x)", wrapperReturnsJSON},
		{"cast(x as json)", wrapperReturnsJSON},
		{"json_unquote(x)", wrapperReturnsText},
		{"cast(x as char)", wrapperReturnsText},
		{"cast(x as binary)", wrapperUnclassified},
		{"lower(x)", wrapperUnclassified},
		{"x + 1", wrapperUnclassified},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			assert.Equal(t, tc.want, classifyKnownJSONWrapper(parse(tc.sql)))
		})
	}
}
