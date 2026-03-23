/*
Copyright 2024 The Vitess Authors.

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

package operators

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestMergeOffsetExpressions(t *testing.T) {
	tests := []struct {
		name           string
		offset1        sqlparser.Expr
		offset2        sqlparser.Expr
		expectedExpr   sqlparser.Expr
		expectedFailed bool
	}{
		{
			name:           "both offsets are integers",
			offset1:        sqlparser.NewIntLiteral("5"),
			offset2:        sqlparser.NewIntLiteral("3"),
			expectedExpr:   sqlparser.NewIntLiteral("8"),
			expectedFailed: false,
		},
		{
			name:           "first offset is nil",
			offset1:        nil,
			offset2:        sqlparser.NewIntLiteral("3"),
			expectedExpr:   sqlparser.NewIntLiteral("3"),
			expectedFailed: false,
		},
		{
			name:           "second offset is nil",
			offset1:        sqlparser.NewIntLiteral("5"),
			offset2:        nil,
			expectedExpr:   sqlparser.NewIntLiteral("5"),
			expectedFailed: false,
		},
		{
			name:           "both offsets are nil",
			offset1:        nil,
			offset2:        nil,
			expectedExpr:   nil,
			expectedFailed: false,
		},
		{
			name:           "first offset is argument",
			offset1:        sqlparser.NewArgument("offset1"),
			offset2:        sqlparser.NewIntLiteral("3"),
			expectedExpr:   nil,
			expectedFailed: true,
		},
		{
			name:           "second offset is argument",
			offset1:        sqlparser.NewIntLiteral("5"),
			offset2:        sqlparser.NewArgument("offset2"),
			expectedExpr:   nil,
			expectedFailed: true,
		},
		{
			name:           "both offsets are arguments",
			offset1:        sqlparser.NewArgument("offset1"),
			offset2:        sqlparser.NewArgument("offset2"),
			expectedExpr:   nil,
			expectedFailed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, failed := mergeOffsetExpressions(tt.offset1, tt.offset2)
			assert.Equal(t, tt.expectedExpr, expr)
			assert.Equal(t, tt.expectedFailed, failed, "failed")
		})
	}
}

func TestMergeLimitExpressions(t *testing.T) {
	tests := []struct {
		name           string
		limit1         sqlparser.Expr
		limit2         sqlparser.Expr
		offset2        sqlparser.Expr
		expectedExpr   sqlparser.Expr
		expectedFailed bool
	}{
		{
			name:           "valid limits and offset",
			limit1:         sqlparser.NewIntLiteral("10"),
			limit2:         sqlparser.NewIntLiteral("7"),
			offset2:        sqlparser.NewIntLiteral("3"),
			expectedExpr:   sqlparser.NewIntLiteral("7"),
			expectedFailed: false,
		},
		{
			name:           "remaining rows after offset2 is zero",
			limit1:         sqlparser.NewIntLiteral("3"),
			limit2:         sqlparser.NewIntLiteral("7"),
			offset2:        sqlparser.NewIntLiteral("5"),
			expectedExpr:   sqlparser.NewIntLiteral("0"),
			expectedFailed: false,
		},
		{
			name:           "first limit is nil",
			limit1:         nil,
			limit2:         sqlparser.NewIntLiteral("7"),
			offset2:        sqlparser.NewIntLiteral("3"),
			expectedExpr:   sqlparser.NewIntLiteral("7"),
			expectedFailed: false,
		},
		{
			name:           "second limit is nil",
			limit1:         sqlparser.NewIntLiteral("10"),
			limit2:         nil,
			offset2:        sqlparser.NewIntLiteral("3"),
			expectedExpr:   sqlparser.NewIntLiteral("7"),
			expectedFailed: false,
		},
		{
			name:           "offset2 is nil",
			limit1:         sqlparser.NewIntLiteral("10"),
			limit2:         sqlparser.NewIntLiteral("7"),
			offset2:        nil,
			expectedExpr:   sqlparser.NewIntLiteral("7"),
			expectedFailed: false,
		},
		{
			name:           "first limit is argument",
			limit1:         sqlparser.NewArgument("limit1"),
			limit2:         sqlparser.NewIntLiteral("7"),
			offset2:        sqlparser.NewIntLiteral("3"),
			expectedExpr:   nil,
			expectedFailed: true,
		},
		{
			name:           "second limit is argument",
			limit1:         sqlparser.NewIntLiteral("10"),
			limit2:         sqlparser.NewArgument("limit2"),
			offset2:        sqlparser.NewIntLiteral("3"),
			expectedExpr:   nil,
			expectedFailed: true,
		},
		{
			name:           "offset2 is argument",
			limit1:         sqlparser.NewIntLiteral("10"),
			limit2:         sqlparser.NewIntLiteral("7"),
			offset2:        sqlparser.NewArgument("offset2"),
			expectedExpr:   nil,
			expectedFailed: true,
		},
		{
			name:           "all are arguments",
			limit1:         sqlparser.NewArgument("limit1"),
			limit2:         sqlparser.NewArgument("limit2"),
			offset2:        sqlparser.NewArgument("offset2"),
			expectedExpr:   nil,
			expectedFailed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, failed := mergeLimitExpressions(tt.limit1, tt.limit2, tt.offset2)
			assert.Equal(t, tt.expectedExpr, expr)
			assert.Equal(t, tt.expectedFailed, failed, "failed")
		})
	}
}

func TestTryPushDistinctWithWindow(t *testing.T) {
	tests := []struct {
		name                     string
		distinctRequired         bool
		sourceAlreadyHasDistinct bool
		expectRewrite            bool
	}{
		{
			name:                     "required distinct pushes performance distinct under window and keeps original",
			distinctRequired:         true,
			sourceAlreadyHasDistinct: false,
			expectRewrite:            true,
		},
		{
			name:                     "performance distinct pushes under window and returns window",
			distinctRequired:         false,
			sourceAlreadyHasDistinct: false,
			expectRewrite:            true,
		},
		{
			name:                     "no rewrite when window source already has distinct",
			distinctRequired:         true,
			sourceAlreadyHasDistinct: true,
			expectRewrite:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var windowSource Operator = &fakeOp{}
			if tt.sourceAlreadyHasDistinct {
				windowSource = newDistinct(windowSource, nil, false)
			}
			window := newWindow(windowSource, nil)
			distinct := newDistinct(window, nil, tt.distinctRequired)

			result, rewriteResult := tryPushDistinct(distinct)

			if !tt.expectRewrite {
				assert.Equal(t, NoRewrite, rewriteResult)
				assert.Same(t, distinct, result, "expected the exact same Distinct object to be returned")
				resultDistinct := result.(*Distinct)
				assert.Same(t, window, resultDistinct.Source, "expected Window to be unchanged")
				assert.Same(t, windowSource, window.Source, "expected Window source to be unchanged")
				return
			}

			assert.NotEqual(t, NoRewrite, rewriteResult)

			var resultWindow *Window
			if tt.distinctRequired {
				resultDistinct, ok := result.(*Distinct)
				assert.True(t, ok, "expected Distinct to be returned")
				assert.True(t, resultDistinct.Required)
				assert.True(t, resultDistinct.PushedPerformance)
				resultWindow, ok = resultDistinct.Source.(*Window)
				assert.True(t, ok, "expected Window under Distinct")
			} else {
				var ok bool
				resultWindow, ok = result.(*Window)
				assert.True(t, ok, "expected Window to be returned")
			}

			underWindow, ok := resultWindow.Source.(*Distinct)
			assert.True(t, ok, "expected Distinct under Window")
			assert.False(t, underWindow.Required, "pushed distinct should not be required")
		})
	}
}