/*
Copyright 2022 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func transformSubQueryFilter(ctx *plancontext.PlanningContext, op *operators.SubQueryFilter, isRoot bool) (logicalPlan, error) {
	outer, err := transformToLogicalPlan(ctx, op.Outer, isRoot)
	if err != nil {
		return nil, err
	}

	inner, err := transformToLogicalPlan(ctx, op.Inner(), false)
	if err != nil {
		return nil, err
	}

	if len(op.JoinVars) == 0 {
		// no correlation, so uncorrelated it is
		return newUncorrelatedSubquery(op.FilterType, op.SubqueryValueName, op.HasValuesName, inner, outer), nil
	}

	return newSemiJoin(outer, inner, op.JoinVarOffsets, op.OuterExpressionsNeeded()), nil
}
