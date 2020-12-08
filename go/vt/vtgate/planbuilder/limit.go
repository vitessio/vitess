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

package planbuilder

import (
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ logicalPlan = (*limit)(nil)

// limit is the logicalPlan for engine.Limit.
// This gets built if a limit needs to be applied
// after rows are returned from an underlying
// operation. Since a limit is the final operation
// of a SELECT, most pushes are not applicable.
type limit struct {
	logicalPlanCommon
	elimit *engine.Limit
}

// newLimit builds a new limit.
func newLimit(plan logicalPlan) *limit {
	return &limit{
		logicalPlanCommon: newBuilderCommon(plan),
		elimit:            &engine.Limit{},
	}
}

// Primitive implements the logicalPlan interface
func (l *limit) Primitive() engine.Primitive {
	l.elimit.Input = l.input.Primitive()
	return l.elimit
}
