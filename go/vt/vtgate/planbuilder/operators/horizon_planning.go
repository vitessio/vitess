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

package operators

import (
	"errors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

var errNotHorizonPlanned = errors.New("query can't be fully operator planned")

func planHorizons(ctx *plancontext.PlanningContext, in Operator) (Operator, error) {
	return rewriteBreakableTopDown(ctx, in, func(ctx *plancontext.PlanningContext, in Operator) (Operator, bool, error) {
		switch in := in.(type) {
		case *Horizon:
			op, err := planHorizon(ctx, in)
			if err != nil {
				return nil, false, err
			}
			return op, true, nil
		case *Route:
			return in, false, nil
		default:
			return in, true, nil
		}
	})
}

func planHorizon(ctx *plancontext.PlanningContext, in *Horizon) (Operator, error) {
	rb, isRoute := in.Source.(*Route)
	if isRoute && rb.IsSingleShard() {
		return planSingleShardRoute(in.Select, rb, in)
	}

	return nil, errNotHorizonPlanned
}
func planSingleShardRoute(statement sqlparser.SelectStatement, rb *Route, horizon *Horizon) (Operator, error) {
	return nil, errNotHorizonPlanned
}
