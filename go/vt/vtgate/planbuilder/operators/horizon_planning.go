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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
)

func planHorizons(in ops.Operator) (ops.Operator, error) {
	return rewrite.TopDown(in, func(in ops.Operator) (ops.Operator, rewrite.TreeIdentity, rewrite.VisitRule, error) {
		switch in := in.(type) {
		case *Horizon:
			op, err := planHorizon(in)
			if err != nil {
				return nil, rewrite.SameTree, rewrite.SkipChildren, err
			}
			return op, rewrite.NewTree, rewrite.VisitChildren, nil
		case *Route:
			return in, rewrite.SameTree, rewrite.SkipChildren, nil
		default:
			return in, rewrite.SameTree, rewrite.VisitChildren, nil
		}
	})
}

func planHorizon(in *Horizon) (ops.Operator, error) {
	rb, isRoute := in.Source.(*Route)
	if isRoute && rb.IsSingleShard() && in.Select.GetLimit() == nil {
		return planSingleShardRoute(rb, in)
	}

	return in, nil
}
func planSingleShardRoute(rb *Route, horizon *Horizon) (ops.Operator, error) {
	rb.Source, horizon.Source = horizon, rb.Source
	return rb, nil
}
