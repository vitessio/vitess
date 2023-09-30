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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// This file has functions to analyze postprocessing
// clauses like ORDER BY, etc.

func createLimit(input logicalPlan, limit *sqlparser.Limit) (logicalPlan, error) {
	plan := newLimit(input)
	pv, err := evalengine.Translate(limit.Rowcount, nil)
	if err != nil {
		return nil, vterrors.Wrap(err, "unexpected expression in LIMIT")
	}
	plan.elimit.Count = pv

	if limit.Offset != nil {
		pv, err = evalengine.Translate(limit.Offset, nil)
		if err != nil {
			return nil, vterrors.Wrap(err, "unexpected expression in OFFSET")
		}
		plan.elimit.Offset = pv
	}

	return plan, nil
}
