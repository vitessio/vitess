/*
Copyright 2020 The Vitess Authors.

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
	"errors"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// planDistinct makes the output distinct
func planDistinct(pb *primitiveBuilder, input builder) (builder, error) {
	switch node := input.(type) {
	case *mergeSort, *pulloutSubquery:
		newInput, err := planDistinct(pb, node.Inputs()[0])
		if err != nil {
			return nil, err
		}
		err = node.Rewrite(newInput)
		if err != nil {
			return nil, err
		}
		return node, nil
	case *route:
		node.Select.(*sqlparser.Select).Distinct = true
		return node, nil
	case *orderedAggregate:
		for i, rc := range node.resultColumns {
			// If the column origin is oa (and not the underlying route),
			// it means that it's an aggregate function supplied by oa.
			// So, the distinct 'operator' cannot be pushed down into the
			// route.
			if rc.column.Origin() == node {
				return nil, errors.New("unsupported: distinct cannot be combined with aggregate functions")
			}
			node.eaggr.Keys = append(node.eaggr.Keys, i)
		}
		newInput, err := planDistinct(pb, node.input)
		if err != nil {
			return nil, err
		}
		node.input = newInput
		return node, nil

	case *subquery:
		return nil, vterrors.New(vtrpc.Code_UNIMPLEMENTED, "unsupported: distinct on cross-shard subquery")
	case *concatenate:
		return nil, vterrors.New(vtrpc.Code_UNIMPLEMENTED, "only union-all is supported for this operator")
	case *join:
		return nil, vterrors.New(vtrpc.Code_UNIMPLEMENTED, "unsupported: distinct on cross-shard join")
	}

	return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "%T.distinct: unreachable", input)
}
