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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// planLock pushes "FOR UPDATE", "LOCK IN SHARE MODE" down to all routes
func planLock(pb *primitiveBuilder, in logicalPlan, lock sqlparser.Lock) (logicalPlan, error) {
	output, err := visit(in, func(bldr logicalPlan) (bool, logicalPlan, error) {
		switch node := in.(type) {
		case *route:
			node.Select.SetLock(lock)
			return false, node, nil
		case *sqlCalcFoundRows, *vindexFunc:
			return false, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%T.locking: unreachable", in)
		}
		return true, bldr, nil
	})
	if err != nil {
		return nil, err
	}
	return output, nil
}
