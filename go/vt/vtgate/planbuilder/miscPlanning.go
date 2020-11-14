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
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// planMisc visits all children and sets a few
func planMisc(pb *primitiveBuilder, in builder, sel *sqlparser.Select) (builder, error) {
	output, err := visit(in, func(bldr builder) (bool, builder, error) {
		switch node := bldr.(type) {
		case *route:
			// TODO: this is not cool
			node.Select.(*sqlparser.Select).Comments = sel.Comments
			node.Select.(*sqlparser.Select).Lock = sel.Lock
			if sel.Into != nil {
				if node.eroute.Opcode != engine.SelectUnsharded {
					return false, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: this construct is not supported on sharded keyspace")
				}
				node.Select.(*sqlparser.Select).Into = sel.Into
			}
			return true, node, nil
		}
		return true, bldr, nil
	})

	if err != nil {
		return nil, err
	}
	return output, nil
}
