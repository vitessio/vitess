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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
)

func (hp *horizonPlanning) planHorizonOp(ctx *planningContext, plan abstract.PhysicalOperator) (abstract.PhysicalOperator, error) {
	rb, isRoute := plan.(*routeOp)
	if !isRoute && ctx.semTable.ShardedError != nil {
		return nil, ctx.semTable.ShardedError
	}

	if isRoute && rb.isSingleShard() {
		// err := planSingleShardRoutePlan(hp.sel, rb)
		// if err != nil {
		// 	return nil, err
		// }
		return rb, nil
	}

	return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "wut?")
}
