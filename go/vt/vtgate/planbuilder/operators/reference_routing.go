/*
Copyright 2023 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// AnyShardRouting is used for routing logic where any shard in the keyspace can be used.
// Shared by unsharded and reference routing
type AnyShardRouting struct {
	keyspace   *vindexes.Keyspace
	Alternates map[*vindexes.Keyspace]*Route
}

var _ Routing = (*AnyShardRouting)(nil)

func (rr *AnyShardRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	rp.Keyspace = rr.keyspace
}

func (rr *AnyShardRouting) Clone() Routing {
	return &AnyShardRouting{keyspace: rr.keyspace}
}

func (rr *AnyShardRouting) updateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	return rr, nil
}

func (rr *AnyShardRouting) Cost() int {
	return 0
}

func (rr *AnyShardRouting) OpCode() engine.Opcode {
	if rr.keyspace.Sharded {
		return engine.Reference
	}
	return engine.Unsharded
}

func (rr *AnyShardRouting) Keyspace() *vindexes.Keyspace {
	return rr.keyspace
}

func (rr *AnyShardRouting) AlternateInKeyspace(keyspace *vindexes.Keyspace) *Route {
	if keyspace.Name == rr.keyspace.Name {
		return nil
	}

	if route, ok := rr.Alternates[keyspace]; ok {
		return route
	}

	return nil
}

// DualRouting represents the dual-table.
// It is special compared to all other tables because it can be merged with tables in any keyspace
type DualRouting struct{}

var _ Routing = (*DualRouting)(nil)

func (dr *DualRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	// intentionally empty
}

func (dr *DualRouting) Clone() Routing {
	return &DualRouting{}
}

func (dr *DualRouting) updateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	return dr, nil
}

func (dr *DualRouting) Cost() int {
	return 0
}

func (dr *DualRouting) OpCode() engine.Opcode {
	return engine.Reference
}

func (dr *DualRouting) Keyspace() *vindexes.Keyspace {
	return nil
}

type SequenceRouting struct{}

var _ Routing = (*SequenceRouting)(nil)

func (sr *SequenceRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	rp.Opcode = engine.Next
}

func (sr *SequenceRouting) Clone() Routing {
	return &SequenceRouting{}
}

func (sr *SequenceRouting) updateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	return sr, nil
}

func (sr *SequenceRouting) Cost() int {
	return 0
}

func (sr *SequenceRouting) OpCode() engine.Opcode {
	return engine.Next
}

func (sr *SequenceRouting) Keyspace() *vindexes.Keyspace {
	return nil
}
