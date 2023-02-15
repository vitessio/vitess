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
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	// NoneRouting is used when we know that this Route will return no results.
	// Can be merged with any other route going to the same keyspace
	NoneRouting struct {
		keyspace *vindexes.Keyspace
	}

	// TargetedRouting is used when the user has used syntax to target the
	// Route against a specific set of shards and/or tablet type. Can't be merged with anything else.
	TargetedRouting struct {
		keyspace *vindexes.Keyspace

		// targetDestination specifies an explicit target destination tablet type
		TargetDestination key.Destination
	}

	// AnyShardRouting is used for routing logic where any shard in the keyspace can be used.
	// Shared by unsharded and reference routing
	AnyShardRouting struct {
		keyspace   *vindexes.Keyspace
		Alternates map[*vindexes.Keyspace]*Route
	}

	// DualRouting represents the dual-table.
	// It is special compared to all other tables because it can be merged with tables in any keyspace
	DualRouting struct{}

	SequenceRouting struct {
		keyspace *vindexes.Keyspace
	}
)

var (
	_ Routing = (*NoneRouting)(nil)
	_ Routing = (*TargetedRouting)(nil)
	_ Routing = (*AnyShardRouting)(nil)
	_ Routing = (*DualRouting)(nil)
	_ Routing = (*SequenceRouting)(nil)
)

func (tr *TargetedRouting) UpdateRoutingParams(_ *plancontext.PlanningContext, rp *engine.RoutingParameters) error {
	rp.Keyspace = tr.keyspace
	rp.TargetDestination = tr.TargetDestination
	return nil
}

func (tr *TargetedRouting) Clone() Routing {
	newTr := *tr
	return &newTr
}

func (tr *TargetedRouting) updateRoutingLogic(_ *plancontext.PlanningContext, _ sqlparser.Expr) (Routing, error) {
	return tr, nil
}

func (tr *TargetedRouting) Cost() int {
	return 1
}

func (tr *TargetedRouting) OpCode() engine.Opcode {
	return engine.ByDestination
}

func (tr *TargetedRouting) Keyspace() *vindexes.Keyspace {
	return tr.keyspace
}

func (n *NoneRouting) UpdateRoutingParams(_ *plancontext.PlanningContext, rp *engine.RoutingParameters) error {
	rp.Keyspace = n.keyspace
	return nil
}

func (n *NoneRouting) Clone() Routing {
	return n
}

func (n *NoneRouting) updateRoutingLogic(*plancontext.PlanningContext, sqlparser.Expr) (Routing, error) {
	return n, nil
}

func (n *NoneRouting) Cost() int {
	return 0
}

func (n *NoneRouting) OpCode() engine.Opcode {
	return engine.None
}

func (n *NoneRouting) Keyspace() *vindexes.Keyspace {
	return n.keyspace
}

func (rr *AnyShardRouting) UpdateRoutingParams(_ *plancontext.PlanningContext, rp *engine.RoutingParameters) error {
	rp.Keyspace = rr.keyspace
	return nil
}

func (rr *AnyShardRouting) Clone() Routing {
	return &AnyShardRouting{
		keyspace:   rr.keyspace,
		Alternates: rr.Alternates,
	}
}

func (rr *AnyShardRouting) updateRoutingLogic(*plancontext.PlanningContext, sqlparser.Expr) (Routing, error) {
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

func (dr *DualRouting) UpdateRoutingParams(*plancontext.PlanningContext, *engine.RoutingParameters) error {
	return nil
}

func (dr *DualRouting) Clone() Routing {
	return &DualRouting{}
}

func (dr *DualRouting) updateRoutingLogic(*plancontext.PlanningContext, sqlparser.Expr) (Routing, error) {
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

func (sr *SequenceRouting) UpdateRoutingParams(_ *plancontext.PlanningContext, rp *engine.RoutingParameters) error {
	rp.Opcode = engine.Next
	rp.Keyspace = sr.keyspace
	return nil
}

func (sr *SequenceRouting) Clone() Routing {
	return &SequenceRouting{keyspace: sr.keyspace}
}

func (sr *SequenceRouting) updateRoutingLogic(*plancontext.PlanningContext, sqlparser.Expr) (Routing, error) {
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
