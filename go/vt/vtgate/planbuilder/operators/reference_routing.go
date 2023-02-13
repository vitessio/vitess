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

func (rr *AnyShardRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
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

type DualRouting struct{}

var _ Routing = (*DualRouting)(nil)

func (dr *DualRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	// intentionally empty
}

func (dr *DualRouting) Clone() Routing {
	return &DualRouting{}
}

func (dr *DualRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
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

func (sr *SequenceRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
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
