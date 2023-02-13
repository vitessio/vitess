package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type ReferenceRouting struct {
	keyspace *vindexes.Keyspace
}

var _ Routing = (*ReferenceRouting)(nil)

func (rr *ReferenceRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	rp.Keyspace = rr.keyspace
}

func (rr *ReferenceRouting) Clone() Routing {
	return &ReferenceRouting{keyspace: rr.keyspace}
}

func (rr *ReferenceRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	return rr, nil
}

func (rr *ReferenceRouting) Cost() int {
	return 0
}

func (rr *ReferenceRouting) OpCode() engine.Opcode {
	return engine.Reference
}

func (rr *ReferenceRouting) Keyspace() *vindexes.Keyspace {
	return rr.keyspace
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
