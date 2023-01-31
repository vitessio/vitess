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
	// intentionally empty
}

func (rr *ReferenceRouting) Clone() Routing {
	return &ReferenceRouting{keyspace: rr.keyspace}
}

func (rr *ReferenceRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	// TODO implement me
	panic("implement me")
}

func (rr *ReferenceRouting) Cost() int {
	return 0
}

func (rr *ReferenceRouting) OpCode() engine.Opcode {
	return engine.Reference
}

type DualRouting struct{}

var _ Routing = (*DualRouting)(nil)

func (rr *DualRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	// intentionally empty
}

func (rr *DualRouting) Clone() Routing {
	return &DualRouting{}
}

func (rr *DualRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	// TODO implement me
	panic("implement me")
}

func (rr *DualRouting) Cost() int {
	return 0
}

func (rr *DualRouting) OpCode() engine.Opcode {
	return engine.Reference
}

type SequenceRouting struct{}

var _ Routing = (*SequenceRouting)(nil)

func (rr *SequenceRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	rp.Opcode = engine.Next
}

func (rr *SequenceRouting) Clone() Routing {
	return &SequenceRouting{}
}

func (rr *SequenceRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	return rr, nil
}

func (rr *SequenceRouting) Cost() int {
	return 0
}

func (rr *SequenceRouting) OpCode() engine.Opcode {
	return engine.Next
}
