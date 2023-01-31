package operators

import (
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	UnshardedRouting struct {
		keyspace *vindexes.Keyspace
	}

	NoneRouting struct {
		keyspace *vindexes.Keyspace
	}

	TargetedRouting struct {
		Keyspace *vindexes.Keyspace
		// TargetDestination specifies an explicit target destination tablet type
		TargetDestination key.Destination
	}
)

var (
	_ Routing = (*UnshardedRouting)(nil)
	_ Routing = (*NoneRouting)(nil)
	_ Routing = (*TargetedRouting)(nil)
)

func (tr *TargetedRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	// TODO implement me
	panic("implement me")
}

func (tr *TargetedRouting) Clone() Routing {
	// TODO implement me
	panic("implement me")
}

func (tr *TargetedRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	// TODO implement me
	panic("implement me")
}

func (tr *TargetedRouting) Cost() int {
	return 1
}

func (tr *TargetedRouting) OpCode() engine.Opcode {
	return engine.ByDestination
}

func (n *NoneRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	// TODO implement me
	panic("implement me")
}

func (n *NoneRouting) Clone() Routing {
	// TODO implement me
	panic("implement me")
}

func (n *NoneRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	// TODO implement me
	panic("implement me")
}

func (isr *NoneRouting) Cost() int {
	return 0
}

func (rr *NoneRouting) OpCode() engine.Opcode {
	return engine.None
}

func (u *UnshardedRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	// TODO implement me
	panic("implement me")
}

func (u *UnshardedRouting) Clone() Routing {
	// TODO implement me
	panic("implement me")
}

func (u *UnshardedRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	return u, nil
}

func (isr *UnshardedRouting) Cost() int {
	return 0
}

func (rr *UnshardedRouting) OpCode() engine.Opcode {
	return engine.Unsharded
}
