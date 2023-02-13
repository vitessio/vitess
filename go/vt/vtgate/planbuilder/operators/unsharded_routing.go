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
		keyspace *vindexes.Keyspace

		//targetDestination specifies an explicit target destination tablet type
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

func (tr *TargetedRouting) ResetRoutingLogic(ctx *plancontext.PlanningContext) (Routing, error) {
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

func (n *NoneRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	rp.Keyspace = n.keyspace
}

func (n *NoneRouting) Clone() Routing {
	return n
}

func (n *NoneRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	// TODO implement me
	panic("implement me")
}

func (n *NoneRouting) ResetRoutingLogic(ctx *plancontext.PlanningContext) (Routing, error) {
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

func (ur *UnshardedRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	rp.Keyspace = ur.keyspace
	rp.Opcode = ur.OpCode()
}

func (ur *UnshardedRouting) Clone() Routing {
	return &UnshardedRouting{keyspace: ur.keyspace}
}

func (ur *UnshardedRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	return ur, nil
}

func (ur *UnshardedRouting) ResetRoutingLogic(ctx *plancontext.PlanningContext) (Routing, error) {
	return ur, nil
}

func (ur *UnshardedRouting) Cost() int {
	return 0
}

func (ur *UnshardedRouting) OpCode() engine.Opcode {
	return engine.Unsharded
}

func (ur *UnshardedRouting) Keyspace() *vindexes.Keyspace {
	return ur.keyspace
}
