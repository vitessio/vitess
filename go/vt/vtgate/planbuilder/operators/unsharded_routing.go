package operators

import (
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	NoneRouting struct {
		keyspace *vindexes.Keyspace
	}

	TargetedRouting struct {
		keyspace *vindexes.Keyspace

		// targetDestination specifies an explicit target destination tablet type
		TargetDestination key.Destination
	}
)

var (
	_ Routing = (*NoneRouting)(nil)
	_ Routing = (*TargetedRouting)(nil)
)

func (tr *TargetedRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	rp.Keyspace = tr.keyspace
	rp.TargetDestination = tr.TargetDestination
}

func (tr *TargetedRouting) Clone() Routing {
	newTr := *tr
	return &newTr
}

func (tr *TargetedRouting) UpdateRoutingLogic(_ *plancontext.PlanningContext, _ sqlparser.Expr) (Routing, error) {
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
