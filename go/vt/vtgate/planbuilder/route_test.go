package planbuilder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

/*
For easy reference, opcodes are:
	SelectUnsharded   0
	SelectEqualUnique 1
	SelectEqual       2
	SelectIN          3
	SelectMultiEqual  4
	SelectScatter     5
	SelectNext        6
	SelectDBA         7
	SelectReference   8
	SelectNone        9
	NumRouteOpcodes   10
*/

func TestJoinCanMerge(t *testing.T) {
	testcases := [engine.NumOpcodes][engine.NumOpcodes]bool{
		{true, false, false, false, false, false, false, false, true, false, false},
		{false, true, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, true, true, false, false},
		{true, true, true, true, true, true, true, true, true, true, true},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
	}

	ks := &vindexes.Keyspace{}
	for left, vals := range testcases {
		for right, val := range vals {
			name := fmt.Sprintf("%d:%d", left, right)
			t.Run(name, func(t *testing.T) {
				lRoute := &route{
					// Setting condition will make SelectEqualUnique match itself.
					condition: &sqlparser.ColName{},
				}
				pb := &primitiveBuilder{
					plan: lRoute,
				}
				rRoute := &route{
					condition: &sqlparser.ColName{},
				}
				lRoute.eroute = engine.NewSimpleRoute(engine.Opcode(left), ks)
				rRoute.eroute = engine.NewSimpleRoute(engine.Opcode(right), ks)
				assert.Equal(t, val, lRoute.JoinCanMerge(pb, rRoute, nil, nil), fmt.Sprintf("%v:%v", lRoute.eroute.RouteType(), rRoute.eroute.RouteType()))
			})
		}
	}
}

func TestSubqueryCanMerge(t *testing.T) {
	testcases := [engine.NumOpcodes][engine.NumOpcodes]bool{
		{true, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, true, true, false, false},
		{true, true, true, true, true, true, true, true, true, true, true},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
	}

	ks := &vindexes.Keyspace{}
	lRoute := &route{}
	pb := &primitiveBuilder{
		plan: lRoute,
	}
	rRoute := &route{}
	for left, vals := range testcases {
		lRoute.eroute = engine.NewSimpleRoute(engine.Opcode(left), ks)
		for right, val := range vals {
			rRoute.eroute = engine.NewSimpleRoute(engine.Opcode(right), ks)
			assert.Equal(t, val, lRoute.SubqueryCanMerge(pb, rRoute), fmt.Sprintf("%v:%v", lRoute.eroute.RouteType(), rRoute.eroute.RouteType()))
		}
	}
}

func TestUnionCanMerge(t *testing.T) {
	testcases := [engine.NumOpcodes][engine.NumOpcodes]bool{
		{true, false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, true, false, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, true, false, false, false},
		{false, false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false, false},
	}
	ks := &vindexes.Keyspace{}
	lRoute := &route{}
	rRoute := &route{}
	for left, vals := range testcases {
		lRoute.eroute = engine.NewSimpleRoute(engine.Opcode(left), ks)
		for right, val := range vals {
			rRoute.eroute = engine.NewSimpleRoute(engine.Opcode(right), ks)
			assert.Equal(t, val, lRoute.unionCanMerge(rRoute, false), fmt.Sprintf("can't create a single route from these two inputs %v:%v", lRoute.eroute.RouteType(), rRoute.eroute.RouteType()))
		}
	}
}
