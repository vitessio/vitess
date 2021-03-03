/*
Copyright 2020 The Vitess Authors.

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
	testcases := [engine.NumRouteOpcodes][engine.NumRouteOpcodes]bool{
		{true, false, false, false, false, false, false, false, true, false},
		{false, true, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, true, true, false},
		{true, true, true, true, true, true, true, true, true, true},
		{false, false, false, false, false, false, false, false, true, false},
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
				lRoute.eroute = engine.NewSimpleRoute(engine.RouteOpcode(left), ks)
				rRoute.eroute = engine.NewSimpleRoute(engine.RouteOpcode(right), ks)
				assert.Equal(t, val, lRoute.JoinCanMerge(pb, rRoute, nil, nil), fmt.Sprintf("%v:%v", lRoute.eroute.RouteType(), rRoute.eroute.RouteType()))
			})
		}
	}
}

func TestSubqueryCanMerge(t *testing.T) {
	testcases := [engine.NumRouteOpcodes][engine.NumRouteOpcodes]bool{
		{true, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, true, true, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, true, false},
	}

	ks := &vindexes.Keyspace{}
	lRoute := &route{}
	pb := &primitiveBuilder{
		plan: lRoute,
	}
	rRoute := &route{}
	for left, vals := range testcases {
		lRoute.eroute = engine.NewSimpleRoute(engine.RouteOpcode(left), ks)
		for right, val := range vals {
			rRoute.eroute = engine.NewSimpleRoute(engine.RouteOpcode(right), ks)
			assert.Equal(t, val, lRoute.SubqueryCanMerge(pb, rRoute), fmt.Sprintf("%v:%v", lRoute.eroute.RouteType(), rRoute.eroute.RouteType()))
		}
	}
}

func TestUnionCanMerge(t *testing.T) {
	testcases := [engine.NumRouteOpcodes][engine.NumRouteOpcodes]bool{
		{true, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, true, false, false, false, false},
		{false, false, false, false, false, false, false, false, false, false},
		{false, false, false, false, false, false, false, true, false, false},
		{false, false, false, false, false, false, false, false, true, false},
		{false, false, false, false, false, false, false, false, false, false},
	}
	ks := &vindexes.Keyspace{}
	lRoute := &route{}
	rRoute := &route{}
	for left, vals := range testcases {
		lRoute.eroute = engine.NewSimpleRoute(engine.RouteOpcode(left), ks)
		for right, val := range vals {
			rRoute.eroute = engine.NewSimpleRoute(engine.RouteOpcode(right), ks)
			assert.Equal(t, val, lRoute.unionCanMerge(rRoute, false), fmt.Sprintf("can't create a single route from these two inputs %v:%v", lRoute.eroute.RouteType(), rRoute.eroute.RouteType()))
		}
	}
}
