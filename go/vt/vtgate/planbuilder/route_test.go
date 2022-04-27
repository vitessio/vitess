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

This test file only tests the V3 planner. It does not test the Subshard opcode

For easy reference, opcodes are:
	Unsharded   	 0
	EqualUnique 	 1
	Equal       	 2
	IN          	 3
	MultiEqual  	 4
	Scatter     	 5
	Next        	 6
	DBA         	 7
	Reference   	 8
	None        	 9
	Subshard         10 <- not covered
	NumRouteOpcodes  11
*/

func TestJoinCanMerge(t *testing.T) {
	testcases := [][]bool{
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
			name := fmt.Sprintf("%s:%s", engine.Opcode(left).String(), engine.Opcode(right).String())
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
	testcases := [][]bool{
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
	testcases := [][]bool{
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
