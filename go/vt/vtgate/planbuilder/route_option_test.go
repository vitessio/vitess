/*
Copyright 2019 The Vitess Authors.

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
	"testing"

	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestIsBetterThan(t *testing.T) {
	// All combinations aren't worth it.
	testcases := []struct {
		left, right         engine.RouteOpcode
		leftcost, rightcost int
		out                 bool
	}{{
		left:  engine.SelectUnsharded,
		right: engine.SelectUnsharded,
		out:   false,
	}, {
		left:  engine.SelectUnsharded,
		right: engine.SelectNext,
		out:   false,
	}, {
		left:  engine.SelectUnsharded,
		right: engine.SelectDBA,
		out:   false,
	}, {
		left:  engine.SelectUnsharded,
		right: engine.SelectEqualUnique,
		out:   true,
	}, {
		left:  engine.SelectUnsharded,
		right: engine.SelectIN,
		out:   true,
	}, {
		left:  engine.SelectUnsharded,
		right: engine.SelectEqual,
		out:   true,
	}, {
		left:  engine.SelectUnsharded,
		right: engine.SelectScatter,
		out:   true,
	}, {
		left:  engine.SelectNext,
		right: engine.SelectUnsharded,
		out:   false,
	}, {
		left:  engine.SelectDBA,
		right: engine.SelectUnsharded,
		out:   false,
	}, {
		left:  engine.SelectEqualUnique,
		right: engine.SelectUnsharded,
		out:   false,
	}, {
		left:      engine.SelectEqualUnique,
		right:     engine.SelectEqualUnique,
		leftcost:  2,
		rightcost: 1,
		out:       false,
	}, {
		left:      engine.SelectEqualUnique,
		right:     engine.SelectEqualUnique,
		leftcost:  1,
		rightcost: 1,
		out:       false,
	}, {
		left:      engine.SelectEqualUnique,
		right:     engine.SelectEqualUnique,
		leftcost:  1,
		rightcost: 2,
		out:       true,
	}, {
		left:  engine.SelectEqualUnique,
		right: engine.SelectIN,
		out:   true,
	}, {
		left:  engine.SelectEqualUnique,
		right: engine.SelectIN,
		out:   true,
	}, {
		left:      engine.SelectIN,
		right:     engine.SelectIN,
		leftcost:  2,
		rightcost: 1,
		out:       false,
	}, {
		left:      engine.SelectIN,
		right:     engine.SelectIN,
		leftcost:  1,
		rightcost: 1,
		out:       false,
	}, {
		left:      engine.SelectIN,
		right:     engine.SelectIN,
		leftcost:  1,
		rightcost: 2,
		out:       true,
	}, {
		left:  engine.SelectIN,
		right: engine.SelectEqual,
		out:   true,
	}, {
		left:  engine.SelectEqual,
		right: engine.SelectUnsharded,
		out:   false,
	}, {
		left:      engine.SelectEqual,
		right:     engine.SelectEqual,
		leftcost:  2,
		rightcost: 1,
		out:       false,
	}, {
		left:      engine.SelectEqual,
		right:     engine.SelectEqual,
		leftcost:  1,
		rightcost: 1,
		out:       false,
	}, {
		left:      engine.SelectEqual,
		right:     engine.SelectEqual,
		leftcost:  1,
		rightcost: 2,
		out:       true,
	}, {
		left:  engine.SelectEqual,
		right: engine.SelectScatter,
		out:   true,
	}, {
		left:  engine.SelectScatter,
		right: engine.SelectUnsharded,
		out:   false,
	}, {
		left:  engine.SelectScatter,
		right: engine.SelectScatter,
		out:   false,
	}}
	buildOption := func(opt engine.RouteOpcode, cost int) *routeOption {
		var v vindexes.Vindex
		switch cost {
		case 1:
			v, _ = newHashIndex("", nil)
		case 2:
			v, _ = newLookupIndex("", nil)
		}
		return &routeOption{
			eroute: &engine.Route{
				Opcode: opt,
				Vindex: v,
			},
		}
	}
	for _, tcase := range testcases {
		left := buildOption(tcase.left, tcase.leftcost)
		right := buildOption(tcase.right, tcase.rightcost)
		got := left.isBetterThan(right)
		if got != tcase.out {
			t.Errorf("isBetterThan(%v, %v, %v, %v): %v, want %v", tcase.left, tcase.leftcost, tcase.right, tcase.rightcost, got, tcase.out)
		}
	}
}
