/*
Copyright 2026 The Vitess Authors.

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

// Package robustness holds the foundation for Vitess robustness testing:
// deterministic fault injection paired with linearizability checking of the
// distributed layer, as proposed in
// https://github.com/vitessio/vitess/issues/20470.
//
// This first commit lands only the reusable pieces that need no live cluster:
// a linearizability model of a single versioned register plus history helpers,
// checked with anishathalye/porcupine (the same checker etcd uses in
// tests/robustness), and self-tests that prove the checker distinguishes a
// linearizable history from a stale-read one. The healthy-cluster baseline and
// the fault drivers (tablet kill, PlannedReparentShard/EmergencyReparentShard,
// network partition) are follow-ups tracked in #20470.
package robustness

import "github.com/anishathalye/porcupine"

type opKind int

const (
	opPut opKind = iota
	opGet
)

type registerInput struct {
	kind  opKind
	value int
}

// RegisterModel is a linearizability model of a single versioned register that
// supports Put(value) and Get(). It is intentionally minimal: it captures the
// externally observable consistency a client expects from a single row/key, so
// a history recorded from a real cluster (a workload over VTGate) can be
// checked for stale or torn reads after failovers.
var RegisterModel = porcupine.Model{
	Init: func() any { return 0 },
	Step: func(state, input, output any) (bool, any) {
		in := input.(registerInput)
		st := state.(int)
		if in.kind == opPut {
			// A write always succeeds and advances the register to its value.
			return true, in.value
		}
		// A read is valid only if it observed the current register value.
		return output.(int) == st, st
	},
	Equal: func(a, b any) bool { return a.(int) == b.(int) },
}

// Put returns a porcupine operation for a write of value by client, with the
// given logical call and return timestamps.
func Put(client, value int, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		ClientId: client,
		Input:    registerInput{kind: opPut, value: value},
		Call:     call,
		Output:   value,
		Return:   ret,
	}
}

// Get returns a porcupine operation for a read that observed value by client,
// with the given logical call and return timestamps.
func Get(client, observed int, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		ClientId: client,
		Input:    registerInput{kind: opGet},
		Call:     call,
		Output:   observed,
		Return:   ret,
	}
}
