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

package robustness

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// TestRegisterModelLinearizable checks that a history where a read observes the
// value of a write that fully precedes it is accepted as linearizable.
func TestRegisterModelLinearizable(t *testing.T) {
	ops := []porcupine.Operation{
		Put(0, 1, 0, 10),  // client 0: Put(1) during [0,10]
		Get(1, 1, 20, 30), // client 1: Get()->1 during [20,30], after the write returned
	}
	if !porcupine.CheckOperations(RegisterModel, ops) {
		t.Fatal("expected the history to be linearizable")
	}
}

// TestRegisterModelStaleReadNonLinearizable checks that a read of the old value
// after a committed write that fully precedes it (the class of bug a failover
// can introduce) is correctly rejected as non-linearizable.
func TestRegisterModelStaleReadNonLinearizable(t *testing.T) {
	ops := []porcupine.Operation{
		Put(0, 1, 0, 10),  // client 0: Put(1) during [0,10]
		Get(1, 0, 20, 30), // client 1: Get()->0 during [20,30]: stale read of a lost write
	}
	if porcupine.CheckOperations(RegisterModel, ops) {
		t.Fatal("expected the stale-read history to be non-linearizable")
	}
}

// TestRegisterModelConcurrentReadEitherValue checks that when a read overlaps a
// write, observing either the old or the new value is linearizable.
func TestRegisterModelConcurrentReadEitherValue(t *testing.T) {
	oldValue := []porcupine.Operation{
		Put(0, 1, 0, 30),  // Put(1) during [0,30]
		Get(1, 0, 10, 20), // Get()->0 overlaps the write: valid (ordered before it)
	}
	newValue := []porcupine.Operation{
		Put(0, 1, 0, 30),  // Put(1) during [0,30]
		Get(1, 1, 10, 20), // Get()->1 overlaps the write: valid (ordered after it)
	}
	if !porcupine.CheckOperations(RegisterModel, oldValue) {
		t.Fatal("expected observing the old value during a concurrent write to be linearizable")
	}
	if !porcupine.CheckOperations(RegisterModel, newValue) {
		t.Fatal("expected observing the new value during a concurrent write to be linearizable")
	}
}
