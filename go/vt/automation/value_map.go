// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"sync"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// ValueMap is a global map where pb.Value entries are
// a) recorded (e.g. as user input or as resulting output) and
// b) they can be retrieved (e.g. to expand task parameters).
// To simplify the design, we assume that entries cannot be removed nor mutated.
type ValueMap struct {
	values  map[string]pb.Value
	rwMutex sync.RWMutex
}

// NewValueMap creates a new instance.
func NewValueMap() *ValueMap {
	return &ValueMap{
		values: make(map[string]pb.Value),
	}
}

// RecordValue stores a key/value pair. An existing key cannot be updated.
func (vm *ValueMap) RecordValue(key string, value pb.Value) {
	vm.rwMutex.Lock()
	defer vm.rwMutex.Unlock()

	// TODO(mberlin): Assert that key doesn't exist yet.
	vm.values[key] = value
}

// GetValue retrives the value for a given key.
func (vm *ValueMap) GetValue(key string) (pb.Value, bool) {
	vm.rwMutex.RLock()
	defer vm.rwMutex.RUnlock()

	value, ok := vm.values[key]
	return value, ok
}
