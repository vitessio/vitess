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

package stats

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// Ring of int64 values
// Not thread safe
type RingInt64 struct {
	position int64
	values   []int64
	mu       sync.RWMutex
}

func NewRingInt64(capacity int) *RingInt64 {
	return &RingInt64{values: make([]int64, 0, capacity)}
}

func (ri *RingInt64) Add(val int64) {
	if int(ri.position) == cap(ri.values)-1 {
		ri.mu.Lock()
		ri.values[ri.position] = val
		ri.position = (ri.position + 1) % int64(cap(ri.values))
		ri.mu.Unlock()
	} else {
		// add 1 atomically so that next call will see the most up to update position
		pos := int(atomic.AddInt64(&ri.position, 1))
		ri.values[pos-1] = val
	}
}

func (ri *RingInt64) Values() (values []int64) {
	pos := int(ri.position)
	values = make([]int64, len(ri.values))
	for i := 0; i < len(ri.values); i++ {
		values[i] = ri.values[(pos+i)%cap(ri.values)]
	}
	return values
}

// MarshalJSON returns a JSON representation of the RingInt64.
func (ri *RingInt64) MarshalJSON() ([]byte, error) {
	b := bytes.NewBuffer(make([]byte, 0, 4096))
	s, _ := json.Marshal(ri.values)
	fmt.Fprintf(b, "%v", string(s))
	return b.Bytes(), nil
}
