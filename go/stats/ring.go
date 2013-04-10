// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

// Ring of int64 values
// Not thread safe
type RingInt64 struct {
	position int
	values   []int64
}

func NewRingInt64(capacity int) *RingInt64 {
	return &RingInt64{values: make([]int64, 0, capacity)}
}

func (ri *RingInt64) Add(val int64) {
	if len(ri.values) == cap(ri.values) {
		ri.values[ri.position] = val
		ri.position = (ri.position + 1) % cap(ri.values)
	} else {
		ri.values = append(ri.values, val)
	}
}

func (ri *RingInt64) Values() (values []int64) {
	values = make([]int64, len(ri.values))
	for i := 0; i < len(ri.values); i++ {
		values[i] = ri.values[(ri.position+i)%cap(ri.values)]
	}
	return values
}
