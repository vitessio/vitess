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

func (self *RingInt64) Add(val int64) {
	if len(self.values) == cap(self.values) {
		self.values[self.position] = val
		self.position = (self.position + 1) % cap(self.values)
	} else {
		self.values = append(self.values, val)
	}
}

func (self *RingInt64) Values() (values []int64) {
	values = make([]int64, len(self.values))
	for i := 0; i < len(self.values); i++ {
		values[i] = self.values[(self.position+i)%cap(self.values)]
	}
	return values
}
