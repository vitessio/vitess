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

package history

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHistory(t *testing.T) {
	q := New(4)

	i := 0
	for ; i < 2; i++ {
		q.Add(i)
	}
	want := []int{1, 0}
	records := q.Records()
	assert.EqualValues(t, len(want), len(records), "len(records): want %v, got %v. records: %+v", len(want), len(records), q)
	for i, record := range records {
		assert.Equal(t, want[i], record)
	}

	for ; i < 6; i++ {
		q.Add(i)
	}

	want = []int{5, 4, 3, 2}
	records = q.Records()
	assert.EqualValues(t, len(want), len(records), "len(records): want %v, got %v. records: %+v", len(want), len(records), q)
	for i, record := range records {
		assert.Equal(t, want[i], record)
	}
}

func TestLatest(t *testing.T) {
	h := New(4)

	// Add first value.
	h.Add(mod10(1))
	assert.EqualValues(t, 1, int(h.Records()[0].(mod10)), "h.Records()[0] = %v, want %v", h.Records()[0], 1)
	assert.EqualValues(t, 1, int(h.Latest().(mod10)), "h.Latest() = %v, want %v", h.Latest(), 1)

	// Add value that isn't a "duplicate".
	h.Add(mod10(2))
	assert.EqualValues(t, 2, int(h.Records()[0].(mod10)), "h.Records()[0] = %v, want %v", h.Records()[0], 2)
	assert.EqualValues(t, 2, int(h.Latest().(mod10)), "h.Latest() = %v, want %v", h.Latest(), 2)

	// Add value that IS a "duplicate".
	h.Add(mod10(12))
	// Records()[0] doesn't change.
	assert.EqualValues(t, 2, int(h.Records()[0].(mod10)), "h.Records()[0] = %v, want %v", h.Records()[0], 2)
	// Latest() does change.
	assert.EqualValues(t, 12, int(h.Latest().(mod10)), "h.Latest() = %v, want %v", h.Latest(), 12)
}

type duplic int

func (d duplic) IsDuplicate(other any) bool {
	return d == other
}

func TestIsEquivalent(t *testing.T) {
	q := New(4)
	q.Add(duplic(0))
	q.Add(duplic(0))
	assert.EqualValues(t, 1, len(q.Records()), "len(q.Records())=%v, want %v", len(q.Records()), 1)
}

type mod10 int

func (m mod10) IsDuplicate(other any) bool {
	return m%10 == other.(mod10)%10
}
