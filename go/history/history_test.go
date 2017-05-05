/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package history

import (
	"testing"
)

func TestHistory(t *testing.T) {
	q := New(4)

	i := 0
	for ; i < 2; i++ {
		q.Add(i)
	}
	want := []int{1, 0}
	records := q.Records()
	if want, got := len(want), len(records); want != got {
		t.Errorf("len(records): want %v, got %v. records: %+v", want, got, q)
	}
	for i, record := range records {
		if record != want[i] {
			t.Errorf("record doesn't match: want %v, got %v", want[i], record)
		}
	}

	for ; i < 6; i++ {
		q.Add(i)
	}

	want = []int{5, 4, 3, 2}
	records = q.Records()
	if want, got := len(want), len(records); want != got {
		t.Errorf("len(records): want %v, got %v. records: %+v", want, got, q)
	}
	for i, record := range records {
		if record != want[i] {
			t.Errorf("record doesn't match: want %v, got %v", want[i], record)
		}
	}
}

func TestLatest(t *testing.T) {
	h := New(4)

	// Add first value.
	h.Add(mod10(1))
	if got, want := int(h.Records()[0].(mod10)), 1; got != want {
		t.Errorf("h.Records()[0] = %v, want %v", got, want)
	}
	if got, want := int(h.Latest().(mod10)), 1; got != want {
		t.Errorf("h.Latest() = %v, want %v", got, want)
	}

	// Add value that isn't a "duplicate".
	h.Add(mod10(2))
	if got, want := int(h.Records()[0].(mod10)), 2; got != want {
		t.Errorf("h.Records()[0] = %v, want %v", got, want)
	}
	if got, want := int(h.Latest().(mod10)), 2; got != want {
		t.Errorf("h.Latest() = %v, want %v", got, want)
	}

	// Add value that IS a "duplicate".
	h.Add(mod10(12))
	// Records()[0] doesn't change.
	if got, want := int(h.Records()[0].(mod10)), 2; got != want {
		t.Errorf("h.Records()[0] = %v, want %v", got, want)
	}
	// Latest() does change.
	if got, want := int(h.Latest().(mod10)), 12; got != want {
		t.Errorf("h.Latest() = %v, want %v", got, want)
	}
}

type duplic int

func (d duplic) IsDuplicate(other interface{}) bool {
	return d == other
}

func TestIsEquivalent(t *testing.T) {
	q := New(4)
	q.Add(duplic(0))
	q.Add(duplic(0))
	if got, want := len(q.Records()), 1; got != want {
		t.Errorf("len(q.Records())=%v, want %v", got, want)
	}
}

type mod10 int

func (m mod10) IsDuplicate(other interface{}) bool {
	return m%10 == other.(mod10)%10
}
