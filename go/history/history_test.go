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
