package tabletmanager

import (
	"errors"
	"testing"
	"time"
)

func TestHealthRecordDeduplication(t *testing.T) {
	now := time.Now()
	later := now.Add(5 * time.Minute)
	cases := []struct {
		left, right *HealthRecord
		duplicate   bool
	}{
		{
			left:      &HealthRecord{Time: now},
			right:     &HealthRecord{Time: later},
			duplicate: true,
		},
		{
			left:      &HealthRecord{Time: now, Error: errors.New("foo")},
			right:     &HealthRecord{Time: now, Error: errors.New("foo")},
			duplicate: true,
		},
		{
			left:      &HealthRecord{Time: now, Result: map[string]string{"a": "1"}},
			right:     &HealthRecord{Time: later, Result: map[string]string{"a": "1"}},
			duplicate: true,
		},
		{
			left:      &HealthRecord{Time: now, Result: map[string]string{"a": "1"}},
			right:     &HealthRecord{Time: later, Result: map[string]string{"a": "2"}},
			duplicate: false,
		},
		{
			left:      &HealthRecord{Time: now, Error: errors.New("foo"), Result: map[string]string{"a": "1"}},
			right:     &HealthRecord{Time: later, Result: map[string]string{"a": "1"}},
			duplicate: false,
		},
	}

	for _, c := range cases {
		if got := c.left.IsDuplicate(c.right); got != c.duplicate {
			t.Errorf("IsDuplicate %v and %v: got %v, want %v", c.left, c.right, got, c.duplicate)
		}
	}
}

func TestHealthRecordClass(t *testing.T) {
	cases := []struct {
		r     *HealthRecord
		state string
	}{
		{
			r:     &HealthRecord{},
			state: "healthy",
		},
		{
			r:     &HealthRecord{Error: errors.New("foo")},
			state: "unhealthy",
		},
		{
			r:     &HealthRecord{Result: map[string]string{"1": "1"}},
			state: "unhappy",
		},
		{
			r:     &HealthRecord{Result: map[string]string{}},
			state: "healthy",
		},
	}

	for _, c := range cases {
		if got := c.r.Class(); got != c.state {
			t.Errorf("class of %v: got %v, want %v", c.r, got, c.state)
		}
	}
}
