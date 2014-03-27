package health

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
)

func TestReporters(t *testing.T) {

	ag := NewAggregator()

	ag.Register("a", FunctionReporter(func(typ topo.TabletType) (map[string]string, error) {
		return map[string]string{"a": "value", "b": "value"}, nil
	}))

	ag.Register("b", FunctionReporter(func(typ topo.TabletType) (map[string]string, error) {
		return map[string]string{"c": "value"}, nil
	}))

	status, err := ag.Run(topo.TYPE_REPLICA)

	if err != nil {
		t.Error(err)
	}
	if want := map[string]string(map[string]string{"a": "value", "b": "value", "c": "value"}); !reflect.DeepEqual(status, want) {
		t.Errorf("status=%#v, want %#v", status, want)
	}

	ag.Register("c", FunctionReporter(func(typ topo.TabletType) (map[string]string, error) {
		return nil, errors.New("e error")
	}))
	if _, err := ag.Run(topo.TYPE_REPLICA); err == nil {
		t.Errorf("ag.Run: expected error")
	}

	// Handle duplicate keys.
	ag.Register("d", FunctionReporter(func(typ topo.TabletType) (map[string]string, error) {
		return map[string]string{"a": "value"}, nil
	}))

	if _, err := ag.Run(topo.TYPE_REPLICA); err == nil {
		t.Errorf("ag.Run: expected error for duplicate keys")
	}
}

func TestRecord(t *testing.T) {
	now := time.Now()
	later := now.Add(5 * time.Minute)
	cases := []struct {
		left, right Record
		duplicate   bool
	}{
		{
			left:      Record{Time: now},
			right:     Record{Time: later},
			duplicate: true,
		},
		{
			left:      Record{Time: now, Error: errors.New("foo")},
			right:     Record{Time: now, Error: errors.New("foo")},
			duplicate: true,
		},
		{
			left:      Record{Time: now, Result: map[string]string{"a": "1"}},
			right:     Record{Time: later, Result: map[string]string{"a": "1"}},
			duplicate: true,
		},
		{
			left:      Record{Time: now, Result: map[string]string{"a": "1"}},
			right:     Record{Time: later, Result: map[string]string{"a": "2"}},
			duplicate: false,
		},
		{
			left:      Record{Time: now, Error: errors.New("foo"), Result: map[string]string{"a": "1"}},
			right:     Record{Time: later, Result: map[string]string{"a": "1"}},
			duplicate: false,
		},
	}

	for _, c := range cases {
		if got := c.left.IsDuplicate(c.right); got != c.duplicate {
			t.Errorf("IsDuplicate %v and %v: got %v, want %v", c.left, c.right, got, c.duplicate)
		}
	}

}
