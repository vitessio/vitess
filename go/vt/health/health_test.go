package health

import (
	"errors"
	"reflect"
	"testing"

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
