package health

import (
	"errors"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
)

func TestReporters(t *testing.T) {

	ag := NewAggregator()
	ag.Register("a", FunctionReporter(func(typ topo.TabletType) (Status, error) {
		return map[string]string{"key": "value"}, nil
	}))

	ag.Register("b", FunctionReporter(func(typ topo.TabletType) (Status, error) {
		return map[string]string{"key": "value"}, nil
	}))

	status, err := ag.Run(topo.TYPE_REPLICA)

	if err != nil {
		t.Error(err)
	}
	if want := Status(map[string]string{"a.key": "value", "b.key": "value"}); !reflect.DeepEqual(status, want) {
		t.Errorf("status=%#v, want %#v", status, want)
	}

	ag.Register("c", FunctionReporter(func(typ topo.TabletType) (Status, error) {
		return nil, errors.New("c error")
	}))
	if _, err := ag.Run(topo.TYPE_REPLICA); err == nil {
		t.Errorf("ag.Run: expected error")
	}
}
