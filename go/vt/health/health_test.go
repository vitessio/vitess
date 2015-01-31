package health

import (
	"errors"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
)

func TestReporters(t *testing.T) {

	ag := NewAggregator()

	ag.Register("a", FunctionReporter(func(topo.TabletType, bool) (time.Duration, error) {
		return 10 * time.Second, nil
	}))

	ag.Register("b", FunctionReporter(func(topo.TabletType, bool) (time.Duration, error) {
		return 5 * time.Second, nil
	}))

	delay, err := ag.Run(topo.TYPE_REPLICA, true)

	if err != nil {
		t.Error(err)
	}
	if delay != 10*time.Second {
		t.Errorf("delay=%v, want 10s", delay)
	}

	ag.Register("c", FunctionReporter(func(topo.TabletType, bool) (time.Duration, error) {
		return 0, errors.New("e error")
	}))
	if _, err := ag.Run(topo.TYPE_REPLICA, false); err == nil {
		t.Errorf("ag.Run: expected error")
	}

	name := ag.HTMLName()
	if string(name) != "FunctionReporter&nbsp; + &nbsp;FunctionReporter&nbsp; + &nbsp;FunctionReporter" {
		t.Errorf("ag.HTMLName() returned: %v", name)
	}
}
