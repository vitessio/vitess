package health

import (
	"errors"
	"testing"
	"time"
)

func TestReporters(t *testing.T) {

	ag := NewAggregator()

	ag.Register("a", FunctionReporter(func(bool, bool) (time.Duration, error) {
		return 10 * time.Second, nil
	}))

	ag.Register("b", FunctionReporter(func(bool, bool) (time.Duration, error) {
		return 5 * time.Second, nil
	}))

	delay, err := ag.Report(true, true)

	if err != nil {
		t.Error(err)
	}
	if delay != 10*time.Second {
		t.Errorf("delay=%v, want 10s", delay)
	}

	ag.Register("c", FunctionReporter(func(bool, bool) (time.Duration, error) {
		return 0, errors.New("e error")
	}))
	if _, err := ag.Report(true, false); err == nil {
		t.Errorf("ag.Run: expected error")
	}

	name := ag.HTMLName()
	if string(name) != "FunctionReporter&nbsp; + &nbsp;FunctionReporter&nbsp; + &nbsp;FunctionReporter" {
		t.Errorf("ag.HTMLName() returned: %v", name)
	}
}
