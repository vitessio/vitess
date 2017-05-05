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

package health

import (
	"errors"
	"testing"
	"time"
)

func TestReporters(t *testing.T) {

	// two aggregators returning valid numbers
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

	// three aggregators, third one returning an error
	cReturns := errors.New("e error")
	ag.Register("c", FunctionReporter(func(bool, bool) (time.Duration, error) {
		return 0, cReturns
	}))
	if _, err := ag.Report(true, false); err == nil {
		t.Errorf("ag.Run: expected error")
	} else {
		want := "c: e error"
		if got := err.Error(); got != want {
			t.Errorf("got wrong error: got '%v' expected '%v'", got, want)
		}
	}

	// three aggregators, third one returning ErrSlaveNotRunning
	cReturns = ErrSlaveNotRunning
	if _, err := ag.Report(true, false); err != ErrSlaveNotRunning {
		t.Errorf("ag.Run: expected error: %v", err)
	}

	// check name is good
	name := ag.HTMLName()
	if string(name) != "FunctionReporter&nbsp; + &nbsp;FunctionReporter&nbsp; + &nbsp;FunctionReporter" {
		t.Errorf("ag.HTMLName() returned: %v", name)
	}
}
