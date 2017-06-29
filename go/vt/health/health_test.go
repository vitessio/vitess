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
	"html/template"
	"testing"
	"time"
)

func TestReporters(t *testing.T) {
	tests := []struct {
		isSlaveType                 bool
		shouldQueryServiceBeRunning bool
		delay1                      time.Duration
		delay2                      time.Duration
		err                         error
		wantDelay                   time.Duration
		strict                      bool
	}{
		{true, true, 10 * time.Second, 5 * time.Second, nil, 10 * time.Second, false},
		{true, false, 10 * time.Second, 5 * time.Second, errors.New("oops"), 0, false},
		{true, false, 10 * time.Second, 5 * time.Second, ErrSlaveNotRunning, 10 * time.Second, true},
	}
	for _, test := range tests {
		ag := NewAggregator()
		ag.Register("1", FunctionReporter(func(bool, bool) (time.Duration, error) {
			return test.delay1, nil
		}))
		ag.Register("2", FunctionReporter(func(bool, bool) (time.Duration, error) {
			return test.delay2, nil
		}))
		ag.RegisterSimpleCheck("simplecheck", func() error { return test.err })
		delay, err := ag.Report(test.isSlaveType, test.shouldQueryServiceBeRunning)
		if delay != test.wantDelay || test.strict && err != test.err || (err == nil) != (test.err == nil) {
			t.Errorf("ag.Report(%v, %v) = (%v, %v), want (%v, %v)",
				test.isSlaveType, test.shouldQueryServiceBeRunning, delay, err, test.wantDelay, test.err)
		}
		wantHTML := template.HTML("FunctionReporter&nbsp; + &nbsp;FunctionReporter&nbsp; + &nbsp;simplecheck")
		if got, want := ag.HTMLName(), wantHTML; got != want {
			t.Errorf("ag.HTMLName() = %v, want %v", got, want)
		}
	}
}
