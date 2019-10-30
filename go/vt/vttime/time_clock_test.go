/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vttime

import (
	"flag"
	"testing"
	"time"
)

// Only checking that the results are somewhat OK.
func TestTimeClock(t *testing.T) {
	flag.Set("vttime_default_clock_type", "time")
	flag.Set("vttime_time_clock_uncertainty", "20ms")

	clock := GetClock()
	i, err := clock.Now()
	if err != nil {
		t.Fatalf("Now failed: %v", err)
	}

	// Check the difference is exactly right: it needs to be twice
	// the uncertainty set above with the vttime_time_clock_uncertainty
	// flag.
	d := i.Latest().Sub(i.Earliest())
	if d != 40*time.Millisecond {
		t.Errorf("uncertainty not respected: %v", d)
	}

	// Check we're somewhat in range with time().
	now := time.Now()
	d = now.Sub(i.Earliest())
	if d.Seconds() > 1 || d.Seconds() < -1 {
		t.Errorf("now very late: %v %v %v", i, now, d)
	}
	d = now.Sub(i.Latest())
	if d.Seconds() > 1 || d.Seconds() < -1 {
		t.Errorf("now very early %v %v %v", now, i, d)
	}
}
