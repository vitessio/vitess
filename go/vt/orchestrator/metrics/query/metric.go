/*
   Copyright 2017 Simon J Mudd

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

package query

/*
  query holds information about query metrics and records the time taken
  waiting before doing the query plus the time taken executing the query.
*/
import (
	"time"
)

// Metric records query metrics of backend writes that go through
// a sized channel.  It allows us to compare the time waiting to
// execute the query against the time needed to run it and in a
// "sized channel" the wait time may be significant and is good to
// measure.
type Metric struct {
	Timestamp      time.Time     // time the metric was started
	WaitLatency    time.Duration // time that we had to wait before starting query execution
	ExecuteLatency time.Duration // time the query took to execute
	Err            error         // any error resulting from the query execution
}

// NewMetric returns a new metric with timestamp starting from now
func NewMetric() *Metric {
	bqm := &Metric{
		Timestamp: time.Now(),
	}

	return bqm
}

// When records the timestamp of the start of the recording
func (m Metric) When() time.Time {
	return m.Timestamp
}
