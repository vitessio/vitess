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

package inst

/*
  query holds information about query metrics and records the time taken
  waiting before doing the query plus the time taken executing the query.
*/
import (
	"time"

	"vitess.io/vitess/go/vt/orchestrator/collection"
	"vitess.io/vitess/go/vt/orchestrator/config"

	"github.com/montanaflynn/stats"
)

// Metric records query metrics of backend writes that go through
// a sized channel.  It allows us to compare the time waiting to
// execute the query against the time needed to run it and in a
// "sized channel" the wait time may be significant and is good to
// measure.
type WriteBufferMetric struct {
	Timestamp    time.Time     // time the metric was started
	Instances    int           // number of flushed instances
	WaitLatency  time.Duration // waiting before flush
	WriteLatency time.Duration // time writing to backend
}

// When records the timestamp of the start of the recording
func (m WriteBufferMetric) When() time.Time {
	return m.Timestamp
}

type AggregatedWriteBufferMetric struct {
	InstanceWriteBufferSize           int // config setting
	InstanceFlushIntervalMilliseconds int // config setting
	CountInstances                    int
	MaxInstances                      float64
	MeanInstances                     float64
	MedianInstances                   float64
	P95Instances                      float64
	MaxWaitSeconds                    float64
	MeanWaitSeconds                   float64
	MedianWaitSeconds                 float64
	P95WaitSeconds                    float64
	MaxWriteSeconds                   float64
	MeanWriteSeconds                  float64
	MedianWriteSeconds                float64
	P95WriteSeconds                   float64
}

// AggregatedSince returns the aggregated query metrics for the period
// given from the values provided.
func AggregatedSince(c *collection.Collection, t time.Time) AggregatedWriteBufferMetric {

	// Initialise timing metrics
	var instancesCounter []float64
	var waitTimings []float64
	var writeTimings []float64

	// Retrieve values since the time specified
	values, err := c.Since(t)
	a := AggregatedWriteBufferMetric{
		InstanceWriteBufferSize:           config.Config.InstanceWriteBufferSize,
		InstanceFlushIntervalMilliseconds: config.Config.InstanceFlushIntervalMilliseconds,
	}
	if err != nil {
		return a // empty data
	}

	// generate the metrics
	for _, v := range values {
		instancesCounter = append(instancesCounter, float64(v.(*WriteBufferMetric).Instances))
		waitTimings = append(waitTimings, v.(*WriteBufferMetric).WaitLatency.Seconds())
		writeTimings = append(writeTimings, v.(*WriteBufferMetric).WriteLatency.Seconds())
		a.CountInstances += v.(*WriteBufferMetric).Instances
	}

	// generate aggregate values
	if s, err := stats.Max(stats.Float64Data(instancesCounter)); err == nil {
		a.MaxInstances = s
	}
	if s, err := stats.Mean(stats.Float64Data(instancesCounter)); err == nil {
		a.MeanInstances = s
	}
	if s, err := stats.Median(stats.Float64Data(instancesCounter)); err == nil {
		a.MedianInstances = s
	}
	if s, err := stats.Percentile(stats.Float64Data(instancesCounter), 95); err == nil {
		a.P95Instances = s
	}
	if s, err := stats.Max(stats.Float64Data(waitTimings)); err == nil {
		a.MaxWaitSeconds = s
	}
	if s, err := stats.Mean(stats.Float64Data(waitTimings)); err == nil {
		a.MeanWaitSeconds = s
	}
	if s, err := stats.Median(stats.Float64Data(waitTimings)); err == nil {
		a.MedianWaitSeconds = s
	}
	if s, err := stats.Percentile(stats.Float64Data(waitTimings), 95); err == nil {
		a.P95WaitSeconds = s
	}
	if s, err := stats.Max(stats.Float64Data(writeTimings)); err == nil {
		a.MaxWriteSeconds = s
	}
	if s, err := stats.Mean(stats.Float64Data(writeTimings)); err == nil {
		a.MeanWriteSeconds = s
	}
	if s, err := stats.Median(stats.Float64Data(writeTimings)); err == nil {
		a.MedianWriteSeconds = s
	}
	if s, err := stats.Percentile(stats.Float64Data(writeTimings), 95); err == nil {
		a.P95WriteSeconds = s
	}

	return a
}
