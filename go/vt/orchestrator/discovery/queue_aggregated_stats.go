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

package discovery

import (
	"github.com/montanaflynn/stats"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

// AggregatedQueueMetrics contains aggregate information some part queue metrics
type AggregatedQueueMetrics struct {
	ActiveMinEntries    float64
	ActiveMeanEntries   float64
	ActiveMedianEntries float64
	ActiveP95Entries    float64
	ActiveMaxEntries    float64
	QueuedMinEntries    float64
	QueuedMeanEntries   float64
	QueuedMedianEntries float64
	QueuedP95Entries    float64
	QueuedMaxEntries    float64
}

// we pull out values in ints so convert to float64 for metric calculations
func intSliceToFloat64Slice(someInts []int) stats.Float64Data {
	var slice stats.Float64Data

	for _, v := range someInts {
		slice = append(slice, float64(v))
	}

	return slice
}

// DiscoveryQueueMetrics returns some raw queue metrics based on the
// period (last N entries) requested.
func (q *Queue) DiscoveryQueueMetrics(period int) []QueueMetric {
	q.Lock()
	defer q.Unlock()

	// adjust period in case we ask for something that's too long
	if period > len(q.metrics) {
		log.Debugf("DiscoveryQueueMetrics: wanted: %d, adjusting period to %d", period, len(q.metrics))
		period = len(q.metrics)
	}

	a := q.metrics[len(q.metrics)-period:]
	log.Debugf("DiscoveryQueueMetrics: returning values: %+v", a)
	return a
}

// AggregatedDiscoveryQueueMetrics Returns some aggregate statistics
// based on the period (last N entries) requested.  We store up to
// config.Config.DiscoveryQueueMaxStatisticsSize values and collect once
// a second so we expect period to be a smaller value.
func (q *Queue) AggregatedDiscoveryQueueMetrics(period int) *AggregatedQueueMetrics {
	wanted := q.DiscoveryQueueMetrics(period)

	var activeEntries, queuedEntries []int
	// fill vars
	for i := range wanted {
		activeEntries = append(activeEntries, wanted[i].Active)
		queuedEntries = append(queuedEntries, wanted[i].Queued)
	}

	a := &AggregatedQueueMetrics{
		ActiveMinEntries:    min(intSliceToFloat64Slice(activeEntries)),
		ActiveMeanEntries:   mean(intSliceToFloat64Slice(activeEntries)),
		ActiveMedianEntries: median(intSliceToFloat64Slice(activeEntries)),
		ActiveP95Entries:    percentile(intSliceToFloat64Slice(activeEntries), 95),
		ActiveMaxEntries:    max(intSliceToFloat64Slice(activeEntries)),
		QueuedMinEntries:    min(intSliceToFloat64Slice(queuedEntries)),
		QueuedMeanEntries:   mean(intSliceToFloat64Slice(queuedEntries)),
		QueuedMedianEntries: median(intSliceToFloat64Slice(queuedEntries)),
		QueuedP95Entries:    percentile(intSliceToFloat64Slice(queuedEntries), 95),
		QueuedMaxEntries:    max(intSliceToFloat64Slice(queuedEntries)),
	}
	log.Debugf("AggregatedDiscoveryQueueMetrics: returning values: %+v", a)
	return a
}
