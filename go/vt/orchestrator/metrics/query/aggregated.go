// Package query provdes query metrics with this file providing
// aggregared metrics based on the underlying values.
package query

import (
	"time"

	"github.com/montanaflynn/stats"

	"vitess.io/vitess/go/vt/orchestrator/collection"
)

type AggregatedQueryMetrics struct {
	// fill me in here
	Count                int
	MaxLatencySeconds    float64
	MeanLatencySeconds   float64
	MedianLatencySeconds float64
	P95LatencySeconds    float64
	MaxWaitSeconds       float64
	MeanWaitSeconds      float64
	MedianWaitSeconds    float64
	P95WaitSeconds       float64
}

// AggregatedSince returns the aggregated query metrics for the period
// given from the values provided.
func AggregatedSince(c *collection.Collection, t time.Time) AggregatedQueryMetrics {

	// Initialise timing metrics
	var waitTimings []float64
	var queryTimings []float64

	// Retrieve values since the time specified
	values, err := c.Since(t)
	a := AggregatedQueryMetrics{}
	if err != nil {
		return a // empty data
	}

	// generate the metrics
	for _, v := range values {
		waitTimings = append(waitTimings, v.(*Metric).WaitLatency.Seconds())
		queryTimings = append(queryTimings, v.(*Metric).ExecuteLatency.Seconds())
	}

	a.Count = len(waitTimings)

	// generate aggregate values
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
	if s, err := stats.Max(stats.Float64Data(queryTimings)); err == nil {
		a.MaxLatencySeconds = s
	}
	if s, err := stats.Mean(stats.Float64Data(queryTimings)); err == nil {
		a.MeanLatencySeconds = s
	}
	if s, err := stats.Median(stats.Float64Data(queryTimings)); err == nil {
		a.MedianLatencySeconds = s
	}
	if s, err := stats.Percentile(stats.Float64Data(queryTimings), 95); err == nil {
		a.P95LatencySeconds = s
	}

	return a
}
