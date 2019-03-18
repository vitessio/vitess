/*
Copyright 2017 Google Inc.

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

package vtgate

import (
	"encoding/json"
	"flag"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"vitess.io/vitess/go/stats"
)

type TimingStats struct {
	totalQueryTime prometheus.Summary
}

type TimingMeasurement struct {
	Median      float64
	NinetyNinth float64
}

type TimingMeasurements struct {
	TotalQueryTime TimingMeasurement
}

var (
	TimingStatistics = &TimingStats{totalQueryTime: prometheus.NewSummary(
		prometheus.SummaryOpts{
			Name:       "total_query_time",
			Help:       "Distributions of total time for vttablet queries.",
			Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001},
			MaxAge:     time.Minute})}
	timingStatsEnabled = flag.Bool("enable-aggregate-vttablet-timings", false, "This enables median and 99th timings for all queries.")

	publishOnce sync.Once
)

func (timingStats *TimingStats) recordStats(logStats *LogStats) {
	if *timingStatsEnabled {
		timingStats.registerIfNeeded()
		timingStats.totalQueryTime.Observe(float64(logStats.TotalTime().Nanoseconds()))
	}
}

func (timingStats *TimingStats) GetMeasurementsJson() string {
	b, err := json.MarshalIndent(TimingStatistics.GetMeasurements(), "", " ")
	if err != nil {
		return "{}"
	} else {
		return string(b)
	}
}

func (timingStats *TimingStats) GetMeasurements() TimingMeasurements {
	return TimingMeasurements{TotalQueryTime: getMeasurement(timingStats.totalQueryTime)}
}

func (timingStats *TimingStats) registerIfNeeded() {
	publishOnce.Do(func() {
		stats.PublishJSONFunc("AggregateQueryTimings", func() string {
			return TimingStatistics.GetMeasurementsJson()
		})
	})
}

func getMeasurement(summary prometheus.Summary) TimingMeasurement {
	dtoMetric := &dto.Metric{}
	summary.Write(dtoMetric)
	return TimingMeasurement{Median: dtoMetric.Summary.GetQuantile()[0].GetValue(),
		NinetyNinth: dtoMetric.Summary.GetQuantile()[1].GetValue()}
}
