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

package tabletenv

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"vitess.io/vitess/go/stats"
)

type TimingStats struct {
	totalQueryTime            prometheus.Summary
	mysqlQueryTime            prometheus.Summary
	connectionAcquisitionTime prometheus.Summary
}

type TimingMeasurement struct {
	Median      float64
	NinetyNinth float64
}

type TimingMeasurements struct {
	TotalQueryTime            TimingMeasurement
	MysqlQueryTime            TimingMeasurement
	ConnectionAcquisitionTime TimingMeasurement
}

var (
	TimingStatistics = &TimingStats{totalQueryTime: prometheus.NewSummary(
		prometheus.SummaryOpts{
			Name:       "total_query_time",
			Help:       "Distributions of total time for vttablet queries.",
			Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001},
			MaxAge:     time.Minute}),
		mysqlQueryTime: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "mysql_query_time",
				Help:       "Distributions of time querying msyql for vttablet queries.",
				Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001},
				MaxAge:     time.Minute}),
		connectionAcquisitionTime: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "connection_acquisition_time",
				Help:       "Distributions of time spent acquiring msyql connections for vttablet queries.",
				Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001},
				MaxAge:     time.Minute})}
	publishOnce sync.Once
)

func (timingStats *TimingStats) recordStats(logStats *LogStats) {
	timingStats.registerIfNeeded()
	timingStats.totalQueryTime.Observe(float64(logStats.TotalTime().Nanoseconds()))
	timingStats.mysqlQueryTime.Observe(float64(logStats.MysqlResponseTime.Nanoseconds()))
	timingStats.connectionAcquisitionTime.Observe(float64(logStats.WaitingForConnection.Nanoseconds()))
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
	return TimingMeasurements{TotalQueryTime: getMeasurement(timingStats.totalQueryTime),
		MysqlQueryTime:            getMeasurement(timingStats.mysqlQueryTime),
		ConnectionAcquisitionTime: getMeasurement(timingStats.connectionAcquisitionTime)}
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
