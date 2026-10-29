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

package prometheusbackend

import (
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log/logtest"
)

func getStats() map[string]int64 {
	stats := make(map[string]int64)
	stats["table1.plan"] = 1
	stats["table2.plan"] = 2
	// Errors are expected to be logged from collectors.go when
	// adding this value (issue #5599).  Test will succeed, as intended.
	stats["table3.dot.plan"] = 3
	return stats
}

func TestCollector(t *testing.T) {
	c := stats.NewCountersFuncWithMultiLabels("Name", "description", []string{"Table", "Plan"}, getStats)
	c.Counts()

	collector := &metricsFuncWithMultiLabelsCollector{
		cfml: c,
		desc: prometheus.NewDesc("name", "description", labelsToSnake(c.Labels()), nil),
		vt:   prometheus.CounterValue,
	}
	logs := logtest.Capture(t, slog.LevelInfo)
	ch := make(chan prometheus.Metric, len(getStats()))
	collector.Collect(ch)
	assert.Len(t, ch, 2)
	record := logs.Record(t, "Error adding metric")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, collector.desc.String(), record["metric_desc"])
}
