/*
Copyright 2020 The Vitess Authors.

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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestTimingStats_GetMeasurementsJson(t *testing.T) {
	type fields struct {
		totalQueryTime            prometheus.Summary
		mysqlQueryTime            prometheus.Summary
		connectionAcquisitionTime prometheus.Summary
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "stat1",
			fields: fields{
				totalQueryTime: TimingStatistics.totalQueryTime,
			},
			want: "{\n \"TotalQueryTime\": {\n  \"Median\": 2000,\n  \"NinetyNinth\": 50000000\n }\n}",
		},
	}

	tests[0].fields.totalQueryTime.Observe(50000000)
	tests[0].fields.totalQueryTime.Observe(1000)
	tests[0].fields.totalQueryTime.Observe(2000)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timingStats := &TimingStats{
				totalQueryTime: tt.fields.totalQueryTime,
			}
			assert.Equalf(t, tt.want, timingStats.GetMeasurementsJson(), "GetMeasurementsJson()")
		})
	}
}
