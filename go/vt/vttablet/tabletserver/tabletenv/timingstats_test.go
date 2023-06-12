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

package tabletenv

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
				totalQueryTime:            TimingStatistics.totalQueryTime,
				mysqlQueryTime:            TimingStatistics.mysqlQueryTime,
				connectionAcquisitionTime: TimingStatistics.connectionAcquisitionTime,
			},
			want: "{\n \"TotalQueryTime\": {\n  \"Median\": 200,\n  \"NinetyNinth\": 400\n },\n \"MysqlQueryTime\": {\n  \"Median\": 200,\n  \"NinetyNinth\": 300\n },\n \"ConnectionAcquisitionTime\": {\n  \"Median\": 20,\n  \"NinetyNinth\": 30\n }\n}",
		},
	}

	tests[0].fields.mysqlQueryTime.Observe(100)
	tests[0].fields.mysqlQueryTime.Observe(200)
	tests[0].fields.mysqlQueryTime.Observe(300)
	tests[0].fields.totalQueryTime.Observe(100)
	tests[0].fields.totalQueryTime.Observe(200)
	tests[0].fields.totalQueryTime.Observe(300)
	tests[0].fields.totalQueryTime.Observe(400)
	tests[0].fields.connectionAcquisitionTime.Observe(10)
	tests[0].fields.connectionAcquisitionTime.Observe(20)
	tests[0].fields.connectionAcquisitionTime.Observe(30)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timingStats := &TimingStats{
				totalQueryTime:            tt.fields.totalQueryTime,
				mysqlQueryTime:            tt.fields.mysqlQueryTime,
				connectionAcquisitionTime: tt.fields.connectionAcquisitionTime,
			}
			assert.Equalf(t, tt.want, timingStats.GetMeasurementsJson(), "GetMeasurementsJson()")
		})
	}
}
