/*
Copyright 2021 The Vitess Authors.

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

package stress

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFailureQPS(t *testing.T) {
	tests := []struct {
		name    string
		qc      queryCount
		seconds float64
		want    int
	}{
		{name: "Simple QPS", qc: queryCount{failure: 1000}, seconds: 10, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.qc.failureQPS(tt.seconds))
		})
	}
}

func TestMeaningfulFailureQPS(t *testing.T) {
	tests := []struct {
		name    string
		qc      queryCount
		seconds float64
		want    int
	}{
		{name: "Simple QPS", qc: queryCount{meaningfulFailure: 1000}, seconds: 10, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.qc.meaningfulFailureQPS(tt.seconds))
		})
	}
}

func TestSuccessQPS(t *testing.T) {
	tests := []struct {
		name    string
		qc      queryCount
		seconds float64
		want    int
	}{
		{name: "Simple QPS", qc: queryCount{success: 1000}, seconds: 10, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.qc.successQPS(tt.seconds))
		})
	}
}

func TestSum(t *testing.T) {
	tests := []struct {
		name string
		qc   queryCount
		want int
	}{
		{name: "Simple sum", qc: queryCount{success: 1000, failure: 1000}, want: 2000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.qc.sum())
		})
	}
}

func TestTotalQPS(t *testing.T) {
	tests := []struct {
		name    string
		qc      queryCount
		seconds float64
		want    int
	}{
		{name: "Simple total QPS", qc: queryCount{success: 1000, failure: 1000}, seconds: 10, want: 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.qc.totalQPS(tt.seconds))
		})
	}
}

func TestAssert(t *testing.T) {
	tests := []struct {
		name string
		r    result
		want bool
	}{
		{name: "Simple valid", r: result{}, want: true},
		{name: "Simple invalid (only inserts failed)", r: result{inserts: queryCount{meaningfulFailure: 1}}, want: false},
		{name: "Simple invalid (only selects failed)", r: result{selects: queryCount{meaningfulFailure: 1}}, want: false},
		{name: "Simple invalid (only deletes failed)", r: result{deletes: queryCount{meaningfulFailure: 1}}, want: false},
		{name: "Simple invalid (multiple failed)", r: result{inserts: queryCount{meaningfulFailure: 1}, selects: queryCount{meaningfulFailure: 1}, deletes: queryCount{meaningfulFailure: 1}}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.r.assert())
		})
	}
}

func Test_sumQueryCounts(t *testing.T) {
	tests := []struct {
		name string
		qcs  []queryCount
		want queryCount
	}{
		{name: "Simple sum", qcs: []queryCount{
			{success: 1, failure: 1, meaningfulFailure: 1},
			{success: 1, failure: 1, meaningfulFailure: 1},
			{success: 1, failure: 1, meaningfulFailure: 1},
		}, want: queryCount{success: 3, failure: 3, meaningfulFailure: 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryCounts := sumQueryCounts(tt.qcs...)
			require.Equal(t, tt.want.success, queryCounts.success)
			require.Equal(t, tt.want.failure, queryCounts.failure)
			require.Equal(t, tt.want.meaningfulFailure, queryCounts.meaningfulFailure)
		})
	}
}
