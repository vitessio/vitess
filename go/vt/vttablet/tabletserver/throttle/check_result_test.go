/*
Copyright 2024 The Vitess Authors.

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

package throttle

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckResultSummary(t *testing.T) {
	tcases := []struct {
		checkResult *CheckResult
		summary     string
	}{
		{
			checkResult: &CheckResult{},
			summary:     "",
		},
		{
			checkResult: &CheckResult{
				StatusCode: http.StatusOK,
				AppName:    "test",
			},
			summary: "test is granted access",
		},
		{
			checkResult: &CheckResult{
				StatusCode: http.StatusTooManyRequests,
				AppName:    "test",
				MetricName: "bugginess",
				Threshold:  100,
				Value:      200,
				Scope:      "self",
			},
			summary: "test is denied access due to self/bugginess metric value 200 exceeding threshold 100",
		},
		{
			checkResult: &CheckResult{
				StatusCode: http.StatusExpectationFailed,
				AppName:    "test",
			},
			summary: "test is explicitly denied access",
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.summary, func(t *testing.T) {
			assert.Equal(t, tcase.summary, tcase.checkResult.Summary())
		})
	}
}
