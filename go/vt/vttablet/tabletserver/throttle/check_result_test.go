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
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
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
				ResponseCode: tabletmanagerdatapb.CheckThrottlerResponseCode_OK,
				AppName:      "test",
			},
			summary: "test is granted access",
		},
		{
			checkResult: &CheckResult{
				ResponseCode: tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED,
				AppName:      "test",
				MetricName:   "bugginess",
				Threshold:    100,
				Value:        200,
				Scope:        "self",
			},
			summary: "test is denied access due to self/bugginess metric value 200 exceeding threshold 100",
		},
		{
			checkResult: &CheckResult{
				ResponseCode: tabletmanagerdatapb.CheckThrottlerResponseCode_APP_DENIED,
				AppName:      "test",
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

// TestCheckResultLogAttrs checks that reason is the response code name and
// that the values Summary formats into its text are in their own attributes.
func TestCheckResultLogAttrs(t *testing.T) {
	err := errors.New("metric probe failed")
	tcases := []struct {
		name        string
		checkResult *CheckResult
		attrs       []slog.Attr
	}{
		{
			name: "threshold exceeded",
			checkResult: &CheckResult{
				ResponseCode: tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED,
				AppName:      "test",
				MetricName:   "bugginess",
				Threshold:    100,
				Value:        200,
				Scope:        "self",
			},
			attrs: []slog.Attr{
				slog.String("reason", "THRESHOLD_EXCEEDED"),
				slog.String("app", "test"),
				slog.String("scope", "self"),
				slog.String("metric_name", "bugginess"),
				slog.Float64("metric_value", 200),
				slog.Float64("threshold", 100),
			},
		},
		{
			name: "internal error",
			checkResult: &CheckResult{
				ResponseCode: tabletmanagerdatapb.CheckThrottlerResponseCode_INTERNAL_ERROR,
				AppName:      "test",
				Error:        err,
			},
			attrs: []slog.Attr{
				slog.String("reason", "INTERNAL_ERROR"),
				slog.String("app", "test"),
				slog.Any("error", err),
				slog.String("error_code", "UNKNOWN"),
			},
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			assert.Equal(t, tcase.attrs, tcase.checkResult.LogAttrs())
		})
	}
}
