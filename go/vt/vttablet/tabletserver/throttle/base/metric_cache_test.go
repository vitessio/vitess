/*
Copyright 2026 The Vitess Authors.

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

package base

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

// ReadThrottleMetrics records its probe metrics under the given prefix, so each throttler
// instance keeps its own.
func TestReadThrottleMetricsPrefix(t *testing.T) {
	// The counters are process-wide, so compare against where they start, e.g. with -count=2.
	probesTotal := stats.GetOrNewCounter("TestReadThrottleMetricsPrefixProbesTotal", "")
	lagProbesTotal := stats.GetOrNewCounter("ThrottlerProbesTotal", "")
	probes, lagProbes := probesTotal.Get(), lagProbesTotal.Get()

	probe := &Probe{Alias: "test-read-throttle-metrics-prefix"}
	ReadThrottleMetrics(t.Context(), "TestReadThrottleMetricsPrefix", probe, nil, func(context.Context, tmclient.TabletManagerClient) ThrottleMetrics {
		return ThrottleMetrics{}
	})
	assert.Eventually(t, func() bool {
		return probesTotal.Get() > probes
	}, 30*time.Second, 10*time.Millisecond)
	assert.Equal(t, lagProbes, lagProbesTotal.Get(), "the probe must not also count under the lag throttler's prefix")
}
