/*
Copyright 2023 The Vitess Authors.

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

// This codebase originates from https://github.com/github/freno, See https://github.com/github/freno/blob/master/LICENSE
/*
	MIT License

	Copyright (c) 2017 GitHub

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

package throttle

import (
	"context"
	"sort"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/mysql"
)

func aggregateMySQLProbes(
	ctx context.Context,
	probes mysql.Probes,
	clusterName string,
	tabletResultsMap mysql.TabletResultMap,
	ignoreHostsCount int,
	IgnoreDialTCPErrors bool,
	ignoreHostsThreshold float64,
) (worstMetric base.MetricResult) {
	// probes is known not to change. It can be *replaced*, but not changed.
	// so it's safe to iterate it
	probeValues := []float64{}
	for _, probe := range probes {
		tabletMetricResult, ok := tabletResultsMap[mysql.GetClusterTablet(clusterName, probe.Alias)]
		if !ok {
			return base.NoMetricResultYet
		}

		value, err := tabletMetricResult.Get()
		if err != nil {
			if IgnoreDialTCPErrors && base.IsDialTCPError(err) {
				continue
			}
			if ignoreHostsCount > 0 {
				// ok to skip this error
				ignoreHostsCount = ignoreHostsCount - 1
				continue
			}
			return tabletMetricResult
		}

		// No error
		probeValues = append(probeValues, value)
	}
	if len(probeValues) == 0 {
		return base.NoHostsMetricResult
	}

	// If we got here, that means no errors (or good-to-skip errors)
	sort.Float64s(probeValues)
	// probeValues sorted ascending (from best, ie smallest, to worst, ie largest)
	for ignoreHostsCount > 0 {
		goodToIgnore := func() bool {
			// Note that these hosts don't have errors
			numProbeValues := len(probeValues)
			if numProbeValues <= 1 {
				// We wish to retain at least one host
				return false
			}
			if ignoreHostsThreshold <= 0 {
				// No threshold conditional (or implicitly "any value exceeds the threshold")
				return true
			}
			if worstValue := probeValues[numProbeValues-1]; worstValue > ignoreHostsThreshold {
				return true
			}
			return false
		}()
		if goodToIgnore {
			probeValues = probeValues[0 : len(probeValues)-1]
		}
		// And, whether ignored or not, we are reducing our tokens
		ignoreHostsCount = ignoreHostsCount - 1
	}
	worstValue := probeValues[len(probeValues)-1]
	worstMetric = base.NewSimpleMetricResult(worstValue)
	return worstMetric
}
