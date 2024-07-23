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

package base

import "sort"

// TabletResultMap maps a tablet to a result
type TabletResultMap map[string]MetricResultMap

func (m TabletResultMap) Split(alias string) (withAlias TabletResultMap, all TabletResultMap) {
	withAlias = make(TabletResultMap)
	if val, ok := m[alias]; ok {
		withAlias[alias] = val
	}
	return withAlias, m
}

func AggregateTabletMetricResults(
	metricName MetricName,
	tabletResultsMap TabletResultMap,
	ignoreHostsCount int,
	IgnoreDialTCPErrors bool,
	ignoreHostsThreshold float64,
) (worstMetric MetricResult) {
	// probes is known not to change. It can be *replaced*, but not changed.
	// so it's safe to iterate it
	probeValues := []float64{}
	for _, tabletMetricResults := range tabletResultsMap {
		tabletMetricResult, ok := tabletMetricResults[metricName]
		if !ok {
			return NoSuchMetric
		}
		if tabletMetricResult == nil {
			return NoMetricResultYet
		}
		value, err := tabletMetricResult.Get()
		if err != nil {
			if IgnoreDialTCPErrors && IsDialTCPError(err) {
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
		return NoHostsMetricResult
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
	worstMetric = NewSimpleMetricResult(worstValue)
	return worstMetric
}
