/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
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
	probes *mysql.Probes,
	clusterName string,
	instanceResultsMap mysql.InstanceMetricResultMap,
	ignoreHostsCount int,
	IgnoreDialTCPErrors bool,
	ignoreHostsThreshold float64,
) (worstMetric base.MetricResult) {
	// probes is known not to change. It can be *replaced*, but not changed.
	// so it's safe to iterate it
	probeValues := []float64{}
	for _, probe := range *probes {
		instanceMetricResult, ok := instanceResultsMap[mysql.GetClusterInstanceKey(clusterName, &probe.Key)]
		if !ok {
			return base.NoMetricResultYet
		}

		value, err := instanceMetricResult.Get()
		if err != nil {
			if IgnoreDialTCPErrors && base.IsDialTCPError(err) {
				continue
			}
			if ignoreHostsCount > 0 {
				// ok to skip this error
				ignoreHostsCount = ignoreHostsCount - 1
				continue
			}
			return instanceMetricResult
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
