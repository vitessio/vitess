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

package throttle

import (
	"math"
	"time"

	"vitess.io/vitess/go/protoutil"
	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

// configLoader is an interface for loading throttler configuration from SrvKeyspace
type configLoader interface {
	initDBConfig(keyspace, shard, tabletAlias string)
	loadConfig(srvks *topodatapb.SrvKeyspace) *topodatapb.ThrottlerConfig
}

// defaultConfigLoader loads configuration from ThrottlerConfig
type defaultConfigLoader struct{}

func (d *defaultConfigLoader) initDBConfig(keyspace, shard, tabletAlias string) {}

func (d *defaultConfigLoader) loadConfig(srvks *topodatapb.SrvKeyspace) *topodatapb.ThrottlerConfig {
	return srvks.ThrottlerConfig
}

// queryConfigLoader loads configuration from QueryThrottlerConfig and
// converts it to ThrottlerConfig format for the underlying throttle.Throttler.
type queryConfigLoader struct{}

func (q *queryConfigLoader) initDBConfig(keyspace, shard, tabletAlias string) {}

func (q *queryConfigLoader) loadConfig(srvks *topodatapb.SrvKeyspace) *topodatapb.ThrottlerConfig {
	return convertQueryThrottlerConfigToThrottlerConfig(srvks.GetQueryThrottlerConfig())
}

// convertQueryThrottlerConfigToThrottlerConfig converts a querythrottlerpb.Config
// to a topodatapb.ThrottlerConfig. Returns nil if the strategy is not TABLET_THROTTLER
// or if the input is nil.
func convertQueryThrottlerConfigToThrottlerConfig(qtCfg *querythrottlerpb.Config) *topodatapb.ThrottlerConfig {
	if qtCfg == nil {
		return nil
	}

	if qtCfg.GetStrategy() != querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER {
		return nil
	}

	appName := throttlerapp.QueryThrottlerName.String()
	throttlerConfig := &topodatapb.ThrottlerConfig{
		Enabled: qtCfg.GetEnabled(),
		ThrottledApps: map[string]*topodatapb.ThrottledAppRule{
			appName: {
				Name:      appName,
				ExpiresAt: protoutil.TimeToProto(time.Now().Add(time.Hour * 24 * 365)),
				Ratio:     0,
				Exempt:    false,
			},
		},
	}

	metricsNames := make([]string, 0)
	seenMetrics := make(map[string]bool)
	metricThresholds := make(map[string]float64)
	for _, stmtRuleSet := range qtCfg.GetTabletStrategyConfig().GetTabletRules() {
		for _, metricRuleSet := range stmtRuleSet.GetStatementRules() {
			for metricName, rule := range metricRuleSet.GetMetricRules() {
				if thresholds := rule.GetThresholds(); len(thresholds) > 0 {
					if _, ok := seenMetrics[metricName]; !ok {
						metricsNames = append(metricsNames, metricName)
						seenMetrics[metricName] = true
					}
					thresholdValue := thresholds[0].GetAbove()
					if existingValue, exists := metricThresholds[metricName]; exists {
						metricThresholds[metricName] = math.Min(existingValue, thresholdValue)
					} else {
						metricThresholds[metricName] = thresholdValue
					}
				}
			}
		}
	}

	if len(metricsNames) > 0 {
		throttlerConfig.AppCheckedMetrics = map[string]*topodatapb.ThrottlerConfig_MetricNames{
			appName: {
				Names: metricsNames,
			},
		}
	}

	if len(metricThresholds) > 0 {
		throttlerConfig.MetricThresholds = metricThresholds
	}

	return throttlerConfig
}
