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

package tabletmanager

import (
	"context"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/stats"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

var (
	statsThrottlerCheckRequests  = stats.NewCounter("ThrottlerCheckRequest", "CheckThrottler requests")
	statsThrottlerStatusRequests = stats.NewCounter("GetThrottlerStatusRequest", "GetThrottlerStatus requests")
)

// CheckThrottler executes a throttler check
func (tm *TabletManager) CheckThrottler(ctx context.Context, req *tabletmanagerdatapb.CheckThrottlerRequest) (*tabletmanagerdatapb.CheckThrottlerResponse, error) {
	statsThrottlerCheckRequests.Add(1)
	if req.AppName == "" {
		req.AppName = throttlerapp.VitessName.String()
	}
	flags := &throttle.CheckFlags{
		Scope:                 base.Scope(req.Scope),
		SkipRequestHeartbeats: req.SkipRequestHeartbeats,
		OKIfNotExists:         req.OkIfNotExists,
		MultiMetricsEnabled:   req.MultiMetricsEnabled,
	}
	checkResult := tm.QueryServiceControl.CheckThrottler(ctx, req.AppName, flags)
	if checkResult == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "nil checkResult")
	}
	resp := &tabletmanagerdatapb.CheckThrottlerResponse{
		ResponseCode:    throttle.ResponseCodeFromStatus(checkResult.ResponseCode, checkResult.StatusCode),
		StatusCode:      int32(checkResult.StatusCode),
		Value:           checkResult.Value,
		Threshold:       checkResult.Threshold,
		Message:         checkResult.Message,
		RecentlyChecked: checkResult.RecentlyChecked,
		AppName:         checkResult.AppName,
		Summary:         checkResult.Summary(),
		Metrics:         make(map[string]*tabletmanagerdatapb.CheckThrottlerResponse_Metric),
	}
	for name, metric := range checkResult.Metrics {
		resp.Metrics[name] = &tabletmanagerdatapb.CheckThrottlerResponse_Metric{
			Name:         name,
			Scope:        metric.Scope,
			StatusCode:   int32(metric.StatusCode),
			ResponseCode: throttle.ResponseCodeFromStatus(metric.ResponseCode, metric.StatusCode),
			Value:        metric.Value,
			Threshold:    metric.Threshold,
			Message:      metric.Message,
		}
	}
	if len(checkResult.Metrics) == 0 {
		// For backwards compatibility, when the checked tablet is of lower version, it does not return a
		// matrics map, but only the one metric.
		resp.Metrics[base.DefaultMetricName.String()] = &tabletmanagerdatapb.CheckThrottlerResponse_Metric{
			StatusCode:   int32(checkResult.StatusCode),
			ResponseCode: throttle.ResponseCodeFromStatus(checkResult.ResponseCode, checkResult.StatusCode),
			Value:        checkResult.Value,
			Threshold:    checkResult.Threshold,
			Message:      checkResult.Message,
		}
	}
	if checkResult.Error != nil {
		resp.Error = checkResult.Error.Error()
	}
	return resp, nil
}

// GetThrottlerStatus returns a detailed breakdown of the throttler status
func (tm *TabletManager) GetThrottlerStatus(ctx context.Context, req *tabletmanagerdatapb.GetThrottlerStatusRequest) (*tabletmanagerdatapb.GetThrottlerStatusResponse, error) {
	statsThrottlerStatusRequests.Add(1)
	status := tm.QueryServiceControl.GetThrottlerStatus(ctx)
	if status == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "nil status")
	}
	resp := &tabletmanagerdatapb.GetThrottlerStatusResponse{
		TabletAlias:             topoproto.TabletAliasString(tm.Tablet().Alias),
		Keyspace:                status.Keyspace,
		Shard:                   status.Shard,
		IsLeader:                status.IsLeader,
		IsOpen:                  status.IsOpen,
		IsEnabled:               status.IsEnabled,
		IsDormant:               status.IsDormant,
		RecentlyChecked:         status.RecentlyChecked,
		LagMetricQuery:          status.Query,
		CustomMetricQuery:       status.CustomQuery,
		DefaultThreshold:        status.Threshold,
		MetricNameUsedAsDefault: status.MetricNameUsedAsDefault,
		AggregatedMetrics:       make(map[string]*tabletmanagerdatapb.GetThrottlerStatusResponse_MetricResult),
		MetricThresholds:        status.MetricsThresholds,
		MetricsHealth:           make(map[string]*tabletmanagerdatapb.GetThrottlerStatusResponse_MetricHealth),
		ThrottledApps:           make(map[string]*topodatapb.ThrottledAppRule),
		AppCheckedMetrics:       status.AppCheckedMetrics,
		RecentApps:              make(map[string]*tabletmanagerdatapb.GetThrottlerStatusResponse_RecentApp),
	}
	for k, m := range status.AggregatedMetrics {
		val, err := m.Get()
		resp.AggregatedMetrics[k] = &tabletmanagerdatapb.GetThrottlerStatusResponse_MetricResult{
			Value: val,
		}
		if err != nil {
			resp.AggregatedMetrics[k].Error = err.Error()
		}
	}
	for k, m := range status.MetricsHealth {
		resp.MetricsHealth[k] = &tabletmanagerdatapb.GetThrottlerStatusResponse_MetricHealth{
			LastHealthyAt:           protoutil.TimeToProto(m.LastHealthyAt),
			SecondsSinceLastHealthy: m.SecondsSinceLastHealthy,
		}
	}
	for _, app := range status.ThrottledApps {
		resp.ThrottledApps[app.AppName] = &topodatapb.ThrottledAppRule{
			Name:      app.AppName,
			Ratio:     app.Ratio,
			ExpiresAt: protoutil.TimeToProto(app.ExpireAt),
			Exempt:    app.Exempt,
		}
	}
	for _, recentApp := range status.RecentApps {
		resp.RecentApps[recentApp.AppName] = &tabletmanagerdatapb.GetThrottlerStatusResponse_RecentApp{
			CheckedAt:    protoutil.TimeToProto(recentApp.CheckedAt),
			StatusCode:   int32(recentApp.StatusCode),
			ResponseCode: recentApp.ResponseCode,
		}
	}
	return resp, nil
}
