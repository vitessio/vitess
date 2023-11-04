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

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

// CheckThrottler executes a throttler check
func (tm *TabletManager) CheckThrottler(ctx context.Context, req *tabletmanagerdatapb.CheckThrottlerRequest) (*tabletmanagerdatapb.CheckThrottlerResponse, error) {
	if req.AppName == "" {
		req.AppName = throttlerapp.VitessName.String()
	}
	flags := &throttle.CheckFlags{
		LowPriority:           false,
		SkipRequestHeartbeats: true,
	}
	checkResult := tm.QueryServiceControl.CheckThrottler(ctx, req.AppName, flags)
	if checkResult == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "nil checkResult")
	}
	resp := &tabletmanagerdatapb.CheckThrottlerResponse{
		StatusCode:      int32(checkResult.StatusCode),
		Value:           checkResult.Value,
		Threshold:       checkResult.Threshold,
		Message:         checkResult.Message,
		RecentlyChecked: checkResult.RecentlyChecked,
	}
	if checkResult.Error != nil {
		resp.Error = checkResult.Error.Error()
	}
	return resp, nil
}
