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

package tabletmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

// throttlerTypeRecorder records the throttler type each throttler RPC asks for.
type throttlerTypeRecorder struct {
	*tabletservermock.Controller
	checkType  tabletmanagerdatapb.ThrottlerType
	statusType tabletmanagerdatapb.ThrottlerType
}

func (r *throttlerTypeRecorder) CheckThrottler(ctx context.Context, appName string, flags *throttle.CheckFlags, throttlerType tabletmanagerdatapb.ThrottlerType) *throttle.CheckResult {
	r.checkType = throttlerType
	return &throttle.CheckResult{}
}

func (r *throttlerTypeRecorder) GetThrottlerStatus(ctx context.Context, throttlerType tabletmanagerdatapb.ThrottlerType) *throttle.ThrottlerStatus {
	r.statusType = throttlerType
	return &throttle.ThrottlerStatus{}
}

// CheckThrottler and GetThrottlerStatus must pass the requested throttler type on to the
// tablet server, so a request for the dedicated query throttler doesn't reach the lag one.
func TestTabletManager_ThrottlerRPCsForwardThrottlerType(t *testing.T) {
	ctx := t.Context()

	for _, throttlerType := range []tabletmanagerdatapb.ThrottlerType{
		tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER,
		tabletmanagerdatapb.ThrottlerType_DEFAULT_LAG_THROTTLER,
	} {
		t.Run(throttlerType.String(), func(t *testing.T) {
			// Start from the other value, so a forwarded zero value can't pass by accident.
			initial := tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER
			if throttlerType == initial {
				initial = tabletmanagerdatapb.ThrottlerType_DEFAULT_LAG_THROTTLER
			}
			qsc := &throttlerTypeRecorder{Controller: tabletservermock.NewController(), checkType: initial, statusType: initial}
			tm := &TabletManager{
				QueryServiceControl: qsc,
				Env:                 vtenv.NewTestEnv(),
				BatchCtx:            ctx,
			}
			tm.tmState = newTMState(tm, newTestTablet(t, 100, "ks", "-80", nil))

			_, err := tm.CheckThrottler(ctx, &tabletmanagerdatapb.CheckThrottlerRequest{ThrottlerType: throttlerType})
			require.NoError(t, err)
			require.Equal(t, throttlerType, qsc.checkType)

			_, err = tm.GetThrottlerStatus(ctx, &tabletmanagerdatapb.GetThrottlerStatusRequest{ThrottlerType: throttlerType})
			require.NoError(t, err)
			require.Equal(t, throttlerType, qsc.statusType)
		})
	}
}
