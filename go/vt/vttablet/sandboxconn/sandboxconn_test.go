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

package sandboxconn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const canceledCallTimeout = 3 * time.Second
const canceledCallExecDelay = canceledCallTimeout * 2

func waitForCanceledCall(t *testing.T, name string, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(canceledCallTimeout):
		require.FailNowf(t, "%s did not return after context cancellation", name, "timeout: %v", canceledCallTimeout)
		return nil
	}
}

func TestExecuteHonorsCanceledContextDuringExecDelay(t *testing.T) {
	sbc := NewSandboxConn(&topodatapb.Tablet{Type: topodatapb.TabletType_PRIMARY})
	sbc.ExecDelayResponse = canceledCallExecDelay
	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := sbc.Execute(ctx, nil, target, "select 1", nil, 0, 0, nil)
		errCh <- err
	}()

	err := waitForCanceledCall(t, "Execute", errCh)
	require.ErrorIs(t, err, context.Canceled)
}

func TestStreamExecuteHonorsCanceledContextDuringExecDelay(t *testing.T) {
	sbc := NewSandboxConn(&topodatapb.Tablet{Type: topodatapb.TabletType_PRIMARY})
	sbc.ExecDelayResponse = canceledCallExecDelay
	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	callbackCalled := false
	errCh := make(chan error, 1)
	go func() {
		errCh <- sbc.StreamExecute(ctx, nil, target, "select 1", nil, 0, 0, nil, func(*sqltypes.Result) error {
			callbackCalled = true
			return nil
		})
	}()

	err := waitForCanceledCall(t, "StreamExecute", errCh)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, callbackCalled)

	sbc.ExecDelayResponse = 0
	retryCallbackCalled := false
	err = sbc.StreamExecute(t.Context(), nil, target, "select 1", nil, 0, 0, nil, func(*sqltypes.Result) error {
		retryCallbackCalled = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, retryCallbackCalled)
}
