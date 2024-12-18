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

package tabletmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

func TestTabletManager_UnresolvedTransactions(t *testing.T) {
	ctx := context.Background()

	qsc := tabletservermock.NewController()
	tm := &TabletManager{
		QueryServiceControl:    qsc,
		Env:                    vtenv.NewTestEnv(),
		_waitForGrantsComplete: make(chan struct{}),
		BatchCtx:               ctx,
	}
	close(tm._waitForGrantsComplete)
	tm.tmState = newTMState(tm, newTestTablet(t, 100, "ks", "-80", nil))

	_, err := tm.GetUnresolvedTransactions(ctx, 0)
	require.NoError(t, err)
	require.True(t, qsc.MethodCalled["UnresolvedTransactions"])
}

func TestTabletManager_ReadTransaction(t *testing.T) {
	ctx := context.Background()

	qsc := tabletservermock.NewController()
	tm := &TabletManager{
		QueryServiceControl:    qsc,
		Env:                    vtenv.NewTestEnv(),
		_waitForGrantsComplete: make(chan struct{}),
		BatchCtx:               ctx,
	}
	close(tm._waitForGrantsComplete)
	tm.tmState = newTMState(tm, newTestTablet(t, 100, "ks", "-80", nil))

	_, err := tm.ReadTransaction(ctx, &tabletmanagerdatapb.ReadTransactionRequest{
		Dtid: "dtid01",
	})
	require.NoError(t, err)
	require.True(t, qsc.MethodCalled["ReadTransaction"])
}

func TestTabletManager_ConcludeTransaction(t *testing.T) {
	ctx := context.Background()

	qsc := tabletservermock.NewController()
	tm := &TabletManager{
		QueryServiceControl:    qsc,
		Env:                    vtenv.NewTestEnv(),
		_waitForGrantsComplete: make(chan struct{}),
		BatchCtx:               ctx,
	}
	close(tm._waitForGrantsComplete)
	tm.tmState = newTMState(tm, newTestTablet(t, 100, "ks", "-80", nil))

	err := tm.ConcludeTransaction(ctx, &tabletmanagerdatapb.ConcludeTransactionRequest{
		Dtid: "dtid01",
		Mm:   false,
	})
	require.NoError(t, err)
	require.True(t, qsc.MethodCalled["RollbackPrepared"])

	err = tm.ConcludeTransaction(ctx, &tabletmanagerdatapb.ConcludeTransactionRequest{
		Dtid: "dtid01",
		Mm:   true,
	})
	require.NoError(t, err)
	require.True(t, qsc.MethodCalled["ConcludeTransaction"])
}
