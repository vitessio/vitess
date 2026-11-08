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

package grpctmserver

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tmrpctest"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

type backupStreamRecorder struct {
	grpc.ServerStream
	ctx        context.Context
	inFlight   atomic.Int32
	overlapped atomic.Bool
	mu         sync.Mutex
	sent       []*tabletmanagerdatapb.BackupResponse
	sendErr    error
}

func (recorder *backupStreamRecorder) Context() context.Context {
	return recorder.ctx
}

func (recorder *backupStreamRecorder) Send(response *tabletmanagerdatapb.BackupResponse) error {
	if recorder.inFlight.Add(1) > 1 {
		recorder.overlapped.Store(true)
	}
	defer recorder.inFlight.Add(-1)
	runtime.Gosched()
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.sent = append(recorder.sent, response)
	return recorder.sendErr
}

type restoreStreamRecorder struct {
	*backupStreamRecorder
}

func (recorder *restoreStreamRecorder) Send(response *tabletmanagerdatapb.RestoreFromBackupResponse) error {
	return recorder.backupStreamRecorder.Send(&tabletmanagerdatapb.BackupResponse{Event: response.Event})
}

type concurrentBackupTM struct {
	tabletmanager.RPCTM
	backupErr  error
	restoreErr error
}

func (tm *concurrentBackupTM) Backup(_ context.Context, logger logutil.Logger, _ *tabletmanagerdatapb.BackupRequest) error {
	logConcurrentProgress(logger)
	return tm.backupErr
}

func (tm *concurrentBackupTM) RestoreFromBackup(_ context.Context, logger logutil.Logger, _ *tabletmanagerdatapb.RestoreFromBackupRequest) error {
	logConcurrentProgress(logger)
	return tm.restoreErr
}

func logConcurrentProgress(logger logutil.Logger) {
	var workers sync.WaitGroup
	start := make(chan struct{})
	for workerIndex := range 4 {
		workers.Go(func() {
			<-start
			for messageIndex := range 10 {
				logger.Infof("progress %d/%d", workerIndex, messageIndex)
			}
		})
	}
	close(start)
	workers.Wait()
}

// TestBackupSerializesLogEvents exercises concurrent engine logging through the RPC handler.
func TestBackupSerializesLogEvents(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		backupErr error
		sendErr   error
	}{
		{name: "successful backup"},
		{name: "failed backup", backupErr: errors.New("backup failed")},
		{name: "send error does not interrupt backup", sendErr: errors.New("client disconnected")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := &backupStreamRecorder{ctx: t.Context(), sendErr: testCase.sendErr}
			handler := &server{tm: &concurrentBackupTM{
				RPCTM:     tmrpctest.NewFakeRPCTM(t),
				backupErr: testCase.backupErr,
			}}
			err := handler.Backup(&tabletmanagerdatapb.BackupRequest{}, recorder)
			if testCase.backupErr != nil {
				require.ErrorIs(t, err, testCase.backupErr)
			} else {
				require.NoError(t, err)
			}
			assert.False(t, recorder.overlapped.Load(), "concurrent Send on the Backup stream")
			require.Len(t, recorder.sent, 40)
			for _, response := range recorder.sent {
				require.NotNil(t, response.Event)
			}
		})
	}
}

// TestRestoreSerializesLogEvents exercises concurrent restore logging through the RPC handler.
func TestRestoreSerializesLogEvents(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		restoreErr error
		sendErr    error
	}{
		{name: "successful restore"},
		{name: "failed restore", restoreErr: errors.New("restore failed")},
		{name: "send error does not interrupt restore", sendErr: errors.New("client disconnected")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := &restoreStreamRecorder{backupStreamRecorder: &backupStreamRecorder{ctx: t.Context(), sendErr: testCase.sendErr}}
			handler := &server{tm: &concurrentBackupTM{
				RPCTM:      tmrpctest.NewFakeRPCTM(t),
				restoreErr: testCase.restoreErr,
			}}
			err := handler.RestoreFromBackup(&tabletmanagerdatapb.RestoreFromBackupRequest{}, recorder)
			if testCase.restoreErr != nil {
				require.ErrorIs(t, err, testCase.restoreErr)
			} else {
				require.NoError(t, err)
			}
			assert.False(t, recorder.overlapped.Load(), "concurrent Send on the RestoreFromBackup stream")
			require.Len(t, recorder.sent, 40)
			for _, response := range recorder.sent {
				require.NotNil(t, response.Event)
			}
		})
	}
}
