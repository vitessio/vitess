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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tmrpctest"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

type lateLoggingBackupTM struct {
	tabletmanager.RPCTM
	background context.Context
	done       chan struct{}
	logger     logutil.Logger
	backupErr  error
}

func (tm *lateLoggingBackupTM) Backup(_ context.Context, logger logutil.Logger, _ *tabletmanagerdatapb.BackupRequest) (mysqlctl.BackupOutcome, error) {
	tm.logger = logger
	started := make(chan struct{})
	go func() {
		defer close(tm.done)
		logger.Infof("progress")
		close(started)
		for {
			select {
			case <-tm.background.Done():
				return
			default:
				logger.Infof("late progress")
			}
		}
	}()
	<-started
	return mysqlctl.BackupOutcome{Name: "b1", Result: mysqlctl.BackupUsable}, tm.backupErr
}

// TestBackupSerializesTerminalMessage checks handler wiring while the tablet keeps logging after returning.
func TestBackupSerializesTerminalMessage(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		backupErr error
	}{
		{name: "successful backup"},
		{name: "stored backup followed by an error", backupErr: errors.New("tablet restoration failed")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			background, cancel := context.WithCancel(t.Context())
			tm := &lateLoggingBackupTM{
				RPCTM:      tmrpctest.NewFakeRPCTM(t),
				background: background,
				done:       make(chan struct{}),
				backupErr:  testCase.backupErr,
			}
			t.Cleanup(func() {
				cancel()
				<-tm.done
			})
			recorder := &backupStreamRecorder{ctx: t.Context()}
			handler := &server{tm: tm}
			err := handler.Backup(&tabletmanagerdatapb.BackupRequest{IncludeOutcome: true}, recorder)
			tm.logger.Infof("after terminal")
			cancel()
			<-tm.done
			if testCase.backupErr != nil {
				require.ErrorIs(t, err, testCase.backupErr)
			} else {
				require.NoError(t, err)
			}
			assert.False(t, recorder.overlapped.Load(), "concurrent Send on the Backup stream")
			require.NotEmpty(t, recorder.sent)
			terminal := recorder.sent[len(recorder.sent)-1]
			assert.Equal(t, tabletmanagerdatapb.BackupResponse_USABLE, terminal.Status)
			assert.Equal(t, "b1", terminal.BackupName)
			assert.Nil(t, terminal.Event)
		})
	}
}
