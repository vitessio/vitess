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
	"vitess.io/vitess/go/vt/mysqlctl"
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
	return nil
}

type concurrentBackupTM struct {
	tabletmanager.RPCTM
	backupErr error
}

func (tm *concurrentBackupTM) Backup(_ context.Context, logger logutil.Logger, _ *tabletmanagerdatapb.BackupRequest) (mysqlctl.BackupOutcome, error) {
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
	return mysqlctl.BackupOutcome{}, tm.backupErr
}

// TestBackupSerializesLogEvents exercises concurrent engine logging through the RPC handler.
func TestBackupSerializesLogEvents(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		backupErr error
	}{
		{name: "successful backup"},
		{name: "failed backup", backupErr: errors.New("backup failed")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := &backupStreamRecorder{ctx: t.Context()}
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
