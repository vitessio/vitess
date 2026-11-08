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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// sendRecorder is a Backup stream that records what is sent and fails the test
// if two Sends overlap.
type sendRecorder struct {
	grpc.ServerStream
	t        *testing.T
	inFlight atomic.Int32
	mu       sync.Mutex
	sent     []*tabletmanagerdatapb.BackupResponse
}

func (r *sendRecorder) Send(resp *tabletmanagerdatapb.BackupResponse) error {
	if r.inFlight.Add(1) > 1 {
		r.t.Error("concurrent Send on the Backup stream")
	}
	defer r.inFlight.Add(-1)
	time.Sleep(time.Millisecond) // widen the window for an overlapping Send
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, resp)
	return nil
}

// TestBackupSender verifies that log events from several goroutines and the
// terminal message never overlap, and that no event follows the terminal
// message, as when an engine's progress goroutine logs after Backup returns.
func TestBackupSender(t *testing.T) {
	rec := &sendRecorder{t: t}
	sender := &backupSender{stream: rec}

	// Events from several goroutines, as from an engine's progress goroutines.
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 5 {
				sender.event(&logutilpb.Event{Value: "progress"})
			}
		})
	}
	wg.Wait()

	// The terminal message, while a goroutine keeps logging.
	var stop atomic.Bool
	wg.Go(func() {
		for !stop.Load() {
			sender.event(&logutilpb.Event{Value: "late progress"})
		}
	})
	time.Sleep(5 * time.Millisecond)
	terminal := &tabletmanagerdatapb.BackupResponse{Status: tabletmanagerdatapb.BackupResponse_USABLE, BackupName: "b1"}
	require.NoError(t, sender.last(terminal))
	stop.Store(true)
	wg.Wait()

	assert.Same(t, terminal, rec.sent[len(rec.sent)-1], "the terminal message must be the last one sent")
}
