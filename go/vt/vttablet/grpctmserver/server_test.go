/*
Copyright 2019 The Vitess Authors.

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

package grpctmserver_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpccommon"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vttablet/grpctmclient"
	"vitess.io/vitess/go/vt/vttablet/grpctmserver"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tmrpctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestGRPCTMServer creates a fake server implementation, a fake client
// implementation, and runs the test suite against the setup.
func TestGRPCTMServer(t *testing.T) {
	// Listen on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := int32(listener.Addr().(*net.TCPAddr).Port)

	// Create a gRPC server and listen on the port.
	s := grpc.NewServer()
	fakeTM := tmrpctest.NewFakeRPCTM(t)
	grpctmserver.RegisterForTest(s, fakeTM)
	go s.Serve(listener)

	// Create a gRPC client to talk to the fake tablet.
	client := grpctmclient.NewClient()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test",
			Uid:  123,
		},
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": port,
		},
	}

	// and run the test suite
	tmrpctest.Run(t, client, tablet, fakeTM)
}

// backupOutcomeTM returns a fixed Backup outcome and delegates everything else
// to the shared fake.
type backupOutcomeTM struct {
	tabletmanager.RPCTM
	outcome mysqlctl.BackupOutcome
	err     error
}

func (tm *backupOutcomeTM) Backup(ctx context.Context, logger logutil.Logger, request *tabletmanagerdatapb.BackupRequest) (mysqlctl.BackupOutcome, error) {
	logger.Infof("backup progress")
	return tm.outcome, tm.err
}

// startServer serves tm over gRPC and returns a tablet record pointing at it.
func startServer(t *testing.T, tm tabletmanager.RPCTM, opts ...grpc.ServerOption) *topodatapb.Tablet {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer(opts...)
	grpctmserver.RegisterForTest(s, tm)
	go s.Serve(listener)
	t.Cleanup(s.Stop)

	return &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "test", Uid: 123},
		Hostname: listener.Addr().(*net.TCPAddr).IP.String(),
		PortMap:  map[string]int32{"grpc": int32(listener.Addr().(*net.TCPAddr).Port)},
	}
}

func TestBackupTerminalMessage(t *testing.T) {
	tcs := []struct {
		name       string
		outcome    mysqlctl.BackupOutcome
		backupErr  error
		wantStatus tabletmanagerdatapb.BackupResponse_Status
		wantErr    string
	}{
		{
			name:       "usable",
			outcome:    mysqlctl.BackupOutcome{Name: "b1", Manifest: `{"BackupName":"b1"}`, Result: mysqlctl.BackupUsable},
			wantStatus: tabletmanagerdatapb.BackupResponse_USABLE,
		},
		{
			name:       "empty",
			outcome:    mysqlctl.BackupOutcome{Result: mysqlctl.BackupEmpty},
			wantStatus: tabletmanagerdatapb.BackupResponse_EMPTY,
		},
		{
			name:       "unusable without error",
			outcome:    mysqlctl.BackupOutcome{Result: mysqlctl.BackupUnusable},
			wantStatus: tabletmanagerdatapb.BackupResponse_STATUS_UNSPECIFIED,
		},
		{
			name:      "error",
			outcome:   mysqlctl.BackupOutcome{Result: mysqlctl.BackupUnusable},
			backupErr: errors.New("boom"),
			wantErr:   "boom",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tablet := startServer(t, &backupOutcomeTM{RPCTM: tmrpctest.NewFakeRPCTM(t), outcome: tc.outcome, err: tc.backupErr})
			stream, err := grpctmclient.NewClient().Backup(t.Context(), tablet, &tabletmanagerdatapb.BackupRequest{})
			require.NoError(t, err)

			progress, err := stream.Recv()
			require.NoError(t, err)
			assert.Equal(t, "backup progress", progress.Event.Value)

			term, err := stream.Recv()
			if tc.wantErr != "" {
				// A failed backup must not send a terminal message.
				require.ErrorContains(t, err, tc.wantErr)
				assert.Nil(t, term)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, term.Status)
			assert.Equal(t, tc.outcome.Name, term.BackupName)
			assert.Equal(t, tc.outcome.Manifest, term.Manifest)
			assert.NotNil(t, term.Event)

			_, err = stream.Recv()
			assert.ErrorIs(t, err, io.EOF)
		})
	}
}

// setMaxMessageSize sets --grpc-max-message-size for the rest of the test.
func setMaxMessageSize(t *testing.T, size int) {
	fs := pflag.NewFlagSet(t.Name(), pflag.ContinueOnError)
	grpccommon.RegisterFlags(fs)
	orig := grpccommon.MaxMessageSize()
	require.NoError(t, fs.Set("grpc-max-message-size", strconv.Itoa(size)))
	t.Cleanup(func() { _ = fs.Set("grpc-max-message-size", strconv.Itoa(orig)) })
}

func TestBackupTerminalMessageTooLarge(t *testing.T) {
	var logBuf bytes.Buffer
	oldLogger := log.SwapLogger(slog.New(slog.NewTextHandler(&logBuf, nil)))
	defer log.SwapLogger(oldLogger)
	setMaxMessageSize(t, 1024)

	// servenv applies the same flag as the server's send limit.
	tablet := startServer(t, &backupOutcomeTM{
		RPCTM:   tmrpctest.NewFakeRPCTM(t),
		outcome: mysqlctl.BackupOutcome{Name: "b1", Manifest: strings.Repeat("x", 4096), Result: mysqlctl.BackupUsable},
	}, grpc.MaxSendMsgSize(grpccommon.MaxMessageSize()))
	stream, err := grpctmclient.NewClient().Backup(t.Context(), tablet, &tabletmanagerdatapb.BackupRequest{})
	require.NoError(t, err)

	_, err = stream.Recv()
	require.NoError(t, err)

	// The backup is stored, so it is reported without its manifest rather than
	// failing the RPC.
	term, err := stream.Recv()
	require.NoError(t, err)
	assert.Equal(t, tabletmanagerdatapb.BackupResponse_USABLE, term.Status)
	assert.Equal(t, "b1", term.BackupName)
	assert.Empty(t, term.Manifest)
	assert.NotNil(t, term.Event)

	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	assert.Contains(t, logBuf.String(), "exceeds --grpc-max-message-size")
}
