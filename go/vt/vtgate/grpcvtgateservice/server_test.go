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

package grpcvtgateservice

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type fakeVStreamServer struct {
	vtgateservicepb.Vitess_VStreamServer
	ctx context.Context
}

func (f *fakeVStreamServer) Context() context.Context {
	return f.ctx
}

func TestVStreamLogsError(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	grpcVTGate := &VTGate{server: &mockVTGateService{vstreamError: vterrors.New(vtrpcpb.Code_UNAVAILABLE, "no tablets")}}

	err := grpcVTGate.VStream(&vtgatepb.VStreamRequest{}, &fakeVStreamServer{ctx: t.Context()})
	require.ErrorContains(t, err, "no tablets")

	record := logs.Record(t, "VStream grpc error")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "no tablets", record["error"])
	assert.Equal(t, "UNAVAILABLE", record["error_code"])
}
