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

package vtgate

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/binlogacl"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestBinlogDumpGTID_Disabled(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	original := enableBinlogDump.Get()
	t.Cleanup(func() { enableBinlogDump.Set(original) })
	enableBinlogDump.Set(false)

	ctx := callerid.NewContext(t.Context(),
		callerid.NewEffectiveCallerID("user", "", ""),
		&querypb.VTGateCallerID{Username: "user"})

	req := &vtgatepb.BinlogDumpGTIDRequest{
		Keyspace: KsTestSharded,
		Shard:    "-20",
	}
	err := vtg.BinlogDumpGTID(ctx, req, func(*vtgatepb.BinlogDumpResponse) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "binlog dump is disabled")
}

func TestBinlogDumpGTID_Unauthorized(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	original := enableBinlogDump.Get()
	t.Cleanup(func() { enableBinlogDump.Set(original) })
	enableBinlogDump.Set(true)

	originalUsers := binlogacl.AuthorizedBinlogUsers.Get()
	t.Cleanup(func() { binlogacl.AuthorizedBinlogUsers.Set(originalUsers) })
	binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("cdcuser"))

	ctx := callerid.NewContext(t.Context(),
		callerid.NewEffectiveCallerID("regularUser", "", ""),
		&querypb.VTGateCallerID{Username: "regularUser"})

	req := &vtgatepb.BinlogDumpGTIDRequest{
		Keyspace: KsTestSharded,
		Shard:    "-20",
	}
	err := vtg.BinlogDumpGTID(ctx, req, func(*vtgatepb.BinlogDumpResponse) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not authorized to perform binlog dump operations")
}

func TestBinlogDumpGTID_MissingKeyspaceShard(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	original := enableBinlogDump.Get()
	t.Cleanup(func() { enableBinlogDump.Set(original) })
	enableBinlogDump.Set(true)

	originalUsers := binlogacl.AuthorizedBinlogUsers.Get()
	t.Cleanup(func() { binlogacl.AuthorizedBinlogUsers.Set(originalUsers) })
	binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))

	ctx := callerid.NewContext(t.Context(),
		callerid.NewEffectiveCallerID("user", "", ""),
		&querypb.VTGateCallerID{Username: "user"})

	tests := []struct {
		name     string
		keyspace string
		shard    string
	}{
		{"missing both", "", ""},
		{"missing keyspace", "", "-20"},
		{"missing shard", KsTestSharded, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &vtgatepb.BinlogDumpGTIDRequest{
				Keyspace: tt.keyspace,
				Shard:    tt.shard,
			}
			err := vtg.BinlogDumpGTID(ctx, req, func(*vtgatepb.BinlogDumpResponse) error { return nil })
			require.Error(t, err)
			assert.Contains(t, err.Error(), "binlog dump requires keyspace and shard")
		})
	}
}

func TestBinlogDumpGTID_FilePositionWithoutAlias(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	original := enableBinlogDump.Get()
	t.Cleanup(func() { enableBinlogDump.Set(original) })
	enableBinlogDump.Set(true)

	originalUsers := binlogacl.AuthorizedBinlogUsers.Get()
	t.Cleanup(func() { binlogacl.AuthorizedBinlogUsers.Set(originalUsers) })
	binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))

	ctx := callerid.NewContext(t.Context(),
		callerid.NewEffectiveCallerID("user", "", ""),
		&querypb.VTGateCallerID{Username: "user"})

	t.Run("filename without alias", func(t *testing.T) {
		req := &vtgatepb.BinlogDumpGTIDRequest{
			Keyspace:       KsTestSharded,
			Shard:          "-20",
			BinlogFilename: "binlog.000003",
		}
		err := vtg.BinlogDumpGTID(ctx, req, func(*vtgatepb.BinlogDumpResponse) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tablet targeting")
	})

	t.Run("non-default position without alias", func(t *testing.T) {
		req := &vtgatepb.BinlogDumpGTIDRequest{
			Keyspace:       KsTestSharded,
			Shard:          "-20",
			BinlogPosition: 1234,
		}
		err := vtg.BinlogDumpGTID(ctx, req, func(*vtgatepb.BinlogDumpResponse) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tablet targeting")
	})

	t.Run("default position without alias is allowed", func(t *testing.T) {
		req := &vtgatepb.BinlogDumpGTIDRequest{
			Keyspace:       KsTestSharded,
			Shard:          "-20",
			BinlogPosition: 4,
		}
		err := vtg.BinlogDumpGTID(ctx, req, func(*vtgatepb.BinlogDumpResponse) error { return nil })
		require.NoError(t, err)
	})
}

func TestBinlogDumpGTID_SuccessViaGateway(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	original := enableBinlogDump.Get()
	t.Cleanup(func() { enableBinlogDump.Set(original) })
	enableBinlogDump.Set(true)

	originalUsers := binlogacl.AuthorizedBinlogUsers.Get()
	t.Cleanup(func() { binlogacl.AuthorizedBinlogUsers.Set(originalUsers) })
	binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))

	ctx := callerid.NewContext(t.Context(),
		callerid.NewEffectiveCallerID("user", "", ""),
		&querypb.VTGateCallerID{Username: "user"})

	sbc1.BinlogDumpResponses = []*binlogdatapb.BinlogDumpResponse{
		{Raw: []byte("packet1")},
		{Raw: []byte("packet2")},
	}
	sbc1.BinlogDumpError = nil

	req := &vtgatepb.BinlogDumpGTIDRequest{
		Keyspace: KsTestSharded,
		Shard:    "-20",
		GtidSet:  "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-100",
	}

	var received []*vtgatepb.BinlogDumpResponse
	err := vtg.BinlogDumpGTID(ctx, req, func(r *vtgatepb.BinlogDumpResponse) error {
		received = append(received, r)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, received, 2)
	assert.Equal(t, []byte("packet1"), received[0].Raw)
	assert.Equal(t, []byte("packet2"), received[1].Raw)
}

func TestBinlogDumpGTID_SuccessViaTabletAlias(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	original := enableBinlogDump.Get()
	t.Cleanup(func() { enableBinlogDump.Set(original) })
	enableBinlogDump.Set(true)

	originalUsers := binlogacl.AuthorizedBinlogUsers.Get()
	t.Cleanup(func() { binlogacl.AuthorizedBinlogUsers.Set(originalUsers) })
	binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))

	ctx := callerid.NewContext(t.Context(),
		callerid.NewEffectiveCallerID("user", "", ""),
		&querypb.VTGateCallerID{Username: "user"})

	tabletAlias := sbc1.Tablet().Alias
	sbc1.BinlogDumpResponses = []*binlogdatapb.BinlogDumpResponse{
		{Raw: []byte("aliased-packet")},
	}
	sbc1.BinlogDumpError = nil

	req := &vtgatepb.BinlogDumpGTIDRequest{
		Keyspace:       KsTestSharded,
		Shard:          "-20",
		TabletAlias:    tabletAlias,
		BinlogFilename: "binlog.000003",
		BinlogPosition: 1234,
		GtidSet:        "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-50",
	}

	var received []*vtgatepb.BinlogDumpResponse
	err := vtg.BinlogDumpGTID(ctx, req, func(r *vtgatepb.BinlogDumpResponse) error {
		received = append(received, r)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, received, 1)
	assert.Equal(t, []byte("aliased-packet"), received[0].Raw)
}

func TestBinlogDumpGTID_TabletError(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	original := enableBinlogDump.Get()
	t.Cleanup(func() { enableBinlogDump.Set(original) })
	enableBinlogDump.Set(true)

	originalUsers := binlogacl.AuthorizedBinlogUsers.Get()
	t.Cleanup(func() { binlogacl.AuthorizedBinlogUsers.Set(originalUsers) })
	binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))

	ctx := callerid.NewContext(t.Context(),
		callerid.NewEffectiveCallerID("user", "", ""),
		&querypb.VTGateCallerID{Username: "user"})

	sbc1.BinlogDumpError = errors.New("test binlog error")
	t.Cleanup(func() { sbc1.BinlogDumpError = nil })

	req := &vtgatepb.BinlogDumpGTIDRequest{
		Keyspace: KsTestSharded,
		Shard:    "-20",
	}
	err := vtg.BinlogDumpGTID(ctx, req, func(*vtgatepb.BinlogDumpResponse) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "test binlog error")
}

func TestBinlogDumpGTID_DefaultTabletType(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	original := enableBinlogDump.Get()
	t.Cleanup(func() { enableBinlogDump.Set(original) })
	enableBinlogDump.Set(true)

	originalUsers := binlogacl.AuthorizedBinlogUsers.Get()
	t.Cleanup(func() { binlogacl.AuthorizedBinlogUsers.Set(originalUsers) })
	binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))

	ctx := callerid.NewContext(t.Context(),
		callerid.NewEffectiveCallerID("user", "", ""),
		&querypb.VTGateCallerID{Username: "user"})

	sbc1.BinlogDumpResponses = []*binlogdatapb.BinlogDumpResponse{}
	sbc1.BinlogDumpError = nil

	// Don't set tablet type — should default to PRIMARY
	req := &vtgatepb.BinlogDumpGTIDRequest{
		Keyspace: KsTestSharded,
		Shard:    "-20",
	}
	err := vtg.BinlogDumpGTID(ctx, req, func(*vtgatepb.BinlogDumpResponse) error { return nil })
	require.NoError(t, err)
}

func TestBinlogDumpGTID_NonexistentTabletAlias(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	original := enableBinlogDump.Get()
	t.Cleanup(func() { enableBinlogDump.Set(original) })
	enableBinlogDump.Set(true)

	originalUsers := binlogacl.AuthorizedBinlogUsers.Get()
	t.Cleanup(func() { binlogacl.AuthorizedBinlogUsers.Set(originalUsers) })
	binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))

	ctx := callerid.NewContext(t.Context(),
		callerid.NewEffectiveCallerID("user", "", ""),
		&querypb.VTGateCallerID{Username: "user"})

	req := &vtgatepb.BinlogDumpGTIDRequest{
		Keyspace: KsTestSharded,
		Shard:    "-20",
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "aa",
			Uid:  9999999,
		},
	}
	err := vtg.BinlogDumpGTID(ctx, req, func(*vtgatepb.BinlogDumpResponse) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), topoproto.TabletAliasString(req.TabletAlias))
}
