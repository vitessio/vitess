package grpcvtctldserver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestGetKeyspace(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := NewVtctldServer(ts)

	expected := &vtctldatapb.Keyspace{
		Name: "testkeyspace",
		Keyspace: &topodatapb.Keyspace{
			ShardingColumnName: "col1",
		},
	}

	in := *expected.Keyspace // take a copy to avoid the XXX_ fields changing

	err := ts.CreateKeyspace(ctx, expected.Name, &in)
	require.NoError(t, err)

	ks, err := vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: expected.Name})
	assert.NoError(t, err)
	assert.Equal(t, expected, ks)

	_, err = vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: "notfound"})
	assert.Error(t, err)
}

func TestGetKeyspaces(t *testing.T) {
	ctx := context.Background()
	ts, topofactory := memorytopo.NewServerAndFactory("cell1")
	vtctld := NewVtctldServer(ts)

	resp, err := vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Keyspaces)

	err = ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{})
	require.NoError(t, err)
	err = ts.CreateKeyspace(ctx, "ks2", &topodatapb.Keyspace{})
	require.NoError(t, err)
	err = ts.CreateKeyspace(ctx, "ks3", &topodatapb.Keyspace{})
	require.NoError(t, err)

	expected := []string{"ks1", "ks2", "ks3"}
	resp, err = vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp.Keyspaces)

	topofactory.SetError(errors.New("error from toposerver"))
	_, err = vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.Error(t, err)
}

type fakeShowAllKeyspacesStream struct {
	ctx       context.Context
	keyspaces []*vtctldatapb.Keyspace
	shouldErr bool
	grpc.ServerStream
}

func (fake *fakeShowAllKeyspacesStream) Context() context.Context {
	return fake.ctx
}

func (fake *fakeShowAllKeyspacesStream) Send(keyspace *vtctldatapb.Keyspace) error {
	if fake.shouldErr {
		return errors.New("error from stream.Send()")
	}

	fake.keyspaces = append(fake.keyspaces, keyspace)
	return nil
}

func TestShowAllKeyspaces(t *testing.T) {
	ctx := context.Background()
	ts, topofactory := memorytopo.NewServerAndFactory("cell1")
	vtctld := NewVtctldServer(ts)

	expected := []*vtctldatapb.Keyspace{
		{
			Name: "ks1",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "col1",
			},
		},
		{
			Name: "ks2",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "col2",
			},
		},
	}

	for _, ks := range expected {
		keyspace := *ks.Keyspace // value copy
		err := ts.CreateKeyspace(ctx, ks.Name, &keyspace)
		require.NoError(t, err)
	}

	stream := &fakeShowAllKeyspacesStream{ctx: ctx}
	err := vtctld.ShowAllKeyspaces(&vtctldatapb.ShowAllKeyspacesRequest{}, stream)
	assert.NoError(t, err)
	assert.Equal(t, stream.keyspaces, expected)

	stream = &fakeShowAllKeyspacesStream{ctx: ctx, shouldErr: true}
	err = vtctld.ShowAllKeyspaces(&vtctldatapb.ShowAllKeyspacesRequest{}, stream)
	assert.Error(t, err)

	topofactory.SetError(errors.New("error from toposerver"))
	stream = &fakeShowAllKeyspacesStream{ctx: ctx}
	err = vtctld.ShowAllKeyspaces(&vtctldatapb.ShowAllKeyspacesRequest{}, stream)
	assert.Error(t, err)
}
