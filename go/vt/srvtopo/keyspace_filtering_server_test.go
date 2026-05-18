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

package srvtopo

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/srvtopo/srvtopotest"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

var (
	stockCell      = "some-cell"
	stockCtx       = context.Background()
	stockFilters   = []string{"bar", "baz"}
	stockKeyspaces = map[string]*topodatapb.SrvKeyspace{
		"foo": {},
		"bar": {},
		"baz": {},
	}
	stockVSchema = &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"foo": {Sharded: true},
			"bar": {Sharded: true},
			"baz": {Sharded: false},
		},
	}
)

func newFiltering(ctx context.Context, filter []string) (*topo.Server, *srvtopotest.PassthroughSrvTopoServer, Server) {
	testServer := srvtopotest.NewPassthroughSrvTopoServer()

	testServer.TopoServer = memorytopo.NewServer(ctx, stockCell)
	testServer.SrvKeyspaceNames = []string{"foo", "bar", "baz"}
	testServer.SrvKeyspace = &topodatapb.SrvKeyspace{}
	testServer.WatchedSrvVSchema = stockVSchema

	filtering, _ := NewKeyspaceFilteringServer(testServer, filter)
	return testServer.TopoServer, testServer, filtering
}

func TestFilteringServerHandlesNilUnderlying(t *testing.T) {
	got, gotErr := NewKeyspaceFilteringServer(nil, []string{})
	assert.Nilf(t, got, "got: %v, wanted: nil server", got)
	assert.Equalf(t, ErrNilUnderlyingServer, gotErr, "Bad error returned: got %v wanted %v", gotErr, ErrNilUnderlyingServer)
}

func TestFilteringServerReturnsUnderlyingServer(t *testing.T) {
	ctx := t.Context()
	_, _, f := newFiltering(ctx, nil)
	got, gotErr := f.GetTopoServer()
	assert.NoError(t, gotErr, "Got error getting topo.Server from FilteringServer")

	readOnly, err := got.IsReadOnly()
	assert.Truef(t, err == nil && readOnly, "Got read-write topo.Server from FilteringServer -- must be read-only")
	gotErr = got.CreateCellsAlias(stockCtx, "should_fail", &topodatapb.CellsAlias{Cells: []string{stockCell}})
	assert.Error(t, gotErr, "Were able to perform a write against the topo.Server from a FilteringServer -- it must be read-only")
}

func doTestGetSrvKeyspaceNames(
	t *testing.T,
	f Server,
	cell string,
	want []string,
	wantErr error,
) {
	got, gotErr := f.GetSrvKeyspaceNames(stockCtx, cell, false)

	assert.NotNil(t, got, "GetSrvKeyspaceNames failed: should not return nil")
	assert.Equalf(t, want, got, "GetSrvKeyspaceNames failed: want %v, got %v", want, got)
	assert.Equalf(t, wantErr, gotErr, "GetSrvKeyspaceNames returned incorrect error: want %v, got %v", wantErr, gotErr)
}

func TestFilteringServerGetSrvKeyspameNamesFiltersEverythingOut(t *testing.T) {
	ctx := t.Context()
	_, _, f := newFiltering(ctx, nil)
	doTestGetSrvKeyspaceNames(t, f, stockCell, []string{}, nil)
}

func TestFilteringServerGetSrvKeyspaceNamesFiltersKeyspaces(t *testing.T) {
	ctx := t.Context()
	_, _, f := newFiltering(ctx, stockFilters)
	doTestGetSrvKeyspaceNames(t, f, stockCell, stockFilters, nil)
}

func TestFilteringServerGetSrvKeyspaceNamesPassesThroughErrors(t *testing.T) {
	ctx := t.Context()
	_, mock, f := newFiltering(ctx, stockFilters)
	wantErr := errors.New("some badcell error")
	mock.SrvKeyspaceNamesError = wantErr
	doTestGetSrvKeyspaceNames(t, f, "badcell", stockFilters, wantErr)
}

func doTestGetSrvKeyspace(
	t *testing.T,
	f Server,
	cell,
	ksName string,
	want *topodatapb.SrvKeyspace,
	wantErr error,
) {
	_, gotErr := f.GetSrvKeyspace(stockCtx, cell, ksName)

	assert.Equalf(t, wantErr, gotErr, "returned error incorrect: got %v, want %v", gotErr, wantErr)
}

func TestFilteringServerGetSrvKeyspaceReturnsSelectedKeyspaces(t *testing.T) {
	ctx := t.Context()
	_, mock, f := newFiltering(ctx, stockFilters)
	mock.SrvKeyspace = stockKeyspaces["bar"]
	doTestGetSrvKeyspace(t, f, stockCell, "bar", stockKeyspaces["bar"], nil)
}

func TestFilteringServerGetSrvKeyspaceErrorPassthrough(t *testing.T) {
	ctx := t.Context()
	wantErr := errors.New("some error")
	_, mock, f := newFiltering(ctx, stockFilters)
	mock.SrvKeyspace = stockKeyspaces["bar"]
	mock.SrvKeyspaceError = wantErr
	doTestGetSrvKeyspace(t, f, "badcell", "bar", stockKeyspaces["bar"], wantErr)
}

func TestFilteringServerGetSrvKeyspaceFilters(t *testing.T) {
	ctx := t.Context()
	wantErr := topo.NewError(topo.NoNode, "foo")
	_, mock, f := newFiltering(ctx, stockFilters)
	mock.SrvKeyspaceError = wantErr
	doTestGetSrvKeyspace(t, f, stockCell, "foo", nil, wantErr)
}

func TestFilteringServerWatchSrvVSchemaFiltersPassthroughSrvVSchema(t *testing.T) {
	ctx := t.Context()
	_, mock, f := newFiltering(ctx, stockFilters)

	allowed := map[string]bool{}
	for _, ks := range stockFilters {
		allowed[ks] = true
	}

	// we need to verify that the nested callback actually gets called
	wg := sync.WaitGroup{}
	wg.Add(1)

	cb := func(gotSchema *vschemapb.SrvVSchema, gotErr error) bool {
		// ensure that only selected keyspaces made it into the callback
		for name, ks := range gotSchema.Keyspaces {
			assert.Truef(t, allowed[name], "Unexpected keyspace found in callback: %v", ks)
			wantKS := mock.WatchedSrvVSchema.Keyspaces[name]
			assert.Equalf(t, wantKS, ks, "Expected keyspace to be passed through unmodified: want %#v got %#v", wantKS, ks)
		}
		wg.Done()
		return true
	}

	f.WatchSrvVSchema(stockCtx, stockCell, cb)
	wg.Wait()
}

func TestFilteringServerWatchSrvVSchemaHandlesNilSchema(t *testing.T) {
	ctx := t.Context()

	wantErr := errors.New("some err")
	_, mock, f := newFiltering(ctx, stockFilters)
	mock.WatchedSrvVSchema = nil
	mock.WatchedSrvVSchemaError = wantErr

	// we need to verify that the nested callback actually gets called
	wg := sync.WaitGroup{}
	wg.Add(1)

	cb := func(gotSchema *vschemapb.SrvVSchema, gotErr error) bool {
		assert.Nilf(t, gotSchema, "Expected nil gotSchema: got %#v", gotSchema)
		assert.Equalf(t, wantErr, gotErr, "Unexpected error: want %v got %v", wantErr, gotErr)
		wg.Done()
		return true
	}

	f.WatchSrvVSchema(stockCtx, "other-cell", cb)
	wg.Wait()
}
