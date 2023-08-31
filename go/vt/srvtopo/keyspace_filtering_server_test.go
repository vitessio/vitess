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
	"fmt"
	"reflect"
	"sync"
	"testing"

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
	if got != nil {
		t.Errorf("got: %v, wanted: nil server", got)
	}
	if gotErr != ErrNilUnderlyingServer {
		t.Errorf("Bad error returned: got %v wanted %v", gotErr, ErrNilUnderlyingServer)
	}
}

func TestFilteringServerReturnsUnderlyingServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, f := newFiltering(ctx, nil)
	got, gotErr := f.GetTopoServer()
	if gotErr != nil {
		t.Errorf("Got error getting topo.Server from FilteringServer")
	}

	readOnly, err := got.IsReadOnly()
	if err != nil || !readOnly {
		t.Errorf("Got read-write topo.Server from FilteringServer -- must be read-only")
	}
	gotErr = got.CreateCellsAlias(stockCtx, "should_fail", &topodatapb.CellsAlias{Cells: []string{stockCell}})
	if gotErr == nil {
		t.Errorf("Were able to perform a write against the topo.Server from a FilteringServer -- it must be read-only")
	}
}

func doTestGetSrvKeyspaceNames(
	t *testing.T,
	f Server,
	cell string,
	want []string,
	wantErr error,
) {
	got, gotErr := f.GetSrvKeyspaceNames(stockCtx, cell, false)

	if got == nil {
		t.Errorf("GetSrvKeyspaceNames failed: should not return nil")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetSrvKeyspaceNames failed: want %v, got %v", want, got)
	}
	if wantErr != gotErr {
		t.Errorf("GetSrvKeyspaceNames returned incorrect error: want %v, got %v", wantErr, gotErr)
	}
}

func TestFilteringServerGetSrvKeyspameNamesFiltersEverythingOut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, f := newFiltering(ctx, nil)
	doTestGetSrvKeyspaceNames(t, f, stockCell, []string{}, nil)
}

func TestFilteringServerGetSrvKeyspaceNamesFiltersKeyspaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, f := newFiltering(ctx, stockFilters)
	doTestGetSrvKeyspaceNames(t, f, stockCell, stockFilters, nil)
}

func TestFilteringServerGetSrvKeyspaceNamesPassesThroughErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, mock, f := newFiltering(ctx, stockFilters)
	wantErr := fmt.Errorf("some badcell error")
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

	if wantErr != gotErr {
		t.Errorf("returned error incorrect: got %v, want %v", gotErr, wantErr)
	}
}

func TestFilteringServerGetSrvKeyspaceReturnsSelectedKeyspaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, mock, f := newFiltering(ctx, stockFilters)
	mock.SrvKeyspace = stockKeyspaces["bar"]
	doTestGetSrvKeyspace(t, f, stockCell, "bar", stockKeyspaces["bar"], nil)
}

func TestFilteringServerGetSrvKeyspaceErrorPassthrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wantErr := fmt.Errorf("some error")
	_, mock, f := newFiltering(ctx, stockFilters)
	mock.SrvKeyspace = stockKeyspaces["bar"]
	mock.SrvKeyspaceError = wantErr
	doTestGetSrvKeyspace(t, f, "badcell", "bar", stockKeyspaces["bar"], wantErr)
}

func TestFilteringServerGetSrvKeyspaceFilters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wantErr := topo.NewError(topo.NoNode, "foo")
	_, mock, f := newFiltering(ctx, stockFilters)
	mock.SrvKeyspaceError = wantErr
	doTestGetSrvKeyspace(t, f, stockCell, "foo", nil, wantErr)
}

func TestFilteringServerWatchSrvVSchemaFiltersPassthroughSrvVSchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
			if !allowed[name] {
				t.Errorf("Unexpected keyspace found in callback: %v", ks)
			}
			wantKS := mock.WatchedSrvVSchema.Keyspaces[name]
			if !reflect.DeepEqual(ks, wantKS) {
				t.Errorf(
					"Expected keyspace to be passed through unmodified: want %#v got %#v",
					wantKS,
					ks,
				)
			}
		}
		wg.Done()
		return true
	}

	f.WatchSrvVSchema(stockCtx, stockCell, cb)
	wg.Wait()
}

func TestFilteringServerWatchSrvVSchemaHandlesNilSchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wantErr := fmt.Errorf("some err")
	_, mock, f := newFiltering(ctx, stockFilters)
	mock.WatchedSrvVSchema = nil
	mock.WatchedSrvVSchemaError = wantErr

	// we need to verify that the nested callback actually gets called
	wg := sync.WaitGroup{}
	wg.Add(1)

	cb := func(gotSchema *vschemapb.SrvVSchema, gotErr error) bool {
		if gotSchema != nil {
			t.Errorf("Expected nil gotSchema: got %#v", gotSchema)
		}
		if gotErr != wantErr {
			t.Errorf("Unexpected error: want %v got %v", wantErr, gotErr)
		}
		wg.Done()
		return true
	}

	f.WatchSrvVSchema(stockCtx, "other-cell", cb)
	wg.Wait()
}
