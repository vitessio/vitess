/*
Copyright 2018 The Vitess Authors.

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
	"fmt"
	"reflect"
	"sync"
	"testing"

	"golang.org/x/net/context"

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
		"foo": &topodatapb.SrvKeyspace{ShardingColumnName: "foo"},
		"bar": &topodatapb.SrvKeyspace{ShardingColumnName: "bar"},
		"baz": &topodatapb.SrvKeyspace{ShardingColumnName: "baz"},
	}
	stockVSchema = &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"foo": &vschemapb.Keyspace{Sharded: true},
			"bar": &vschemapb.Keyspace{Sharded: true},
			"baz": &vschemapb.Keyspace{Sharded: false},
		},
	}
)

func newFiltering(filter []string) (*topo.Server, *srvtopotest.PassthroughSrvTopoServer, Server) {
	testServer := srvtopotest.NewPassthroughSrvTopoServer()

	testServer.TopoServer = memorytopo.NewServer(stockCell)
	testServer.SrvKeyspaceNames = []string{"foo", "bar", "baz"}
	testServer.SrvKeyspace = &topodatapb.SrvKeyspace{ShardingColumnName: "test-column"}
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
	_, _, f := newFiltering(nil)
	got, gotErr := f.GetTopoServer()
	if got != nil {
		t.Errorf("Got non-nil topo.Server from FilteringServer")
	}
	if gotErr != ErrTopoServerNotAvailable {
		t.Errorf("Unexpected error from GetTopoServer; wanted %v but got %v", ErrTopoServerNotAvailable, gotErr)
	}
}

func doTestGetSrvKeyspaceNames(
	t *testing.T,
	f Server,
	cell string,
	want []string,
	wantErr error,
) {
	got, gotErr := f.GetSrvKeyspaceNames(stockCtx, cell)

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

func TestFilteringServerGetSrvKeyspaceNamesFiltersKeyspaces(t *testing.T) {
	_, _, f := newFiltering(stockFilters)
	doTestGetSrvKeyspaceNames(t, f, stockCell, stockFilters, nil)
}

func TestFilteringServerGetSrvKeyspaceNamesPassesThroughErrors(t *testing.T) {
	_, mock, f := newFiltering(stockFilters)
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
	got, gotErr := f.GetSrvKeyspace(stockCtx, cell, ksName)

	gotColumnName := ""
	wantColumnName := ""
	if got != nil {
		gotColumnName = got.ShardingColumnName
	}
	if want != nil {
		wantColumnName = want.ShardingColumnName
	}

	// a different pointer comes back so compare the expected return by proxy
	// of a field we know the value of
	if gotColumnName != wantColumnName {
		t.Errorf("keyspace incorrect: got %v, want %v", got, want)
	}

	if wantErr != gotErr {
		t.Errorf("returned error incorrect: got %v, want %v", gotErr, wantErr)
	}
}

func TestFilteringServerGetSrvKeyspaceReturnsSelectedKeyspaces(t *testing.T) {
	_, mock, f := newFiltering(stockFilters)
	mock.SrvKeyspace = stockKeyspaces["bar"]
	doTestGetSrvKeyspace(t, f, stockCell, "bar", stockKeyspaces["bar"], nil)
}

func TestFilteringServerGetSrvKeyspaceErrorPassthrough(t *testing.T) {
	wantErr := fmt.Errorf("some error")
	_, mock, f := newFiltering(stockFilters)
	mock.SrvKeyspace = stockKeyspaces["bar"]
	mock.SrvKeyspaceError = wantErr
	doTestGetSrvKeyspace(t, f, "badcell", "bar", stockKeyspaces["bar"], wantErr)
}

func TestFilteringServerGetSrvKeyspaceFilters(t *testing.T) {
	wantErr := topo.NewError(topo.NoNode, "foo")
	_, mock, f := newFiltering(stockFilters)
	mock.SrvKeyspaceError = wantErr
	doTestGetSrvKeyspace(t, f, stockCell, "foo", nil, wantErr)
}

func TestFilteringServerWatchSrvVSchemaFiltersPassthroughSrvVSchema(t *testing.T) {
	_, mock, f := newFiltering(stockFilters)

	allowed := map[string]bool{}
	for _, ks := range stockFilters {
		allowed[ks] = true
	}

	// we need to verify that the nested callback actually gets called
	wg := sync.WaitGroup{}
	wg.Add(1)

	cb := func(gotSchema *vschemapb.SrvVSchema, gotErr error) {
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
	}

	f.WatchSrvVSchema(stockCtx, stockCell, cb)
	wg.Wait()
}

func TestFilteringServerWatchSrvVSchemaHandlesNilSchema(t *testing.T) {
	wantErr := fmt.Errorf("some err")
	_, mock, f := newFiltering(stockFilters)
	mock.WatchedSrvVSchema = nil
	mock.WatchedSrvVSchemaError = wantErr

	// we need to verify that the nested callback actually gets called
	wg := sync.WaitGroup{}
	wg.Add(1)

	cb := func(gotSchema *vschemapb.SrvVSchema, gotErr error) {
		if gotSchema != nil {
			t.Errorf("Expected nil gotSchema: got %#v", gotSchema)
		}
		if gotErr != wantErr {
			t.Errorf("Unexpected error: want %v got %v", wantErr, gotErr)
		}
		wg.Done()
	}

	f.WatchSrvVSchema(stockCtx, "other-cell", cb)
	wg.Wait()
}
