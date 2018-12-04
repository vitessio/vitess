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
	"testing"

	"golang.org/x/net/context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

type testServer struct {
	t             *testing.T
	underlying    *topo.Server
	keyspaceNames *keyspaceNamesMock
	getKeyspace   *getKeyspaceMock
	watch         *watchMock
}

type keyspaceNamesMock struct {
	want keyspaceNamesArgMock
	ret  keyspaceNamesRetMock
}

type keyspaceNamesArgMock struct {
	ctx  context.Context
	cell string
}

type keyspaceNamesRetMock struct {
	name []string
	err  error
}

type getKeyspaceArgs struct {
	ctx      context.Context
	cell     string
	keyspace string
}

type getKeyspaceRet struct {
	keyspace *topodatapb.SrvKeyspace
	err      error
}

type getKeyspaceMock struct {
	want getKeyspaceArgs
	ret  getKeyspaceRet
}

type watchMock struct {
	ctx       context.Context
	cell      string
	wrappedCB func(*vschemapb.SrvVSchema, error)
}

func (ts *testServer) GetTopoServer() *topo.Server { return ts.underlying }

func (ts *testServer) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	if ts.keyspaceNames == nil {
		ts.t.Errorf("Unexpected call to GetSrvKeyspaceNames")
	}

	if ts.keyspaceNames.want.ctx != ctx || ts.keyspaceNames.want.cell != cell {
		ts.t.Errorf("GetSrvKeyspaceNames incorrect parameters")
	}

	ret := ts.keyspaceNames.ret
	return ret.name, ret.err
}

func (ts *testServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	if ts.getKeyspace == nil {
		ts.t.Errorf("Unexpected call to GetSrvKeyspace")
	}

	want := ts.getKeyspace.want
	ret := ts.getKeyspace.ret

	if want.ctx != ctx {
		ts.t.Errorf("Incorrect context in GetSrvKeyspace: got %v, want %v", ctx, want.ctx)
	}
	if want.cell != cell {
		ts.t.Errorf("Incorrect cell in GetSrvKeyspace: got %v, want %v", cell, want.cell)
	}
	if want.keyspace != keyspace {
		ts.t.Errorf("Incorrect keyspace in GetServKeyspace: got %v, want %v", keyspace, want.keyspace)
	}

	return ret.keyspace, ret.err
}

func (ts *testServer) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error)) {
	if ts.watch == nil {
		ts.t.Errorf("Unexpected call to WatchSrvVSchema")
	}

	want := ts.watch

	if want.ctx != ctx {
		ts.t.Errorf("WatchSrvVSchema incorrect ctx: got %v, want %v", ctx, want.ctx)
	}
	if want.cell != cell {
		ts.t.Errorf("WatchSrvVSchema incorrect cell: got %v, want %v", cell, want.cell)
	}

	ts.watch.wrappedCB = callback
}

func newFiltering(underlying *testServer, filter []string) Server {
	return NewKeyspaceFilteringServer(underlying, filter)
}

var (
	stockTopoServer = &topo.Server{}
	stockFilters    = []string{"bar", "baz"}
	stockCell       = "some-cell"
	stockCtx, _     = context.WithCancel(context.Background())
)

func TestFilteringServerReturnsUnderlyingServer(t *testing.T) {
	f := newFiltering(&testServer{t: t, underlying: stockTopoServer}, nil)
	got := f.GetTopoServer()
	want := stockTopoServer
	if got != want {
		t.Errorf("Got incorrect topo.Server from FilteringServer: expected %p but got %p", want, got)
	}
}

func doTestGetSrvKeyspaceNames(t *testing.T, ret, want keyspaceNamesRetMock) {
	f := newFiltering(
		&testServer{
			t: t,
			keyspaceNames: &keyspaceNamesMock{
				keyspaceNamesArgMock{stockCtx, stockCell},
				ret,
			},
		},
		stockFilters,
	)

	got, gotErr := f.GetSrvKeyspaceNames(stockCtx, stockCell)
	if !reflect.DeepEqual(got, want.name) {
		t.Errorf("GetSrvKeyspaceNames failed: want %v, got %v", want.name, got)
	}
	if gotErr != want.err {
		t.Errorf("GetSrvKeyspaceNames returned incorrect error: want %v, got %v", want.err, gotErr)
	}

}

func TestFilteringServerGetSrvKeyspaceNamesFiltersKeyspaces(t *testing.T) {
	doTestGetSrvKeyspaceNames(
		t,
		keyspaceNamesRetMock{[]string{stockFilters[0], "foo", stockFilters[1]}, nil},
		keyspaceNamesRetMock{stockFilters, nil},
	)
}

func TestFilteringServerGetSrvKeyspaceNamesPassesThroughErrors(t *testing.T) {
	want := []string{"bar", "baz"}
	wantErr := fmt.Errorf("an error to pass back")
	doTestGetSrvKeyspaceNames(
		t,
		keyspaceNamesRetMock{want, wantErr},
		keyspaceNamesRetMock{want, wantErr},
	)
}

func doTestGetSrvKeyspace(t *testing.T, ksName string, upstreamReturns, want getKeyspaceRet) {
	f := newFiltering(&testServer{
		t: t,
		getKeyspace: &getKeyspaceMock{
			getKeyspaceArgs{stockCtx, stockCell, ksName},
			upstreamReturns,
		},
	}, stockFilters)

	got, gotErr := f.GetSrvKeyspace(stockCtx, stockCell, ksName)
	if got != want.keyspace {
		t.Errorf("keyspace incorrect: got %v, want %v", got, want.keyspace)
	}

	if gotErr != want.err {
		t.Errorf("returned error incorrect: got %v, want %v", gotErr, want.err)
	}

}
func TestFilteringServerGetSrvKeyspaceReturnsSelectedKeyspaces(t *testing.T) {
	want := &topodatapb.SrvKeyspace{}
	var wantErr error
	ret := getKeyspaceRet{want, wantErr}
	doTestGetSrvKeyspace(t, "bar", ret, ret)
}

func TestFilteringServerGetSrvKeyspaceErrorPassthrough(t *testing.T) {
	var want *topodatapb.SrvKeyspace
	wantErr := fmt.Errorf("an error to pass through")
	ret := getKeyspaceRet{want, wantErr}
	doTestGetSrvKeyspace(t, "bar", ret, ret)
}

func TestFilteringServerGetSrvKeyspaceFilters(t *testing.T) {
	ret := &topodatapb.SrvKeyspace{}
	wantErr := topo.NewError(topo.NoNode, "foo")
	doTestGetSrvKeyspace(
		t,
		"foo",
		getKeyspaceRet{ret, nil},
		getKeyspaceRet{nil, wantErr},
	)
}

func TestFilteringServerWatchSrvVSchemaFiltersPassthroughSrvVSchema(t *testing.T) {
	ts := &testServer{
		t:     t,
		watch: &watchMock{ctx: stockCtx, cell: stockCell},
	}
	f := newFiltering(ts, stockFilters)

	start := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"foo": &vschemapb.Keyspace{},
			"bar": &vschemapb.Keyspace{},
			"baz": &vschemapb.Keyspace{},
		},
		XXX_sizecache: 434343,
	}

	// want is a separate instance than start because the filtering deletes map
	// entries which could mask errors if too many are removed
	want := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"bar": start.Keyspaces["bar"],
			"baz": start.Keyspaces["baz"],
		},
		XXX_sizecache: 434343,
	}

	wantErr := fmt.Errorf("passthorugh error")

	// we need to verify that the nested callback actually gets called
	wasCalled := false
	cb := func(gotSchema *vschemapb.SrvVSchema, gotErr error) {
		wasCalled = true
		// this verifies that the schema we've been watching for gets filtered
		// as expected
		if !reflect.DeepEqual(gotSchema, want) {
			t.Errorf("WatchSrvVSchema callback got bad schema: want %v got %v", want, gotSchema)
		}
		// and this assures us any errors get passed through
		if gotErr != wantErr {
			t.Errorf("WatchSrvVSchema callback got bad error: want %v got %v", wantErr, gotErr)
		}
	}

	f.WatchSrvVSchema(stockCtx, stockCell, cb)

	ts.watch.wrappedCB(start, wantErr)
	if !wasCalled {
		t.Errorf("WatchSrvVSchema callback was not called as expected")
	}
}
