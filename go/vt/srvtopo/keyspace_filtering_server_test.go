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
	"reflect"
	"testing"

	"golang.org/x/net/context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

var (
	keyspaces = map[string]*topodatapb.SrvKeyspace{
		"foo": &topodatapb.SrvKeyspace{ShardingColumnName: "foo"},
		"bar": &topodatapb.SrvKeyspace{ShardingColumnName: "bar"},
		"baz": &topodatapb.SrvKeyspace{ShardingColumnName: "baz"},
	}
	stockCell            = "some-cell"
	stockCtx, _          = context.WithCancel(context.Background())
	stockFilters         = []string{"bar", "baz"}
	stockResilientServer Server
	stockTopoServer      *topo.Server
	stockVSchema         *vschemapb.SrvVSchema
)

func newFiltering(filter []string) (*topo.Server, Server, Server) {
	filtering, _ := NewKeyspaceFilteringServer(stockResilientServer, filter)
	return stockTopoServer, stockResilientServer, filtering
}

func init() {
	stockTopoServer = memorytopo.NewServer(stockCell)
	for ks, kp := range keyspaces {
		stockTopoServer.UpdateSrvKeyspace(context.Background(), stockCell, ks, kp)
	}

	stockVSchema = &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"foo": &vschemapb.Keyspace{Sharded: true},
			"bar": &vschemapb.Keyspace{Sharded: true},
			"baz": &vschemapb.Keyspace{Sharded: false},
		},
	}
	stockTopoServer.UpdateSrvVSchema(stockCtx, stockCell, stockVSchema)

	stockResilientServer = NewResilientServer(stockTopoServer, "srvtopo_resilient")
}

func TestFilteringServerHandlesNilUnderlying(t *testing.T) {
	got, gotErr := NewKeyspaceFilteringServer(nil, []string{})
	if got != nil {
		t.Errorf("got: %v, wanted: nil server", got)
	}
	if gotErr != NilUnderlyingServer {
		t.Errorf("Bad error returned: got %v wanted %v", gotErr, NilUnderlyingServer)
	}
}

func TestFilteringServerReturnsUnderlyingServer(t *testing.T) {
	want, _, f := newFiltering(nil)
	got := f.GetTopoServer()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Got incorrect topo.Server from FilteringServer: expected %p but got %p", want, got)
	}
}

func doTestGetSrvKeyspaceNames(
	t *testing.T,
	cell string,
	want []string,
	wantErr topo.ErrorCode,
) {
	_, _, f := newFiltering(stockFilters)
	got, gotErr := f.GetSrvKeyspaceNames(stockCtx, cell)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetSrvKeyspaceNames failed: want %v, got %v", want, got)
	}
	if wantErr == -1 && gotErr != nil || wantErr != -1 && !topo.IsErrType(gotErr, wantErr) {
		t.Errorf("GetSrvKeyspaceNames returned incorrect error: want %v, got %v", wantErr, gotErr)
	}

}

func TestFilteringServerGetSrvKeyspaceNamesFiltersKeyspaces(t *testing.T) {
	doTestGetSrvKeyspaceNames(t, stockCell, stockFilters, -1)
}

func TestFilteringServerGetSrvKeyspaceNamesPassesThroughErrors(t *testing.T) {
	doTestGetSrvKeyspaceNames(t, "badcell", []string{}, topo.NoNode)
}

func doTestGetSrvKeyspace(
	t *testing.T,
	cell,
	ksName string,
	want *topodatapb.SrvKeyspace,
	wantErr topo.ErrorCode,
) {
	_, _, f := newFiltering(stockFilters)
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

	if wantErr == -1 && gotErr != nil || wantErr != -1 && !topo.IsErrType(gotErr, wantErr) {
		t.Errorf("returned error incorrect: got %v, want %v", gotErr, wantErr)
	}

}
func TestFilteringServerGetSrvKeyspaceReturnsSelectedKeyspaces(t *testing.T) {
	doTestGetSrvKeyspace(t, stockCell, "bar", keyspaces["bar"], -1)
}

func TestFilteringServerGetSrvKeyspaceErrorPassthrough(t *testing.T) {
	doTestGetSrvKeyspace(t, "badcell", "bar", nil, topo.NoNode)
}

func TestFilteringServerGetSrvKeyspaceFilters(t *testing.T) {
	doTestGetSrvKeyspace(t, stockCell, "foo", nil, topo.NoNode)
}

func TestFilteringServerWatchSrvVSchemaFiltersPassthroughSrvVSchema(t *testing.T) {
	_, _, f := newFiltering(stockFilters)

	allowed := map[string]bool{}
	for _, ks := range stockFilters {
		allowed[ks] = true
	}

	// we need to verify that the nested callback actually gets called
	wasCalled := false
	cb := func(gotSchema *vschemapb.SrvVSchema, gotErr error) {
		wasCalled = true
		// verify that only selected keyspaces made it into the callback
		for ks := range gotSchema.Keyspaces {
			if !allowed[ks] {
				t.Errorf("Unexpected keyspace found in callback: %v", ks)
			}
		}
	}

	f.WatchSrvVSchema(stockCtx, stockCell, cb)

	if !wasCalled {
		t.Errorf("WatchSrvVSchema callback was not called as expected")
	}
}
