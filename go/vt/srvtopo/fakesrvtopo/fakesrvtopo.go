/*
Copyright 2025 The Vitess Authors.

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

package fakesrvtopo

import (
	"context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

type FakeSrvTopo struct {
	Ts                     *topo.Server
	SrvKeyspaceNamesOutput map[string][]string
	SrvKeyspaceNamesError  map[string]error
	SrvKeyspaceOutput      map[string]map[string]*topodatapb.SrvKeyspace
	SrvKeyspaceError       map[string]map[string]error
}

func (f *FakeSrvTopo) GetTopoServer() (*topo.Server, error) {
	return f.Ts, nil
}

func (f *FakeSrvTopo) GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error) {
	if f.SrvKeyspaceNamesError != nil && f.SrvKeyspaceNamesError[cell] != nil {
		return nil, f.SrvKeyspaceNamesError[cell]
	}
	if f.SrvKeyspaceNamesOutput == nil {
		return nil, nil
	}
	return f.SrvKeyspaceNamesOutput[cell], nil
}

func (f *FakeSrvTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	if f.SrvKeyspaceError != nil && f.SrvKeyspaceError[cell] != nil && f.SrvKeyspaceError[cell][keyspace] != nil {
		return nil, f.SrvKeyspaceError[cell][keyspace]
	}
	if f.SrvKeyspaceOutput == nil || f.SrvKeyspaceOutput[cell] == nil {
		return nil, nil
	}
	return f.SrvKeyspaceOutput[cell][keyspace], nil
}

func (f *FakeSrvTopo) WatchSrvKeyspace(ctx context.Context, cell, keyspace string, callback func(*topodatapb.SrvKeyspace, error) bool) {
	panic("unsupported in FakeSrvTopo")
}

func (f *FakeSrvTopo) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error) bool) {
	panic("unsupported in FakeSrvTopo")
}
