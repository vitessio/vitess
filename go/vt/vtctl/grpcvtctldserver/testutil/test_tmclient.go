/*
Copyright 2021 The Vitess Authors.

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

package testutil

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vttime"
)

// tabletManagerClient implements the tmclient.TabletManagerClient for
// testing. It allows users to mock various tmclient methods.
type tabletManagerClient struct {
	tmclient.TabletManagerClient
	Topo    *topo.Server
	Schemas map[string]*tabletmanagerdatapb.SchemaDefinition
}

// ChangeType is part of the tmclient.TabletManagerClient interface.
func (c *tabletManagerClient) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, newType topodatapb.TabletType) error {
	if c.Topo == nil {
		return assert.AnError
	}

	_, err := topotools.ChangeType(ctx, c.Topo, tablet.Alias, newType, &vttime.Time{})
	return err
}

// GetSchema is part of the tmclient.TabletManagerClient interface.
func (c *tabletManagerClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tablets []string, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	key := topoproto.TabletAliasString(tablet.Alias)

	schema, ok := c.Schemas[key]
	if !ok {
		return nil, fmt.Errorf("no schemas for %s", key)
	}

	return schema, nil
}

// SetMaster is part of the tmclient.TabletManagerClient interface.
func (c *tabletManagerClient) SetMaster(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) error {
	if c.Topo == nil {
		return assert.AnError
	}

	return nil
}

var (
	tmclientLock        sync.Mutex
	tmclientFactoryLock sync.Mutex
	tmclients           = map[string]tmclient.TabletManagerClient{}
	tmclientFactories   = map[string]func() tmclient.TabletManagerClient{}
)

// NewVtctldServerWithTabletManagerClient returns a new
// grpcvtctldserver.VtctldServer configured with the given topo server and
// tmclient.TabletManagerClient implementation for testing.
//
// It synchronizes on private locks to prevent multiple goroutines from stepping
// on each other during VtctldServer initialization, but still run the rest of
// the test in parallel.
//
// NOTE, THE FIRST: It is only safe to use in parallel with other tests using
// this method of creating a VtctldServer, or with tests that do not depend on a
// VtctldServer's tmclient.TabletManagerClient implementation.
//
// NOTE, THE SECOND: It needs to register a unique name to the tmclient factory
// registry, so we keep a shadow map of factories registered for "protocols" by
// this function. That way, if we happen to have multiple tests with the same
// name, we can swap out the return value for the factory and allow both tests
// to run, rather than the second test failing when it attempts to register a
// second factory for the same "protocol" name.
//
// NOTE, THE THIRD: we take a "new" func to produce a valid
// vtctlservicepb.VtctldServer implementation, rather than constructing directly
// ourselves with grpcvtctldserver.NewVtctldServer. This is to prevent an import
// cycle between this package and package grpcvtctldserver. Further, because the
// return type of NewVtctldServer is the struct type
// (*grpcvtctldserver.VtctldServer) and not the interface type
// vtctlservicepb.VtctldServer, tests will need to indirect that call through an
// extra layer rather than passing the function identifier directly, e.g.:
//
//		vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &testutil.TabletManagerClient{
//			...
//		}, func(ts *topo.Server) vtctlservicepb.VtctldServer { return NewVtctldServer(ts) })
//
func NewVtctldServerWithTabletManagerClient(t *testing.T, ts *topo.Server, tmc tmclient.TabletManagerClient, newVtctldServerFn func(ts *topo.Server) vtctlservicepb.VtctldServer) vtctlservicepb.VtctldServer {
	tmclientFactoryLock.Lock()
	defer tmclientFactoryLock.Unlock()

	protocol := t.Name()

	if _, alreadyRegisteredFactory := tmclientFactories[protocol]; !alreadyRegisteredFactory {
		factory := func() tmclient.TabletManagerClient {
			tmclientLock.Lock()
			defer tmclientLock.Unlock()

			client, ok := tmclients[protocol]
			if !ok {
				t.Fatal("Test managed to register a factory for a client value that never got set; this should be impossible")
			}

			return client
		}

		tmclient.RegisterTabletManagerClientFactory(protocol, factory)
		tmclientFactories[protocol] = factory
	}

	// Always swap in the new client return value for the given protocol name.
	// We cannot defer the unlock here, because grpcvtctldserver.NewVtctldServer
	// eventually will call into the factory we registered above, and we will
	// deadlock ourselves.
	tmclientLock.Lock()
	tmclients[protocol] = tmc
	tmclientLock.Unlock()

	// Be (mostly, we can't help concurrent goroutines not using this function)
	// atomic with our mutation of the global TabletManagerProtocol pointer.
	oldProto := *tmclient.TabletManagerProtocol
	defer func() { *tmclient.TabletManagerProtocol = oldProto }()

	*tmclient.TabletManagerProtocol = protocol

	return newVtctldServerFn(ts)
}

// TabletManagerClientProtocol is the protocol this package registers its client
// test implementation under. Users should set *tmclient.TabletManagerProtocol
// to this value before use.
const TabletManagerClientProtocol = "grpcvtctldserver.testutil"

// TabletManagerClient is the singleton test client instance. It is public and
// singleton to allow tests to mutate and verify its state.
var TabletManagerClient = &tabletManagerClient{
	Schemas: map[string]*tabletmanagerdatapb.SchemaDefinition{},
}

func init() {
	tmclient.RegisterTabletManagerClientFactory(TabletManagerClientProtocol, func() tmclient.TabletManagerClient {
		return TabletManagerClient
	})
}
