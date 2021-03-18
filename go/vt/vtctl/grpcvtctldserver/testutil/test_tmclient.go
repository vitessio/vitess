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
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vttime"
)

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

// TabletManagerClient implements the tmclient.TabletManagerClient interface
// with mock delays and response values, for use in unit tests.
type TabletManagerClient struct {
	tmclient.TabletManagerClient
	// TopoServer is used for certain TabletManagerClient rpcs that update topo
	// information, e.g. ChangeType. To force an error result for those rpcs in
	// a test, set tmc.TopoServer = nil.
	TopoServer *topo.Server
	// keyed by tablet alias.
	DemoteMasterDelays map[string]time.Duration
	// keyed by tablet alias.
	DemoteMasterResults map[string]struct {
		Status *replicationdatapb.MasterStatus
		Error  error
	}
	// keyed by tablet alias.
	GetSchemaDelays map[string]time.Duration
	// keyed by tablet alias.
	GetSchemaResults map[string]struct {
		Schema *tabletmanagerdatapb.SchemaDefinition
		Error  error
	}
	// keyed by tablet alias.
	MasterPositionDelays map[string]time.Duration
	// keyed by tablet alias.
	MasterPositionResults map[string]struct {
		Position string
		Error    error
	}
	// keyed by tablet alias.
	PopulateReparentJournalDelays map[string]time.Duration
	// keyed by tablet alias
	PopulateReparentJournalResults map[string]error
	// keyed by tablet alias.
	PromoteReplicaDelays map[string]time.Duration
	// keyed by tablet alias. injects a sleep to the end of the function
	// regardless of parent context timeout or error result.
	PromoteReplicaPostDelays map[string]time.Duration
	// keyed by tablet alias.
	PromoteReplicaResults map[string]struct {
		Result string
		Error  error
	}
	ReplicationStatusDelays  map[string]time.Duration
	ReplicationStatusResults map[string]struct {
		Position *replicationdatapb.Status
		Error    error
	}
	// keyed by tablet alias.
	SetMasterDelays map[string]time.Duration
	// keyed by tablet alias.
	SetMasterResults map[string]error
	// keyed by tablet alias.
	SetReadWriteDelays map[string]time.Duration
	// keyed by tablet alias.
	SetReadWriteResults map[string]error
	// keyed by tablet alias.
	StopReplicationAndGetStatusDelays map[string]time.Duration
	// keyed by tablet alias.
	StopReplicationAndGetStatusResults map[string]struct {
		Status     *replicationdatapb.Status
		StopStatus *replicationdatapb.StopReplicationStatus
		Error      error
	}
	// keyed by tablet alias.
	UndoDemoteMasterDelays map[string]time.Duration
	// keyed by tablet alias
	UndoDemoteMasterResults map[string]error
	// tablet alias => duration
	VReplicationExecDelays map[string]time.Duration
	// tablet alias => query string => result
	VReplicationExecResults map[string]map[string]struct {
		Result *querypb.QueryResult
		Error  error
	}
	// keyed by tablet alias.
	WaitForPositionDelays map[string]time.Duration
	// keyed by tablet alias. injects a sleep to the end of the function
	// regardless of parent context timeout or error result.
	WaitForPositionPostDelays map[string]time.Duration
	// WaitForPosition(tablet *topodatapb.Tablet, position string) error, so we
	// key by tablet alias and then by position.
	WaitForPositionResults map[string]map[string]error
}

// ChangeType is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, newType topodatapb.TabletType) error {
	if fake.TopoServer == nil {
		return assert.AnError
	}

	_, err := topotools.ChangeType(ctx, fake.TopoServer, tablet.Alias, newType, &vttime.Time{})
	return err
}

// DemoteMaster is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) DemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.MasterStatus, error) {
	if fake.DemoteMasterResults == nil {
		return nil, assert.AnError
	}

	if tablet.Alias == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.DemoteMasterDelays != nil {
		if delay, ok := fake.DemoteMasterDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.DemoteMasterResults[key]; ok {
		return result.Status, result.Error
	}

	return nil, assert.AnError
}

// GetSchema is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tablets []string, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	if fake.GetSchemaResults == nil {
		return nil, assert.AnError
	}

	if tablet.Alias == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.GetSchemaDelays != nil {
		if delay, ok := fake.GetSchemaDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.GetSchemaResults[key]; ok {
		return result.Schema, result.Error
	}

	return nil, fmt.Errorf("%w: no schemas for %s", assert.AnError, key)
}

// MasterPosition is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) MasterPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	if fake.MasterPositionResults == nil {
		return "", assert.AnError
	}

	if tablet.Alias == nil {
		return "", assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.MasterPositionDelays != nil {
		if delay, ok := fake.MasterPositionDelays[key]; ok {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.MasterPositionResults[key]; ok {
		return result.Position, result.Error
	}

	return "", assert.AnError
}

// PopulateReparentJournal is part of the tmclient.TabletManagerClient
// interface.
func (fake *TabletManagerClient) PopulateReparentJournal(ctx context.Context, tablet *topodatapb.Tablet, timeCreatedNS int64, actionName string, primaryAlias *topodatapb.TabletAlias, pos string) error {
	if fake.PopulateReparentJournalResults == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.PopulateReparentJournalDelays != nil {
		if delay, ok := fake.PopulateReparentJournalDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}
	if result, ok := fake.PopulateReparentJournalResults[key]; ok {
		return result
	}

	return assert.AnError
}

// PromoteReplica is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) PromoteReplica(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	if fake.PromoteReplicaResults == nil {
		return "", assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	defer func() {
		if fake.PromoteReplicaPostDelays == nil {
			return
		}

		if delay, ok := fake.PromoteReplicaPostDelays[key]; ok {
			time.Sleep(delay)
		}
	}()

	if fake.PromoteReplicaDelays != nil {
		if delay, ok := fake.PromoteReplicaDelays[key]; ok {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.PromoteReplicaResults[key]; ok {
		return result.Result, result.Error
	}

	return "", assert.AnError
}

// ReplicationStatus is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) ReplicationStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	if fake.ReplicationStatusResults == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.ReplicationStatusDelays != nil {
		if delay, ok := fake.ReplicationStatusDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.ReplicationStatusResults[key]; ok {
		return result.Position, result.Error
	}

	return nil, assert.AnError
}

// SetMaster is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) SetMaster(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) error {
	if fake.SetMasterResults == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.SetMasterDelays != nil {
		if delay, ok := fake.SetMasterDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.SetMasterResults[key]; ok {
		return result
	}

	return assert.AnError
}

// SetReadWrite is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) SetReadWrite(ctx context.Context, tablet *topodatapb.Tablet) error {
	if fake.SetReadWriteResults == nil {
		return assert.AnError
	}

	if tablet.Alias == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.SetReadWriteDelays != nil {
		if delay, ok := fake.SetReadWriteDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if err, ok := fake.SetReadWriteResults[key]; ok {
		return err
	}

	return assert.AnError
}

// StopReplicationAndGetStatus is part of the tmclient.TabletManagerClient
// interface.
func (fake *TabletManagerClient) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet, mode replicationdatapb.StopReplicationMode) (*replicationdatapb.Status, *replicationdatapb.StopReplicationStatus, error) {
	if fake.StopReplicationAndGetStatusResults == nil {
		return nil, nil, assert.AnError
	}

	if tablet.Alias == nil {
		return nil, nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.StopReplicationAndGetStatusDelays != nil {
		if delay, ok := fake.StopReplicationAndGetStatusDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.StopReplicationAndGetStatusResults[key]; ok {
		return result.Status, result.StopStatus, result.Error
	}

	return nil, nil, assert.AnError
}

// WaitForPosition is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) WaitForPosition(ctx context.Context, tablet *topodatapb.Tablet, position string) error {
	tabletKey := topoproto.TabletAliasString(tablet.Alias)

	defer func() {
		if fake.WaitForPositionPostDelays == nil {
			return
		}

		if delay, ok := fake.WaitForPositionPostDelays[tabletKey]; ok {
			time.Sleep(delay)
		}
	}()

	if fake.WaitForPositionDelays != nil {
		if delay, ok := fake.WaitForPositionDelays[tabletKey]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if fake.WaitForPositionResults == nil {
		return assert.AnError
	}

	tabletResultsByPosition, ok := fake.WaitForPositionResults[tabletKey]
	if !ok {
		return assert.AnError
	}

	result, ok := tabletResultsByPosition[position]
	if !ok {
		return assert.AnError
	}

	return result
}

// UndoDemoteMaster is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) UndoDemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) error {
	if fake.UndoDemoteMasterResults == nil {
		return assert.AnError
	}

	if tablet.Alias == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.UndoDemoteMasterDelays != nil {
		if delay, ok := fake.UndoDemoteMasterDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.UndoDemoteMasterResults[key]; ok {
		return result
	}

	return assert.AnError
}

// VReplicationExec is part of the tmclient.TabletManagerCLient interface.
func (fake *TabletManagerClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	if fake.VReplicationExecResults == nil {
		return nil, assert.AnError
	}

	if tablet.Alias == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.VReplicationExecDelays != nil {
		if delay, ok := fake.VReplicationExecDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if resultsForTablet, ok := fake.VReplicationExecResults[key]; ok {
		// Round trip the expected query both to ensure it's valid and to
		// standardize on capitalization and formatting.
		stmt, err := sqlparser.Parse(query)
		if err != nil {
			return nil, err
		}

		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("%v", stmt)

		parsedQuery := buf.ParsedQuery().Query

		// Now do the map lookup.
		if result, ok := resultsForTablet[parsedQuery]; ok {
			return result.Result, result.Error
		}
	}

	return nil, assert.AnError
}
