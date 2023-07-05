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
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/timer"
	hk "vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/internal/grpcshim"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
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
//	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &testutil.TabletManagerClient{
//		...
//	}, func(ts *topo.Server) vtctlservicepb.VtctldServer { return NewVtctldServer(ts) })
func NewVtctldServerWithTabletManagerClient(t testing.TB, ts *topo.Server, tmc tmclient.TabletManagerClient, newVtctldServerFn func(ts *topo.Server) vtctlservicepb.VtctldServer) vtctlservicepb.VtctldServer {
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
	reset := setTMClientProtocol(protocol)
	defer reset()

	return newVtctldServerFn(ts)
}

const (
	fsName                   = "go.vt.vtctl.grpcvtctldserver.testutil"
	tmclientProtocolFlagName = "tablet_manager_protocol"
)

var fs *pflag.FlagSet

// N.B. we cannot use tmclienttest.SetProtocol because it trips the race
// detector because of how many grpcvtctldserver tests run in parallel.
func setTMClientProtocol(protocol string) (reset func()) {
	switch oldVal, err := fs.GetString(tmclientProtocolFlagName); err {
	case nil:
		reset = func() { setTMClientProtocol(oldVal) }
	default:
		log.Errorf("failed to get string value for flag %q: %v", tmclientProtocolFlagName, err)
		reset = func() {}
	}

	if err := fs.Set(tmclientProtocolFlagName, protocol); err != nil {
		msg := "failed to set flag %q to %q: %v"
		log.Errorf(msg, tmclientProtocolFlagName, protocol, err)
		reset = func() {}
	}

	return reset
}

func init() {
	var tmp []string
	tmp, os.Args = os.Args[:], []string{fsName}
	defer func() { os.Args = tmp }()

	// do this once at import-time before any tests run.
	servenv.OnParseFor(fsName, func(_fs *pflag.FlagSet) {
		fs = _fs
		if fs.Lookup(tmclientProtocolFlagName) != nil {
			return
		}

		tmclient.RegisterFlags(fs)
	})
	servenv.ParseFlags(fsName)
}

// TabletManagerClient implements the tmclient.TabletManagerClient interface
// with mock delays and response values, for use in unit tests.
type TabletManagerClient struct {
	tmclient.TabletManagerClient
	// TopoServer is used for certain TabletManagerClient rpcs that update topo
	// information, e.g. ChangeType. To force an error result for those rpcs in
	// a test, set tmc.TopoServer = nil.
	TopoServer *topo.Server
	Backups    map[string]struct {
		Events        []*logutilpb.Event
		EventInterval time.Duration
		EventJitter   time.Duration
		ErrorAfter    time.Duration
	}
	// keyed by tablet alias.
	ChangeTabletTypeResult map[string]error
	// keyed by tablet alias.
	DemotePrimaryDelays map[string]time.Duration
	// keyed by tablet alias.
	DemotePrimaryResults map[string]struct {
		Status *replicationdatapb.PrimaryStatus
		Error  error
	}
	// keyed by tablet alias.
	ExecuteFetchAsAppDelays map[string]time.Duration
	// keyed by tablet alias.
	ExecuteFetchAsAppResults map[string]struct {
		Response *querypb.QueryResult
		Error    error
	}
	// keyed by tablet alias.
	ExecuteFetchAsDbaDelays map[string]time.Duration
	// keyed by tablet alias.
	ExecuteFetchAsDbaResults map[string]struct {
		Response *querypb.QueryResult
		Error    error
	}
	// keyed by tablet alias.
	ExecuteHookDelays map[string]time.Duration
	// keyed by tablet alias.
	ExecuteHookResults map[string]struct {
		Response *hk.HookResult
		Error    error
	}
	// FullStatus result
	FullStatusResult *replicationdatapb.FullStatus
	// keyed by tablet alias.
	GetPermissionsDelays map[string]time.Duration
	// keyed by tablet alias.
	GetPermissionsResults map[string]struct {
		Permissions *tabletmanagerdatapb.Permissions
		Error       error
	}
	// keyed by tablet alias.
	GetReplicasResults map[string]struct {
		Replicas []string
		Error    error
	}
	// keyed by tablet alias.
	GetSchemaDelays map[string]time.Duration
	// keyed by tablet alias.
	GetSchemaResults map[string]struct {
		Schema *tabletmanagerdatapb.SchemaDefinition
		Error  error
	}
	// keyed by tablet alias.
	InitPrimaryDelays map[string]time.Duration
	// keyed by tablet alias. injects a sleep to the end of the function
	// regardless of parent context timeout or error result.
	InitPrimaryPostDelays map[string]time.Duration
	// keyed by tablet alias.
	InitPrimaryResults map[string]struct {
		Result string
		Error  error
	}
	// keyed by tablet alias.
	PrimaryPositionDelays map[string]time.Duration
	// keyed by tablet alias.
	PrimaryPositionResults map[string]struct {
		Position string
		Error    error
	}
	// keyed by tablet alias
	PingDelays map[string]time.Duration
	// keyed by tablet alias
	PingResults map[string]error
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
	// keyed by tablet alias.
	RefreshStateResults map[string]error
	// keyed by `<tablet_alias>/<wait_pos>`.
	ReloadSchemaDelays map[string]time.Duration
	// keyed by `<tablet_alias>/<wait_pos>`.
	ReloadSchemaResults      map[string]error
	ReplicationStatusDelays  map[string]time.Duration
	ReplicationStatusResults map[string]struct {
		Position *replicationdatapb.Status
		Error    error
	}
	RestoreFromBackupResults map[string]struct {
		Events        []*logutilpb.Event
		EventInterval time.Duration
		EventJitter   time.Duration
		ErrorAfter    time.Duration
	}
	// keyed by tablet alias
	RunHealthCheckDelays map[string]time.Duration
	// keyed by tablet alias
	RunHealthCheckResults map[string]error
	// keyed by tablet alias.
	SetReplicationSourceDelays map[string]time.Duration
	// keyed by tablet alias.
	SetReplicationSourceResults map[string]error
	// keyed by tablet alias.
	SetReplicationSourceSemiSync map[string]bool
	// keyed by tablet alias
	SetReadOnlyDelays map[string]time.Duration
	// keyed by tablet alias
	SetReadOnlyResults map[string]error
	// keyed by tablet alias.
	SetReadWriteDelays map[string]time.Duration
	// keyed by tablet alias.
	SetReadWriteResults map[string]error
	// keyed by tablet alias
	SleepDelays map[string]time.Duration
	// keyed by tablet alias
	SleepResults map[string]error
	// keyed by tablet alias
	StartReplicationDelays map[string]time.Duration
	// keyed by tablet alias
	StartReplicationResults map[string]error
	// keyed by tablet alias
	StopReplicationDelays map[string]time.Duration
	// keyed by tablet alias
	StopReplicationResults map[string]error
	// keyed by tablet alias.
	StopReplicationAndGetStatusDelays map[string]time.Duration
	// keyed by tablet alias.
	StopReplicationAndGetStatusResults map[string]struct {
		StopStatus *replicationdatapb.StopReplicationStatus
		Error      error
	}
	// keyed by tablet alias.
	UndoDemotePrimaryDelays map[string]time.Duration
	// keyed by tablet alias
	UndoDemotePrimaryResults map[string]error
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

type backupStreamAdapter struct {
	*grpcshim.BidiStream
	ch chan *logutilpb.Event
}

func (stream *backupStreamAdapter) Recv() (*logutilpb.Event, error) {
	select {
	case <-stream.Context().Done():
		return nil, stream.Context().Err()
	case err := <-stream.ErrCh:
		return nil, err
	case msg := <-stream.ch:
		return msg, nil
	case <-stream.Closed():
		return nil, stream.CloseErr()
	}
}

func (stream *backupStreamAdapter) Send(msg *logutilpb.Event) error {
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-stream.Closed():
		return grpcshim.ErrStreamClosed
	case stream.ch <- msg:
		return nil
	}
}

// Backup is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) Backup(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.BackupRequest) (logutil.EventStream, error) {
	if tablet.Type == topodatapb.TabletType_PRIMARY && !req.AllowPrimary {
		return nil, fmt.Errorf("cannot backup primary with allowPrimary=false")
	}

	key := topoproto.TabletAliasString(tablet.Alias)
	testdata, ok := fake.Backups[key]
	if !ok {
		return nil, fmt.Errorf("no Backup fake result set for %s", key)
	}

	stream := &backupStreamAdapter{
		BidiStream: grpcshim.NewBidiStream(ctx),
		ch:         make(chan *logutilpb.Event, len(testdata.Events)),
	}
	go func() {
		if testdata.EventInterval == 0 {
			testdata.EventInterval = 10 * time.Millisecond
			log.Warningf("testutil.TabletManagerClient.Backup faked with no event interval for %s, defaulting to %s", key, testdata.EventInterval)
		}

		if testdata.EventJitter == 0 {
			testdata.EventJitter = time.Millisecond
			log.Warningf("testutil.TabletManagerClient.Backup faked with no event jitter for %s, defaulting to %s", key, testdata.EventJitter)
		}

		errCtx, errCancel := context.WithCancel(context.Background())
		switch testdata.ErrorAfter {
		case 0:
			// no error to send, cancel the error context immediately
			errCancel()
		default:
			go func() {
				timer := time.NewTimer(testdata.ErrorAfter)
				defer func() { // Stop the timer and drain the channel.
					if !timer.Stop() {
						<-timer.C
					}
				}()
				defer errCancel()

				<-timer.C
				stream.ErrCh <- fmt.Errorf("error triggered after %s", testdata.ErrorAfter)
			}()
		}

		ticker := timer.NewRandTicker(testdata.EventInterval, testdata.EventJitter)

		defer ticker.Stop()
		defer stream.CloseWithError(nil)

		for _, event := range testdata.Events {
			stream.ch <- event
			<-ticker.C
		}

		// Wait for the error goroutine to finish. Note that if ErrorAfter
		// is zero, we never start the goroutine and cancel this context
		// immediately.
		//
		// The reason for this select is so that the error goroutine does
		// not attempt to send to stream.errCh after the call to CloseSend().
		<-errCtx.Done()
	}()

	return stream, nil
}

// ChangeType is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, newType topodatapb.TabletType, semiSync bool) error {
	if result, ok := fake.ChangeTabletTypeResult[topoproto.TabletAliasString(tablet.Alias)]; ok {
		return result
	}

	if fake.TopoServer == nil {
		return assert.AnError
	}

	_, err := topotools.ChangeType(ctx, fake.TopoServer, tablet.Alias, newType, &vttime.Time{})
	return err
}

// DemotePrimary is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) DemotePrimary(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	if fake.DemotePrimaryResults == nil {
		return nil, assert.AnError
	}

	if tablet.Alias == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.DemotePrimaryDelays != nil {
		if delay, ok := fake.DemotePrimaryDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.DemotePrimaryResults[key]; ok {
		return result.Status, result.Error
	}

	return nil, assert.AnError
}

// ExecuteFetchAsApp is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error) {
	if fake.ExecuteFetchAsAppResults == nil {
		return nil, fmt.Errorf("%w: no ExecuteFetchAsApp results on fake TabletManagerClient", assert.AnError)
	}

	key := topoproto.TabletAliasString(tablet.Alias)
	if fake.ExecuteFetchAsAppDelays != nil {
		if delay, ok := fake.ExecuteFetchAsAppDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}
	if result, ok := fake.ExecuteFetchAsAppResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no ExecuteFetchAsApp result set for tablet %s", assert.AnError, key)
}

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	if fake.ExecuteFetchAsDbaResults == nil {
		return nil, fmt.Errorf("%w: no ExecuteFetchAsDba results on fake TabletManagerClient", assert.AnError)
	}

	key := topoproto.TabletAliasString(tablet.Alias)
	if fake.ExecuteFetchAsDbaDelays != nil {
		if delay, ok := fake.ExecuteFetchAsDbaDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}
	if result, ok := fake.ExecuteFetchAsDbaResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no ExecuteFetchAsDba result set for tablet %s", assert.AnError, key)
}

// ExecuteHook is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) ExecuteHook(ctx context.Context, tablet *topodatapb.Tablet, hook *hk.Hook) (*hk.HookResult, error) {
	if fake.ExecuteHookResults == nil {
		return nil, fmt.Errorf("%w: no ExecuteHook results on fake TabletManagerClient", assert.AnError)
	}

	key := topoproto.TabletAliasString(tablet.Alias)
	if fake.ExecuteHookDelays != nil {
		if delay, ok := fake.ExecuteHookDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}
	if result, ok := fake.ExecuteHookResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no ExecuteHook result set for tablet %s", assert.AnError, key)
}

// FullStatus is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) FullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	if fake.FullStatusResult != nil {
		return fake.FullStatusResult, nil
	}

	if fake.TopoServer == nil {
		return nil, assert.AnError
	}

	return nil, fmt.Errorf("no output set for FullStatus")
}

// GetPermissions is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) GetPermissions(ctx context.Context, tablet *topodatapb.Tablet) (*tabletmanagerdatapb.Permissions, error) {
	if fake.GetPermissionsResults == nil {
		return nil, assert.AnError
	}

	if tablet.Alias == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.GetPermissionsDelays != nil {
		if delay, ok := fake.GetPermissionsDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.GetPermissionsResults[key]; ok {
		return result.Permissions, result.Error
	}

	return nil, fmt.Errorf("%w: no permissions for %s", assert.AnError, key)
}

// GetReplicas is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) GetReplicas(ctx context.Context, tablet *topodatapb.Tablet) ([]string, error) {
	if fake.GetReplicasResults == nil {
		return nil, fmt.Errorf("no results set on fake")
	}

	key := topoproto.TabletAliasString(tablet.Alias)
	if result, ok := fake.GetReplicasResults[key]; ok {
		return result.Replicas, result.Error
	}

	return nil, fmt.Errorf("no result set for %v", key)
}

// GetSchema is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
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

// InitPrimary is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) InitPrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) (string, error) {
	if fake.InitPrimaryResults == nil {
		return "", assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	defer func() {
		if fake.InitPrimaryPostDelays == nil {
			return
		}

		if delay, ok := fake.InitPrimaryPostDelays[key]; ok {
			time.Sleep(delay)
		}
	}()

	if fake.InitPrimaryDelays != nil {
		if delay, ok := fake.InitPrimaryDelays[key]; ok {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.InitPrimaryResults[key]; ok {
		return result.Result, result.Error
	}

	return "", assert.AnError
}

// PrimaryPosition is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) PrimaryPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	if fake.PrimaryPositionResults == nil {
		return "", assert.AnError
	}

	if tablet.Alias == nil {
		return "", assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.PrimaryPositionDelays != nil {
		if delay, ok := fake.PrimaryPositionDelays[key]; ok {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.PrimaryPositionResults[key]; ok {
		return result.Position, result.Error
	}

	return "", assert.AnError
}

// Ping is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) Ping(ctx context.Context, tablet *topodatapb.Tablet) error {
	if fake.PingResults == nil {
		return assert.AnError
	}

	if tablet.Alias == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.PingDelays != nil {
		if delay, ok := fake.PingDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if err, ok := fake.PingResults[key]; ok {
		return err
	}

	return fmt.Errorf("%w: no result for key %s", assert.AnError, key)
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
func (fake *TabletManagerClient) PromoteReplica(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) (string, error) {
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

// RefreshState is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	if fake.RefreshStateResults == nil {
		return fmt.Errorf("%w: no RefreshState results on fake TabletManagerClient", assert.AnError)
	}

	key := topoproto.TabletAliasString(tablet.Alias)
	if err, ok := fake.RefreshStateResults[key]; ok {
		return err
	}

	return fmt.Errorf("%w: no RefreshState result set for tablet %s", assert.AnError, key)
}

// ReloadSchema is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, waitPosition string) error {
	if fake.ReloadSchemaResults == nil {
		return fmt.Errorf("%w: no ReloadSchema results on fake TabletManagerClient", assert.AnError)
	}

	key := path.Join(topoproto.TabletAliasString(tablet.Alias), waitPosition)

	if fake.ReloadSchemaDelays != nil {
		if delay, ok := fake.ReloadSchemaDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if err, ok := fake.ReloadSchemaResults[key]; ok {
		return err
	}

	return fmt.Errorf("%w: no ReloadSchema result set for tablet %s", assert.AnError, key)
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

type backupRestoreStreamAdapter struct {
	*grpcshim.BidiStream
	ch chan *logutilpb.Event
}

func (stream *backupRestoreStreamAdapter) Recv() (*logutilpb.Event, error) {
	select {
	case <-stream.Context().Done():
		return nil, stream.Context().Err()
	case err := <-stream.ErrCh:
		return nil, err
	case msg := <-stream.ch:
		return msg, nil
	case <-stream.Closed():
		return nil, stream.CloseErr()
	}
}

func (stream *backupRestoreStreamAdapter) Send(msg *logutilpb.Event) error {
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-stream.Closed():
		return grpcshim.ErrStreamClosed
	case stream.ch <- msg:
		return nil
	}
}

// RestoreFromBackup is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) RestoreFromBackup(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.RestoreFromBackupRequest) (logutil.EventStream, error) {
	key := topoproto.TabletAliasString(tablet.Alias)
	testdata, ok := fake.RestoreFromBackupResults[key]
	if !ok {
		return nil, fmt.Errorf("no RestoreFromBackup fake result set for %s", key)
	}

	stream := &backupRestoreStreamAdapter{
		BidiStream: grpcshim.NewBidiStream(ctx),
		ch:         make(chan *logutilpb.Event, len(testdata.Events)),
	}
	go func() {
		if testdata.EventInterval == 0 {
			testdata.EventInterval = 10 * time.Millisecond
			log.Warningf("testutil.TabletManagerClient.RestoreFromBackup faked with no event interval for %s, defaulting to %s", key, testdata.EventInterval)
		}

		if testdata.EventJitter == 0 {
			testdata.EventJitter = time.Millisecond
			log.Warningf("testutil.TabletManagerClient.RestoreFromBackup faked with no event jitter for %s, defaulting to %s", key, testdata.EventJitter)
		}

		errCtx, errCancel := context.WithCancel(context.Background())
		switch testdata.ErrorAfter {
		case 0:
			// no error to send, cancel the error context immediately
			errCancel()
		default:
			go func() {
				timer := time.NewTimer(testdata.ErrorAfter)
				defer func() { // Stop the timer and drain the channel.
					if !timer.Stop() {
						<-timer.C
					}
				}()
				defer errCancel()

				<-timer.C
				stream.ErrCh <- fmt.Errorf("error triggered after %s", testdata.ErrorAfter)
			}()
		}

		ticker := timer.NewRandTicker(testdata.EventInterval, testdata.EventJitter)

		defer ticker.Stop()
		defer stream.CloseWithError(nil)

		for _, event := range testdata.Events {
			stream.ch <- event
			<-ticker.C
		}

		// Wait for the error goroutine to finish. Note that if ErrorAfter
		// is zero, we never start the goroutine and cancel this context
		// immediately.
		//
		// The reason for this select is so that the error goroutine does
		// not attempt to send to stream.errCh after the call to CloseSend().
		<-errCtx.Done()
	}()

	return stream, nil
}

// RunHealthCheck is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) RunHealthCheck(ctx context.Context, tablet *topodatapb.Tablet) error {
	if fake.RunHealthCheckResults == nil {
		return assert.AnError
	}

	if tablet.Alias == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.RunHealthCheckDelays != nil {
		if delay, ok := fake.RunHealthCheckDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if err, ok := fake.RunHealthCheckResults[key]; ok {
		return err
	}

	return fmt.Errorf("%w: no result for key %s", assert.AnError, key)
}

// SetReplicationSource is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) SetReplicationSource(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool, semiSync bool) error {
	if fake.SetReplicationSourceResults == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.SetReplicationSourceDelays != nil {
		if delay, ok := fake.SetReplicationSourceDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if fake.SetReplicationSourceSemiSync != nil {
		if semiSyncRequirement, ok := fake.SetReplicationSourceSemiSync[key]; ok {
			if semiSyncRequirement != semiSync {
				return fmt.Errorf("semi-sync settings incorrect")
			}
		}
	}

	if result, ok := fake.SetReplicationSourceResults[key]; ok {
		return result
	}

	return assert.AnError
}

// SetReadOnly is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) SetReadOnly(ctx context.Context, tablet *topodatapb.Tablet) error {
	if fake.SetReadOnlyResults == nil {
		return assert.AnError
	}

	if tablet.Alias == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.SetReadOnlyDelays != nil {
		if delay, ok := fake.SetReadOnlyDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if err, ok := fake.SetReadOnlyResults[key]; ok {
		return err
	}

	return fmt.Errorf("%w: no result for key %s", assert.AnError, key)
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

	return fmt.Errorf("%w: no result for key %s", assert.AnError, key)
}

// Sleep is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) Sleep(ctx context.Context, tablet *topodatapb.Tablet, duration time.Duration) error {
	if fake.SleepResults == nil {
		return assert.AnError
	}

	if tablet.Alias == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.SleepDelays != nil {
		if delay, ok := fake.SleepDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if err, ok := fake.SleepResults[key]; ok {
		time.Sleep(duration)
		return err
	}

	return fmt.Errorf("%w: no result for key %s", assert.AnError, key)
}

// StartReplication is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) StartReplication(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	if fake.StartReplicationResults == nil {
		return assert.AnError
	}

	if tablet.Alias == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.StartReplicationDelays != nil {
		if delay, ok := fake.StartReplicationDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if err, ok := fake.StartReplicationResults[key]; ok {
		return err
	}

	return fmt.Errorf("%w: no result for key %s", assert.AnError, key)
}

// StopReplication is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) StopReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	if fake.StopReplicationResults == nil {
		return assert.AnError
	}

	if tablet.Alias == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.StopReplicationDelays != nil {
		if delay, ok := fake.StopReplicationDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if err, ok := fake.StopReplicationResults[key]; ok {
		return err
	}

	return fmt.Errorf("%w: no result for key %s", assert.AnError, key)
}

// StopReplicationAndGetStatus is part of the tmclient.TabletManagerClient
// interface.
func (fake *TabletManagerClient) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet, mode replicationdatapb.StopReplicationMode) (*replicationdatapb.StopReplicationStatus, error) {
	if fake.StopReplicationAndGetStatusResults == nil {
		return nil, assert.AnError
	}

	if tablet.Alias == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.StopReplicationAndGetStatusDelays != nil {
		if delay, ok := fake.StopReplicationAndGetStatusDelays[key]; ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.StopReplicationAndGetStatusResults[key]; ok {
		return result.StopStatus, result.Error
	}

	return nil, assert.AnError
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

// UndoDemotePrimary is part of the tmclient.TabletManagerClient interface.
func (fake *TabletManagerClient) UndoDemotePrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	if fake.UndoDemotePrimaryResults == nil {
		return assert.AnError
	}

	if tablet.Alias == nil {
		return assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if fake.UndoDemotePrimaryDelays != nil {
		if delay, ok := fake.UndoDemotePrimaryDelays[key]; ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// proceed to results
			}
		}
	}

	if result, ok := fake.UndoDemotePrimaryResults[key]; ok {
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
