/*
Copyright 2017 Google Inc.

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

package schemamanager

import (
	"errors"
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vttablet/faketmclient"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"

	// import the gRPC client implementation for tablet manager
	_ "github.com/youtube/vitess/go/vt/vttablet/grpctmclient"
)

var (
	errControllerOpen = errors.New("Open Fail")
	errControllerRead = errors.New("Read Fail")
)

func init() {
	// enforce we will use the right protocol (gRPC) (note the
	// client is unused, but it is initialized, so it needs to exist)
	*tmclient.TabletManagerProtocol = "grpc"
}

func TestSchemaManagerControllerOpenFail(t *testing.T) {
	controller := newFakeController(
		[]string{"select * from test_db"}, true, false, false)
	ctx := context.Background()

	err := Run(ctx, controller, newFakeExecutor())
	if err != errControllerOpen {
		t.Fatalf("controller.Open fail, shoud get error: %v, but get error: %v",
			errControllerOpen, err)
	}
}

func TestSchemaManagerControllerReadFail(t *testing.T) {
	controller := newFakeController(
		[]string{"select * from test_db"}, false, true, false)
	ctx := context.Background()
	err := Run(ctx, controller, newFakeExecutor())
	if err != errControllerRead {
		t.Fatalf("controller.Read fail, shoud get error: %v, but get error: %v",
			errControllerRead, err)
	}
	if !controller.onReadFailTriggered {
		t.Fatalf("OnReadFail should be called")
	}
}

func TestSchemaManagerValidationFail(t *testing.T) {
	controller := newFakeController(
		[]string{"invalid sql"}, false, false, false)
	ctx := context.Background()

	err := Run(ctx, controller, newFakeExecutor())
	if err == nil {
		t.Fatalf("run schema change should fail due to executor.Validate fail")
	}
}

func TestSchemaManagerExecutorOpenFail(t *testing.T) {
	controller := newFakeController(
		[]string{"create table test_table (pk int);"}, false, false, false)
	controller.SetKeyspace("unknown_keyspace")
	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(), newFakeTabletManagerClient())
	executor := NewTabletExecutor(wr, testWaitSlaveTimeout)
	ctx := context.Background()

	err := Run(ctx, controller, executor)
	if err == nil {
		t.Fatalf("run schema change should fail due to executor.Open fail")
	}
}

func TestSchemaManagerExecutorExecuteFail(t *testing.T) {
	controller := newFakeController(
		[]string{"create table test_table (pk int);"}, false, false, false)
	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(), newFakeTabletManagerClient())
	executor := NewTabletExecutor(wr, testWaitSlaveTimeout)
	ctx := context.Background()

	err := Run(ctx, controller, executor)
	if err == nil {
		t.Fatalf("run schema change should fail due to executor.Execute fail")
	}
}

func TestSchemaManagerRun(t *testing.T) {
	sql := "create table test_table (pk int)"
	controller := newFakeController(
		[]string{sql}, false, false, false)
	fakeTmc := newFakeTabletManagerClient()
	fakeTmc.AddSchemaChange(sql, &tabletmanagerdatapb.SchemaChangeResult{
		BeforeSchema: &tabletmanagerdatapb.SchemaDefinition{},
		AfterSchema: &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   "test_table",
					Schema: sql,
					Type:   tmutils.TableBaseTable,
				},
			},
		},
	})

	fakeTmc.AddSchemaDefinition("vt_test_keyspace", &tabletmanagerdatapb.SchemaDefinition{})

	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(), fakeTmc)
	executor := NewTabletExecutor(wr, testWaitSlaveTimeout)

	ctx := context.Background()
	err := Run(ctx, controller, executor)

	if err != nil {
		t.Fatalf("schema change should success but get error: %v", err)
	}
	if !controller.onReadSuccessTriggered {
		t.Fatalf("OnReadSuccess should be called")
	}
	if controller.onReadFailTriggered {
		t.Fatalf("OnReadFail should not be called")
	}
	if !controller.onValidationSuccessTriggered {
		t.Fatalf("OnValidateSuccess should be called")
	}
	if controller.onValidationFailTriggered {
		t.Fatalf("OnValidationFail should not be called")
	}
	if !controller.onExecutorCompleteTriggered {
		t.Fatalf("OnExecutorComplete should be called")
	}
}

func TestSchemaManagerExecutorFail(t *testing.T) {
	sql := "create table test_table (pk int)"
	controller := newFakeController([]string{sql}, false, false, false)
	fakeTmc := newFakeTabletManagerClient()
	fakeTmc.AddSchemaChange(sql, &tabletmanagerdatapb.SchemaChangeResult{
		BeforeSchema: &tabletmanagerdatapb.SchemaDefinition{},
		AfterSchema: &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   "test_table",
					Schema: sql,
					Type:   tmutils.TableBaseTable,
				},
			},
		},
	})

	fakeTmc.AddSchemaDefinition("vt_test_keyspace", &tabletmanagerdatapb.SchemaDefinition{})
	fakeTmc.EnableExecuteFetchAsDbaError = true
	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(), fakeTmc)
	executor := NewTabletExecutor(wr, testWaitSlaveTimeout)

	ctx := context.Background()
	err := Run(ctx, controller, executor)

	if err == nil {
		t.Fatalf("schema change should fail")
	}
}

func TestSchemaManagerRegisterControllerFactory(t *testing.T) {
	sql := "create table test_table (pk int)"
	RegisterControllerFactory(
		"test_controller",
		func(params map[string]string) (Controller, error) {
			return newFakeController([]string{sql}, false, false, false), nil

		})

	_, err := GetControllerFactory("unknown")
	if err == nil {
		t.Fatalf("controller factory is not registered, GetControllerFactory should return an error")
	}
	_, err = GetControllerFactory("test_controller")
	if err != nil {
		t.Fatalf("GetControllerFactory should succeed, but get an error: %v", err)
	}
	func() {
		defer func() {
			err := recover()
			if err == nil {
				t.Fatalf("RegisterControllerFactory should fail, it registers a registered ControllerFactory")
			}
		}()
		RegisterControllerFactory(
			"test_controller",
			func(params map[string]string) (Controller, error) {
				return newFakeController([]string{sql}, false, false, false), nil

			})
	}()
}

func newFakeExecutor() *TabletExecutor {
	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(), newFakeTabletManagerClient())
	return NewTabletExecutor(wr, testWaitSlaveTimeout)
}

func newFakeTabletManagerClient() *fakeTabletManagerClient {
	return &fakeTabletManagerClient{
		TabletManagerClient: faketmclient.NewFakeTabletManagerClient(),
		preflightSchemas:    make(map[string]*tabletmanagerdatapb.SchemaChangeResult),
		schemaDefinitions:   make(map[string]*tabletmanagerdatapb.SchemaDefinition),
	}
}

type fakeTabletManagerClient struct {
	tmclient.TabletManagerClient
	EnableExecuteFetchAsDbaError bool
	preflightSchemas             map[string]*tabletmanagerdatapb.SchemaChangeResult
	schemaDefinitions            map[string]*tabletmanagerdatapb.SchemaDefinition
}

func (client *fakeTabletManagerClient) AddSchemaChange(sql string, schemaResult *tabletmanagerdatapb.SchemaChangeResult) {
	client.preflightSchemas[sql] = schemaResult
}

func (client *fakeTabletManagerClient) AddSchemaDefinition(
	dbName string, schemaDefinition *tabletmanagerdatapb.SchemaDefinition) {
	client.schemaDefinitions[dbName] = schemaDefinition
}

func (client *fakeTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topodatapb.Tablet, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	var result []*tabletmanagerdatapb.SchemaChangeResult
	for _, change := range changes {
		scr, ok := client.preflightSchemas[change]
		if ok {
			result = append(result, scr)
		} else {
			result = append(result, &tabletmanagerdatapb.SchemaChangeResult{})
		}
	}
	return result, nil
}

func (client *fakeTabletManagerClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	result, ok := client.schemaDefinitions[topoproto.TabletDbName(tablet)]
	if !ok {
		return nil, fmt.Errorf("unknown database: %s", topoproto.TabletDbName(tablet))
	}
	return result, nil
}

func (client *fakeTabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, query []byte, maxRows int, disableBinlogs, reloadSchema bool) (*querypb.QueryResult, error) {
	if client.EnableExecuteFetchAsDbaError {
		return nil, fmt.Errorf("ExecuteFetchAsDba occur an unknown error")
	}
	return client.TabletManagerClient.ExecuteFetchAsDba(ctx, tablet, usePool, query, maxRows, disableBinlogs, reloadSchema)
}

type fakeTopo struct {
	faketopo.FakeTopo
	WithEmptyMasterAlias bool
}

func newFakeTopo() topo.Server {
	return topo.Server{
		Impl: &fakeTopo{},
	}
}

func (ts *fakeTopo) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	if keyspace != "test_keyspace" {
		return nil, fmt.Errorf("expect to get keyspace: test_keyspace, but got: %s",
			keyspace)
	}
	return []string{"0", "1", "2"}, nil
}

func (ts *fakeTopo) GetShard(ctx context.Context, keyspace string, shard string) (*topodatapb.Shard, int64, error) {
	var masterAlias *topodatapb.TabletAlias
	if !ts.WithEmptyMasterAlias {
		masterAlias = &topodatapb.TabletAlias{
			Cell: "test_cell",
			Uid:  0,
		}
	}
	value := &topodatapb.Shard{
		MasterAlias: masterAlias,
	}
	return value, 0, nil
}

func (ts *fakeTopo) GetTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) (*topodatapb.Tablet, int64, error) {
	return &topodatapb.Tablet{
		Alias:    tabletAlias,
		Keyspace: "test_keyspace",
	}, 0, nil
}

func (ts *fakeTopo) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	return "", nil
}

func (ts *fakeTopo) UnlockKeyspaceForAction(ctx context.Context, keyspace, lockPath, results string) error {
	return nil
}

type fakeController struct {
	sqls                         []string
	keyspace                     string
	openFail                     bool
	readFail                     bool
	closeFail                    bool
	onReadSuccessTriggered       bool
	onReadFailTriggered          bool
	onValidationSuccessTriggered bool
	onValidationFailTriggered    bool
	onExecutorCompleteTriggered  bool
}

func newFakeController(
	sqls []string, openFail bool, readFail bool, closeFail bool) *fakeController {
	return &fakeController{
		sqls:      sqls,
		keyspace:  "test_keyspace",
		openFail:  openFail,
		readFail:  readFail,
		closeFail: closeFail,
	}
}

func (controller *fakeController) SetKeyspace(keyspace string) {
	controller.keyspace = keyspace
}

func (controller *fakeController) Open(ctx context.Context) error {
	if controller.openFail {
		return errControllerOpen
	}
	return nil
}

func (controller *fakeController) Read(ctx context.Context) ([]string, error) {
	if controller.readFail {
		return nil, errControllerRead
	}
	return controller.sqls, nil
}

func (controller *fakeController) Close() {
}

func (controller *fakeController) Keyspace() string {
	return controller.keyspace
}

func (controller *fakeController) OnReadSuccess(ctx context.Context) error {
	controller.onReadSuccessTriggered = true
	return nil
}

func (controller *fakeController) OnReadFail(ctx context.Context, err error) error {
	controller.onReadFailTriggered = true
	return err
}

func (controller *fakeController) OnValidationSuccess(ctx context.Context) error {
	controller.onValidationSuccessTriggered = true
	return nil
}

func (controller *fakeController) OnValidationFail(ctx context.Context, err error) error {
	controller.onValidationFailTriggered = true
	return err
}

func (controller *fakeController) OnExecutorComplete(ctx context.Context, result *ExecuteResult) error {
	controller.onExecutorCompleteTriggered = true
	return nil
}

var _ Controller = (*fakeController)(nil)
