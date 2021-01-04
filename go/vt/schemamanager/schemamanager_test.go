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

package schemamanager

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/faketmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	// import the gRPC client implementation for tablet manager
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
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

	err := Run(ctx, controller, newFakeExecutor(t))
	if err != errControllerOpen {
		t.Fatalf("controller.Open fail, should get error: %v, but get error: %v",
			errControllerOpen, err)
	}
}

func TestSchemaManagerControllerReadFail(t *testing.T) {
	controller := newFakeController(
		[]string{"select * from test_db"}, false, true, false)
	ctx := context.Background()
	err := Run(ctx, controller, newFakeExecutor(t))
	if err != errControllerRead {
		t.Fatalf("controller.Read fail, should get error: %v, but get error: %v",
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

	err := Run(ctx, controller, newFakeExecutor(t))
	if err == nil || !strings.Contains(err.Error(), "failed to parse sql") {
		t.Fatalf("run schema change should fail due to executor.Validate fail, but got: %v", err)
	}
}

func TestSchemaManagerExecutorOpenFail(t *testing.T) {
	controller := newFakeController(
		[]string{"create table test_table (pk int);"}, false, false, false)
	controller.SetKeyspace("unknown_keyspace")
	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(t), newFakeTabletManagerClient())
	executor := NewTabletExecutor("TestSchemaManagerExecutorOpenFail", wr, testWaitReplicasTimeout)
	ctx := context.Background()

	err := Run(ctx, controller, executor)
	if err == nil || !strings.Contains(err.Error(), "unknown_keyspace") {
		t.Fatalf("run schema change should fail due to executor.Open fail, but got: %v", err)
	}
}

func TestSchemaManagerExecutorExecuteFail(t *testing.T) {
	controller := newFakeController(
		[]string{"create table test_table (pk int);"}, false, false, false)
	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(t), newFakeTabletManagerClient())
	executor := NewTabletExecutor("TestSchemaManagerExecutorExecuteFail", wr, testWaitReplicasTimeout)
	ctx := context.Background()

	err := Run(ctx, controller, executor)
	if err == nil || !strings.Contains(err.Error(), "unknown database: vt_test_keyspace") {
		t.Fatalf("run schema change should fail due to executor.Execute fail, but got: %v", err)
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

	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(t), fakeTmc)
	executor := NewTabletExecutor("TestSchemaManagerRun", wr, testWaitReplicasTimeout)

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
	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(t), fakeTmc)
	executor := NewTabletExecutor("TestSchemaManagerExecutorFail", wr, testWaitReplicasTimeout)

	ctx := context.Background()
	err := Run(ctx, controller, executor)

	if err == nil || !strings.Contains(err.Error(), "schema change failed") {
		t.Fatalf("schema change should fail, but got err: %v", err)
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
	if err == nil || !strings.Contains(err.Error(), "there is no data sourcer factory") {
		t.Fatalf("controller factory is not registered, GetControllerFactory should return an error, but got: %v", err)
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

func newFakeExecutor(t *testing.T) *TabletExecutor {
	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(t), newFakeTabletManagerClient())
	return NewTabletExecutor("newFakeExecutor", wr, testWaitReplicasTimeout)
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

// newFakeTopo returns a topo with:
// - a keyspace named 'test_keyspace'.
// - 3 shards named '1', '2', '3'.
// - A master tablet for each shard.
func newFakeTopo(t *testing.T) *topo.Server {
	ts := memorytopo.NewServer("test_cell")
	ctx := context.Background()
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	for i, shard := range []string{"0", "1", "2"} {
		if err := ts.CreateShard(ctx, "test_keyspace", shard); err != nil {
			t.Fatalf("CreateShard(%v) failed: %v", shard, err)
		}
		tablet := &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "test_cell",
				Uid:  uint32(i + 1),
			},
			Keyspace: "test_keyspace",
			Shard:    shard,
		}
		if err := ts.CreateTablet(ctx, tablet); err != nil {
			t.Fatalf("CreateTablet failed: %v", err)
		}
		if _, err := ts.UpdateShardFields(ctx, "test_keyspace", shard, func(si *topo.ShardInfo) error {
			si.Shard.MasterAlias = tablet.Alias
			return nil
		}); err != nil {
			t.Fatalf("UpdateShardFields failed: %v", err)
		}
	}
	if err := ts.CreateKeyspace(ctx, "unsharded_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	if err := ts.CreateShard(ctx, "unsharded_keyspace", "0"); err != nil {
		t.Fatalf("CreateShard(%v) failed: %v", "0", err)
	}
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test_cell",
			Uid:  uint32(4),
		},
		Keyspace: "test_keyspace",
		Shard:    "0",
	}
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}
	if _, err := ts.UpdateShardFields(ctx, "unsharded_keyspace", "0", func(si *topo.ShardInfo) error {
		si.Shard.MasterAlias = tablet.Alias
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}
	return ts
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
