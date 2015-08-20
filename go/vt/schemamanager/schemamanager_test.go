// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"errors"
	"fmt"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/faketmclient"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"

	// import the gRPC client implementation for tablet manager
	_ "github.com/youtube/vitess/go/vt/tabletmanager/grpctmclient"
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
	executor := NewTabletExecutor(
		newFakeTabletManagerClient(),
		newFakeTopo())
	ctx := context.Background()

	err := Run(ctx, controller, executor)
	if err == nil {
		t.Fatalf("run schema change should fail due to executor.Open fail")
	}
}

func TestSchemaManagerExecutorExecuteFail(t *testing.T) {
	controller := newFakeController(
		[]string{"create table test_table (pk int);"}, false, false, false)
	executor := NewTabletExecutor(
		newFakeTabletManagerClient(),
		newFakeTopo())
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
	fakeTmc.AddSchemaChange(sql, &proto.SchemaChangeResult{
		BeforeSchema: &proto.SchemaDefinition{},
		AfterSchema: &proto.SchemaDefinition{
			DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
			TableDefinitions: []*proto.TableDefinition{
				&proto.TableDefinition{
					Name:   "test_table",
					Schema: sql,
					Type:   proto.TableBaseTable,
				},
			},
		},
	})

	fakeTmc.AddSchemaDefinition("vt_test_keyspace", &proto.SchemaDefinition{})

	executor := NewTabletExecutor(
		fakeTmc,
		newFakeTopo())

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
	fakeTmc.AddSchemaChange(sql, &proto.SchemaChangeResult{
		BeforeSchema: &proto.SchemaDefinition{},
		AfterSchema: &proto.SchemaDefinition{
			DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
			TableDefinitions: []*proto.TableDefinition{
				&proto.TableDefinition{
					Name:   "test_table",
					Schema: sql,
					Type:   proto.TableBaseTable,
				},
			},
		},
	})

	fakeTmc.AddSchemaDefinition("vt_test_keyspace", &proto.SchemaDefinition{})
	fakeTmc.EnableExecuteFetchAsDbaError = true
	executor := NewTabletExecutor(fakeTmc, newFakeTopo())

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
	return NewTabletExecutor(
		newFakeTabletManagerClient(),
		newFakeTopo())
}

func newFakeTabletManagerClient() *fakeTabletManagerClient {
	return &fakeTabletManagerClient{
		TabletManagerClient: faketmclient.NewFakeTabletManagerClient(),
		preflightSchemas:    make(map[string]*proto.SchemaChangeResult),
		schemaDefinitions:   make(map[string]*proto.SchemaDefinition),
	}
}

type fakeTabletManagerClient struct {
	tmclient.TabletManagerClient
	EnableExecuteFetchAsDbaError bool
	preflightSchemas             map[string]*proto.SchemaChangeResult
	schemaDefinitions            map[string]*proto.SchemaDefinition
}

func (client *fakeTabletManagerClient) AddSchemaChange(
	sql string, schemaResult *proto.SchemaChangeResult) {
	client.preflightSchemas[sql] = schemaResult
}

func (client *fakeTabletManagerClient) AddSchemaDefinition(
	dbName string, schemaDefinition *proto.SchemaDefinition) {
	client.schemaDefinitions[dbName] = schemaDefinition
}

func (client *fakeTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topo.TabletInfo, change string) (*proto.SchemaChangeResult, error) {
	result, ok := client.preflightSchemas[change]
	if !ok {
		var scr proto.SchemaChangeResult
		return &scr, nil
	}
	return result, nil
}

func (client *fakeTabletManagerClient) GetSchema(ctx context.Context, tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool) (*proto.SchemaDefinition, error) {
	result, ok := client.schemaDefinitions[tablet.DbName()]
	if !ok {
		return nil, fmt.Errorf("unknown database: %s", tablet.DbName())
	}
	return result, nil
}

func (client *fakeTabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int, wantFields, disableBinlogs, reloadSchema bool) (*mproto.QueryResult, error) {
	if client.EnableExecuteFetchAsDbaError {
		var result mproto.QueryResult
		return &result, fmt.Errorf("ExecuteFetchAsDba occur an unknown error")
	}
	return client.TabletManagerClient.ExecuteFetchAsDba(ctx, tablet, query, maxRows, wantFields, disableBinlogs, reloadSchema)
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

func (topoServer *fakeTopo) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	if keyspace != "test_keyspace" {
		return nil, fmt.Errorf("expect to get keyspace: test_keyspace, but got: %s",
			keyspace)
	}
	return []string{"0", "1", "2"}, nil
}

func (topoServer *fakeTopo) GetShard(ctx context.Context, keyspace string, shard string) (*pb.Shard, int64, error) {
	var masterAlias *pb.TabletAlias
	if !topoServer.WithEmptyMasterAlias {
		masterAlias = &pb.TabletAlias{
			Cell: "test_cell",
			Uid:  0,
		}
	}
	value := &pb.Shard{
		MasterAlias: masterAlias,
	}
	return value, 0, nil
}

func (topoServer *fakeTopo) GetTablet(ctx context.Context, tabletAlias *pb.TabletAlias) (*pb.Tablet, int64, error) {
	return &pb.Tablet{
		Alias:    tabletAlias,
		Keyspace: "test_keyspace",
	}, 0, nil
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
