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
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/faketmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"

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
	tmclienttest.SetProtocol("go.vt.schemamanager", "grpc")
}

func TestSchemaManagerControllerOpenFail(t *testing.T) {
	controller := newFakeController(
		[]string{"select * from test_db"}, true, false, false)
	ctx := context.Background()

	_, err := Run(ctx, controller, newFakeExecutor(t))
	require.ErrorIs(t, err, errControllerOpen)
}

func TestSchemaManagerControllerReadFail(t *testing.T) {
	controller := newFakeController(
		[]string{"select * from test_db"}, false, true, false)
	ctx := context.Background()
	_, err := Run(ctx, controller, newFakeExecutor(t))
	require.ErrorIs(t, err, errControllerRead)
	require.True(t, controller.onReadFailTriggered, "OnReadFail should be called")
}

func TestSchemaManagerValidationFail(t *testing.T) {
	controller := newFakeController(
		[]string{"invalid sql"}, false, false, false)
	ctx := context.Background()

	_, err := Run(ctx, controller, newFakeExecutor(t))
	require.ErrorContains(t, err, "failed to parse sql", "run schema change should fail due to executor.Validate fail")
}

func TestSchemaManagerExecutorOpenFail(t *testing.T) {
	controller := newFakeController(
		[]string{"create table test_table (pk int);"}, false, false, false)
	controller.SetKeyspace("unknown_keyspace")
	executor := NewTabletExecutor("TestSchemaManagerExecutorOpenFail", newFakeTopo(t), newFakeTabletManagerClient(), logutil.NewConsoleLogger(), testWaitReplicasTimeout, 0, sqlparser.NewTestParser())
	ctx := context.Background()

	_, err := Run(ctx, controller, executor)
	require.ErrorContains(t, err, "unknown_keyspace", "run schema change should fail due to executor.Open fail")
}

func TestSchemaManagerRun(t *testing.T) {
	for _, batchSize := range []int{0, 1, 10} {
		t.Run(fmt.Sprintf("batch-size=%d", batchSize), func(t *testing.T) {
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
			executor := NewTabletExecutor("TestSchemaManagerRun", newFakeTopo(t), fakeTmc, logutil.NewConsoleLogger(), testWaitReplicasTimeout, 0, sqlparser.NewTestParser())

			ctx := context.Background()
			resp, err := Run(ctx, controller, executor)

			require.Lenf(t, resp.UUIDs, 0, "response should contain an empty list of UUIDs")
			require.NoError(t, err)

			require.True(t, controller.onReadSuccessTriggered, "OnReadSuccess should be called")
			require.False(t, controller.onReadFailTriggered, "OnReadFail should not be called")
			require.True(t, controller.onValidationSuccessTriggered, "OnValidateSuccess should be called")
			require.False(t, controller.onValidationFailTriggered, "OnValidationFail should not be called")
			require.True(t, controller.onExecutorCompleteTriggered, "OnExecutorComplete should be called")
		})
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
	executor := NewTabletExecutor("TestSchemaManagerExecutorFail", newFakeTopo(t), fakeTmc, logutil.NewConsoleLogger(), testWaitReplicasTimeout, 0, sqlparser.NewTestParser())

	ctx := context.Background()
	resp, err := Run(ctx, controller, executor)
	require.Lenf(t, resp.UUIDs, 0, "response should contain an empty list of UUIDs")
	require.ErrorContains(t, err, "schema change failed", "schema change should fail")
}

func TestSchemaManagerExecutorBatchVsStrategyFail(t *testing.T) {
	sql := "create table test_table (pk int)"
	controller := newFakeController([]string{sql}, false, false, false)
	fakeTmc := newFakeTabletManagerClient()

	fakeTmc.AddSchemaDefinition("vt_test_keyspace", &tabletmanagerdatapb.SchemaDefinition{})
	fakeTmc.EnableExecuteFetchAsDbaError = true
	executor := NewTabletExecutor("TestSchemaManagerExecutorFail", newFakeTopo(t), fakeTmc, logutil.NewConsoleLogger(), testWaitReplicasTimeout, 10, sqlparser.NewTestParser())
	executor.SetDDLStrategy("online")

	ctx := context.Background()
	_, err := Run(ctx, controller, executor)

	assert.ErrorContains(t, err, "--batch-size requires 'direct'")
}

func TestSchemaManagerExecutorBatchVsQueriesFail(t *testing.T) {
	sql := "alter table test_table force"
	controller := newFakeController([]string{sql}, false, false, false)
	fakeTmc := newFakeTabletManagerClient()

	fakeTmc.AddSchemaDefinition("vt_test_keyspace", &tabletmanagerdatapb.SchemaDefinition{})
	fakeTmc.EnableExecuteFetchAsDbaError = true
	executor := NewTabletExecutor("TestSchemaManagerExecutorFail", newFakeTopo(t), fakeTmc, logutil.NewConsoleLogger(), testWaitReplicasTimeout, 10, sqlparser.NewTestParser())
	executor.SetDDLStrategy("direct")

	ctx := context.Background()
	_, err := Run(ctx, controller, executor)

	assert.ErrorContains(t, err, "--batch-size only allowed when all queries are CREATE")
}

func TestSchemaManagerExecutorBatchVsUUIDsFail(t *testing.T) {
	sql := "create table test_table (pk int)"
	controller := newFakeController([]string{sql}, false, false, false)
	fakeTmc := newFakeTabletManagerClient()

	fakeTmc.AddSchemaDefinition("vt_test_keyspace", &tabletmanagerdatapb.SchemaDefinition{})
	fakeTmc.EnableExecuteFetchAsDbaError = true
	executor := NewTabletExecutor("TestSchemaManagerExecutorFail", newFakeTopo(t), fakeTmc, logutil.NewConsoleLogger(), testWaitReplicasTimeout, 10, sqlparser.NewTestParser())
	executor.SetDDLStrategy("direct")
	executor.SetUUIDList([]string{"4e5dcf80_354b_11eb_82cd_f875a4d24e90"})

	ctx := context.Background()
	_, err := Run(ctx, controller, executor)

	assert.ErrorContains(t, err, "--batch-size conflicts with --uuid-list")
}

func TestSchemaManagerRegisterControllerFactory(t *testing.T) {
	sql := "create table test_table (pk int)"
	RegisterControllerFactory(
		"test_controller",
		func(params map[string]string) (Controller, error) {
			return newFakeController([]string{sql}, false, false, false), nil
		})

	_, err := GetControllerFactory("unknown")
	require.ErrorContains(t, err, "there is no data sourcer factory", "controller factory is not registered, GetControllerFactory should return an error")

	_, err = GetControllerFactory("test_controller")
	require.NoError(t, err)

	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err, "RegisterControllerFactory should fail, it registers a registered ControllerFactory")
		}()
		RegisterControllerFactory(
			"test_controller",
			func(params map[string]string) (Controller, error) {
				return newFakeController([]string{sql}, false, false, false), nil

			})
	}()
}

func newFakeExecutor(t *testing.T) *TabletExecutor {
	return NewTabletExecutor("newFakeExecutor", newFakeTopo(t), newFakeTabletManagerClient(), logutil.NewConsoleLogger(), testWaitReplicasTimeout, 0, sqlparser.NewTestParser())
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

func (client *fakeTabletManagerClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	result, ok := client.schemaDefinitions[topoproto.TabletDbName(tablet)]
	if !ok {
		return nil, fmt.Errorf("unknown database: %s", topoproto.TabletDbName(tablet))
	}
	return result, nil
}

func (client *fakeTabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	if client.EnableExecuteFetchAsDbaError {
		return nil, fmt.Errorf("ExecuteFetchAsDba occur an unknown error")
	}
	return client.TabletManagerClient.ExecuteFetchAsDba(ctx, tablet, usePool, req)
}

// newFakeTopo returns a topo with:
// - a keyspace named 'test_keyspace'.
// - 3 shards named '1', '2', '3'.
// - A primary tablet for each shard.
func newFakeTopo(t *testing.T) *topo.Server {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "test_cell")
	err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{})
	require.NoError(t, err)

	for i, shard := range []string{"0", "1", "2"} {
		err = ts.CreateShard(ctx, "test_keyspace", shard)
		require.NoError(t, err)

		tablet := &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "test_cell",
				Uid:  uint32(i + 1),
			},
			Keyspace: "test_keyspace",
			Shard:    shard,
		}

		err = ts.CreateTablet(ctx, tablet)
		require.NoError(t, err)

		_, err = ts.UpdateShardFields(ctx, "test_keyspace", shard, func(si *topo.ShardInfo) error {
			si.Shard.PrimaryAlias = tablet.Alias
			return nil
		})
		require.NoError(t, err)
	}

	err = ts.CreateKeyspace(ctx, "unsharded_keyspace", &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.CreateShard(ctx, "unsharded_keyspace", "0")
	require.NoError(t, err)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test_cell",
			Uid:  uint32(4),
		},
		Keyspace: "test_keyspace",
		Shard:    "0",
	}
	err = ts.CreateTablet(ctx, tablet)
	require.NoError(t, err)

	_, err = ts.UpdateShardFields(ctx, "unsharded_keyspace", "0", func(si *topo.ShardInfo) error {
		si.Shard.PrimaryAlias = tablet.Alias
		return nil
	})
	require.NoError(t, err)
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
