// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"errors"
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/faketmclient"
	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

var (
	errDataSourcerOpen  = errors.New("Open Fail")
	errDataSourcerRead  = errors.New("Read Fail")
	errDataSourcerClose = errors.New("Close Fail")
)

func TestRunSchemaChangesDataSourcerOpenFail(t *testing.T) {
	dataSourcer := newFakeDataSourcer([]string{"select * from test_db"}, true, false, false)
	handler := newFakeHandler()
	exec := newFakeExecutor()
	err := Run(dataSourcer, exec, handler)
	if err != errDataSourcerOpen {
		t.Fatalf("data sourcer open fail, shoud get error: %v, but get error: %v",
			errDataSourcerOpen, err)
	}
}

func TestRunSchemaChangesDataSourcerReadFail(t *testing.T) {
	dataSourcer := newFakeDataSourcer([]string{"select * from test_db"}, false, true, false)
	handler := newFakeHandler()
	exec := newFakeExecutor()
	err := Run(dataSourcer, exec, handler)
	if err != errDataSourcerRead {
		t.Fatalf("data sourcer read fail, shoud get error: %v, but get error: %v",
			errDataSourcerRead, err)
	}
	if !handler.onDataSourcerReadFailTriggered {
		t.Fatalf("event handler should call OnDataSourcerReadFail but it didn't")
	}
}

func TestRunSchemaChangesValidationFail(t *testing.T) {
	dataSourcer := newFakeDataSourcer([]string{"invalid sql"}, false, false, false)
	handler := newFakeHandler()
	exec := newFakeExecutor()
	err := Run(dataSourcer, exec, handler)
	if err == nil {
		t.Fatalf("run schema change should fail due to executor.Validate fail")
	}
}

func TestRunSchemaChangesExecutorOpenFail(t *testing.T) {
	dataSourcer := newFakeDataSourcer([]string{"create table test_table (pk int);"}, false, false, false)
	handler := newFakeHandler()
	exec := NewTabletExecutor(
		newFakeTabletManagerClient(),
		newFakeTopo(),
		"unknown_keyspace")
	err := Run(dataSourcer, exec, handler)
	if err == nil {
		t.Fatalf("run schema change should fail due to executor.Open fail")
	}
}

func TestRunSchemaChanges(t *testing.T) {
	sql := "create table test_table (pk int)"
	dataSourcer := NewSimpleDataSourcer(sql)
	handler := newFakeHandler()
	fakeTmc := newFakeTabletManagerClient()
	fakeTmc.AddSchemaChange(sql, &proto.SchemaChangeResult{
		BeforeSchema: &proto.SchemaDefinition{},
		AfterSchema: &proto.SchemaDefinition{
			DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
			TableDefinitions: []*proto.TableDefinition{
				&proto.TableDefinition{
					Name:   "test_table",
					Schema: sql,
					Type:   proto.TABLE_BASE_TABLE,
				},
			},
		},
	})

	exec := NewTabletExecutor(
		fakeTmc,
		newFakeTopo(),
		"test_keyspace")

	err := Run(dataSourcer, exec, handler)
	if err != nil {
		t.Fatalf("schema change should success but get error: %v", err)
	}
	if !handler.onDataSourcerReadSuccessTriggered {
		t.Fatalf("event handler should call OnDataSourcerReadSuccess but it didn't")
	}
	if handler.onDataSourcerReadFailTriggered {
		t.Fatalf("event handler should not call OnDataSourcerReadFail but it did")
	}
	if !handler.onValidationSuccessTriggered {
		t.Fatalf("event handler should call OnDataSourcerValidateSuccess but it didn't")
	}
	if handler.onValidationFailTriggered {
		t.Fatalf("event handler should not call OnValidationFail but it did")
	}
	if !handler.onExecutorCompleteTriggered {
		t.Fatalf("event handler should call OnExecutorComplete but it didn't")
	}
}

func newFakeExecutor() *TabletExecutor {
	return NewTabletExecutor(
		newFakeTabletManagerClient(),
		newFakeTopo(),
		"test_keyspace")
}

func newFakeTabletManagerClient() *fakeTabletManagerClient {
	return &fakeTabletManagerClient{
		TabletManagerClient: faketmclient.NewFakeTabletManagerClient(),
		preflightSchemas:    make(map[string]*proto.SchemaChangeResult),
	}
}

type fakeTabletManagerClient struct {
	tmclient.TabletManagerClient
	preflightSchemas map[string]*proto.SchemaChangeResult
}

func (client *fakeTabletManagerClient) AddSchemaChange(
	sql string, schemaResult *proto.SchemaChangeResult) {
	client.preflightSchemas[sql] = schemaResult
}

func (client *fakeTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topo.TabletInfo, change string) (*proto.SchemaChangeResult, error) {
	result, ok := client.preflightSchemas[change]
	if !ok {
		var scr proto.SchemaChangeResult
		return &scr, nil
	}
	return result, nil
}

type fakeTopo struct{}

func newFakeTopo() *fakeTopo {
	return &fakeTopo{}
}

func (topoServer *fakeTopo) GetShardNames(keyspace string) ([]string, error) {
	if keyspace != "test_keyspace" {
		return nil, fmt.Errorf("expect to get keyspace: test_keyspace, but got: %s",
			keyspace)
	}
	return []string{"0", "1", "2"}, nil
}

func (topoServer *fakeTopo) GetShard(keyspace string, shard string) (*topo.ShardInfo, error) {
	value := &topo.Shard{
		MasterAlias: topo.TabletAlias{
			Cell: "test_cell",
			Uid:  0,
		},
	}
	return topo.NewShardInfo(keyspace, shard, value, 0), nil
}

func (topoServer *fakeTopo) GetTablet(tabletAlias topo.TabletAlias) (*topo.TabletInfo, error) {
	return &topo.TabletInfo{
		Tablet: &topo.Tablet{
			Alias:    tabletAlias,
			Keyspace: "test_keyspace",
		},
	}, nil
}

func (topoServer *fakeTopo) GetSrvKeyspaceNames(cell string) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	return nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	return nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) Close() {}

func (topoServer *fakeTopo) GetKnownCells() ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) CreateKeyspace(keyspace string, value *topo.Keyspace) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UpdateKeyspace(ki *topo.KeyspaceInfo, existingVersion int64) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) GetKeyspace(keyspace string) (*topo.KeyspaceInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) GetKeyspaces() ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) DeleteKeyspaceShards(keyspace string) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) CreateShard(keyspace, shard string, value *topo.Shard) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UpdateShard(si *topo.ShardInfo, existingVersion int64) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) ValidateShard(keyspace, shard string) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) DeleteShard(keyspace, shard string) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) CreateTablet(tablet *topo.Tablet) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UpdateTablet(tablet *topo.TabletInfo, existingVersion int64) (newVersion int64, err error) {
	return 0, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UpdateTabletFields(tabletAlias topo.TabletAlias, update func(*topo.Tablet) error) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) DeleteTablet(alias topo.TabletAlias) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) GetTabletsByCell(cell string) ([]topo.TabletAlias, error) {
	return nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UpdateShardReplicationFields(cell, keyspace, shard string, update func(*topo.ShardReplication) error) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) GetShardReplication(cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) DeleteShardReplication(cell, keyspace, shard string) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) LockSrvShardForAction(ctx context.Context, cell, keyspace, shard, contents string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UnlockSrvShardForAction(cell, keyspace, shard, lockPath, results string) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) GetSrvTabletTypesPerShard(cell, keyspace, shard string) ([]topo.TabletType, error) {
	return nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UpdateEndPoints(cell, keyspace, shard string, tabletType topo.TabletType, addrs *topo.EndPoints) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) DeleteEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) WatchEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (<-chan *topo.EndPoints, chan<- struct{}, error) {
	return nil, nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UpdateSrvShard(cell, keyspace, shard string, srvShard *topo.SrvShard) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) GetSrvShard(cell, keyspace, shard string) (*topo.SrvShard, error) {
	return nil, fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) DeleteSrvShard(cell, keyspace, shard string) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UpdateSrvKeyspace(cell, keyspace string, srvKeyspace *topo.SrvKeyspace) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UpdateTabletEndpoint(cell, keyspace, shard string, tabletType topo.TabletType, addr *topo.EndPoint) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UnlockKeyspaceForAction(keyspace, lockPath, results string) error {
	return fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (topoServer *fakeTopo) UnlockShardForAction(keyspace, shard, lockPath, results string) error {
	return fmt.Errorf("not implemented")
}

type fakeDataSourcer struct {
	sqls      []string
	openFail  bool
	readFail  bool
	closeFail bool
}

func newFakeDataSourcer(sqls []string, openFail bool, readFail bool, closeFail bool) *fakeDataSourcer {
	return &fakeDataSourcer{sqls, openFail, readFail, closeFail}
}

func (sourcer fakeDataSourcer) Open() error {
	if sourcer.openFail {
		return errDataSourcerOpen
	}
	return nil
}

func (sourcer fakeDataSourcer) Read() ([]string, error) {
	if sourcer.readFail {
		return nil, errDataSourcerRead
	}
	return sourcer.sqls, nil
}

func (sourcer fakeDataSourcer) Close() error {
	if sourcer.closeFail {
		return errDataSourcerClose
	}
	return nil
}

type fakeEventHandler struct {
	onDataSourcerReadSuccessTriggered bool
	onDataSourcerReadFailTriggered    bool
	onValidationSuccessTriggered      bool
	onValidationFailTriggered         bool
	onExecutorCompleteTriggered       bool
}

func newFakeHandler() *fakeEventHandler {
	return &fakeEventHandler{}
}

func (handler *fakeEventHandler) OnDataSourcerReadSuccess([]string) error {
	handler.onDataSourcerReadSuccessTriggered = true
	return nil
}

func (handler *fakeEventHandler) OnDataSourcerReadFail(err error) error {
	handler.onDataSourcerReadFailTriggered = true
	return err
}

func (handler *fakeEventHandler) OnValidationSuccess([]string) error {
	handler.onValidationSuccessTriggered = true
	return nil
}

func (handler *fakeEventHandler) OnValidationFail(err error) error {
	handler.onValidationFailTriggered = true
	return err
}

func (handler *fakeEventHandler) OnExecutorComplete(*ExecuteResult) error {
	handler.onExecutorCompleteTriggered = true
	return nil
}

var _ EventHandler = (*fakeEventHandler)(nil)
var _ DataSourcer = (*fakeDataSourcer)(nil)
