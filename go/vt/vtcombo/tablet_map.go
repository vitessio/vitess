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

package vtcombo

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

// tablet contains all the data for an individual tablet.
type comboTablet struct {
	// configuration parameters
	alias      *topodatapb.TabletAlias
	keyspace   string
	shard      string
	tabletType topodatapb.TabletType
	dbname     string
	uid        uint32

	// objects built at construction time
	qsc tabletserver.Controller
	tm  *tabletmanager.TabletManager
}

// tabletMap maps the tablet uid to the tablet record
var tabletMap map[uint32]*comboTablet

// CreateTablet creates an individual tablet, with its tm, and adds
// it to the map. If it's a primary tablet, it also issues a TER.
func CreateTablet(
	ctx context.Context,
	ts *topo.Server,
	cell string,
	uid uint32,
	keyspace, shard, dbname string,
	tabletType topodatapb.TabletType,
	mysqld mysqlctl.MysqlDaemon,
	dbcfgs *dbconfigs.DBConfigs,
) error {
	alias := &topodatapb.TabletAlias{
		Cell: cell,
		Uid:  uid,
	}
	log.Infof("Creating %v tablet %v for %v/%v", tabletType, topoproto.TabletAliasString(alias), keyspace, shard)

	controller := tabletserver.NewServer(topoproto.TabletAliasString(alias), ts, alias)
	initTabletType := tabletType
	if tabletType == topodatapb.TabletType_PRIMARY {
		initTabletType = topodatapb.TabletType_REPLICA
	}
	_, kr, err := topo.ValidateShardName(shard)
	if err != nil {
		return err
	}
	tm := &tabletmanager.TabletManager{
		BatchCtx:            context.Background(),
		TopoServer:          ts,
		MysqlDaemon:         mysqld,
		DBConfigs:           dbcfgs,
		QueryServiceControl: controller,
	}
	tablet := &topodatapb.Tablet{
		Alias: alias,
		PortMap: map[string]int32{
			"vt":   int32(8000 + uid),
			"grpc": int32(9000 + uid),
		},
		Keyspace:       keyspace,
		Shard:          shard,
		KeyRange:       kr,
		Type:           initTabletType,
		DbNameOverride: dbname,
	}
	if err := tm.Start(tablet, 0); err != nil {
		return err
	}

	if tabletType == topodatapb.TabletType_PRIMARY {
		// Semi-sync has to be set to false, since we have 1 single backing MySQL
		if err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY /* semi-sync */, false); err != nil {
			return fmt.Errorf("TabletExternallyReparented failed on primary %v: %v", topoproto.TabletAliasString(alias), err)
		}
	}
	controller.AddStatusHeader()
	controller.AddStatusPart()
	tabletMap[uid] = &comboTablet{
		alias:      alias,
		keyspace:   keyspace,
		shard:      shard,
		tabletType: tabletType,
		dbname:     dbname,
		uid:        uid,

		qsc: controller,
		tm:  tm,
	}
	return nil
}

// InitRoutingRules saves the routing rules into ts and reloads the vschema.
func InitRoutingRules(
	ctx context.Context,
	ts *topo.Server,
	rr *vschemapb.RoutingRules,
) error {
	if rr == nil {
		return nil
	}

	if err := ts.SaveRoutingRules(ctx, rr); err != nil {
		return err
	}

	return ts.RebuildSrvVSchema(ctx, nil)
}

// InitTabletMap creates the action tms and associated data structures
// for all tablets, based on the vttest proto parameter.
func InitTabletMap(
	ts *topo.Server,
	tpb *vttestpb.VTTestTopology,
	mysqld mysqlctl.MysqlDaemon,
	dbcfgs *dbconfigs.DBConfigs,
	schemaDir string,
	ensureDatabase bool,
) (uint32, error) {
	tabletMap = make(map[uint32]*comboTablet)

	ctx := context.Background()

	// Register the tablet manager client factory for tablet manager
	// Do this before any tablets are created so that they respect the protocol,
	// otherwise it defaults to grpc.
	//
	// main() forces the --tablet_manager_protocol flag to this value.
	tmclient.RegisterTabletManagerClientFactory("internal", func() tmclient.TabletManagerClient {
		return &internalTabletManagerClient{}
	})

	// iterate through the keyspaces
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, nil)
	var uid uint32 = 1
	for _, kpb := range tpb.Keyspaces {
		var err error
		uid, err = CreateKs(ctx, ts, tpb, mysqld, dbcfgs, schemaDir, kpb, ensureDatabase, uid, wr)
		if err != nil {
			return 0, err
		}
	}

	// Rebuild the SrvVSchema object
	if err := ts.RebuildSrvVSchema(ctx, tpb.Cells); err != nil {
		return 0, fmt.Errorf("RebuildVSchemaGraph failed: %v", err)
	}

	// Register the tablet dialer for tablet server. main() forces the --tablet_protocol
	// flag to this value.
	tabletconn.RegisterDialer("internal", dialer)

	// run healthcheck on all vttablets
	tmc := tmclient.NewTabletManagerClient()
	for _, tablet := range tabletMap {
		tabletInfo, err := ts.GetTablet(ctx, tablet.alias)
		if err != nil {
			return 0, fmt.Errorf("cannot find tablet: %+v", tablet.alias)
		}
		tmc.RunHealthCheck(ctx, tabletInfo.Tablet)
	}

	return uid, nil
}

// DeleteKs deletes keyspace, shards and tablets with mysql databases
func DeleteKs(
	ctx context.Context,
	ts *topo.Server,
	ksName string,
	mysqld mysqlctl.MysqlDaemon,
	tpb *vttestpb.VTTestTopology,
) error {
	for key, tablet := range tabletMap {
		if tablet.keyspace == ksName {
			delete(tabletMap, key)
			tablet.tm.Stop()
			tablet.tm.Close()
			tablet.qsc.SchemaEngine().Close()
			err := ts.DeleteTablet(ctx, tablet.alias)
			if err != nil {
				return err
			}
		}
	}

	var ks *vttestpb.Keyspace
	index := 0
	for _, keyspace := range tpb.Keyspaces {
		if keyspace.Name == ksName {
			ks = keyspace
			break
		}
		index++
	}
	if ks == nil {
		return fmt.Errorf("database not found")
	}

	conn, err := mysqld.GetDbaConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, shard := range ks.Shards {
		q := fmt.Sprintf("DROP DATABASE IF EXISTS `vt_%s_%s`", ksName, shard.GetName())
		if _, err = conn.ExecuteFetch(q, 1, false); err != nil {
			return err
		}
		if err := ts.DeleteShard(ctx, ksName, shard.GetName()); err != nil {
			return err
		}
	}

	if err = ts.DeleteKeyspace(ctx, ksName); err != nil {
		return err
	}

	kss := tpb.Keyspaces             // to save on chars
	copy(kss[index:], kss[index+1:]) // shift keyspaces to the left, overwriting the value to remove
	tpb.Keyspaces = kss[:len(kss)-1] // shrink the slice by one

	return nil
}

// CreateKs creates keyspace, shards and tablets with mysql database
func CreateKs(
	ctx context.Context,
	ts *topo.Server,
	tpb *vttestpb.VTTestTopology,
	mysqld mysqlctl.MysqlDaemon,
	dbcfgs *dbconfigs.DBConfigs,
	schemaDir string,
	kpb *vttestpb.Keyspace,
	ensureDatabase bool,
	uid uint32,
	wr *wrangler.Wrangler,
) (uint32, error) {
	keyspace := kpb.Name

	if kpb.ServedFrom != "" {
		// if we have a redirect, create a completely redirected
		// keyspace and no tablet
		if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{
			ServedFroms: []*topodatapb.Keyspace_ServedFrom{
				{
					TabletType: topodatapb.TabletType_PRIMARY,
					Keyspace:   kpb.ServedFrom,
				},
				{
					TabletType: topodatapb.TabletType_REPLICA,
					Keyspace:   kpb.ServedFrom,
				},
				{
					TabletType: topodatapb.TabletType_RDONLY,
					Keyspace:   kpb.ServedFrom,
				},
			},
		}); err != nil {
			return 0, fmt.Errorf("CreateKeyspace(%v) failed: %v", keyspace, err)
		}
	} else {
		// create a regular keyspace
		if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil {
			return 0, fmt.Errorf("CreateKeyspace(%v) failed: %v", keyspace, err)
		}

		// iterate through the shards
		for _, spb := range kpb.Shards {
			shard := spb.Name
			if err := ts.CreateShard(ctx, keyspace, shard); err != nil {
				return 0, fmt.Errorf("CreateShard(%v:%v) failed: %v", keyspace, shard, err)
			}

			for _, cell := range tpb.Cells {
				dbname := spb.DbNameOverride
				if dbname == "" {
					dbname = fmt.Sprintf("vt_%v_%v", keyspace, shard)
				}

				replicas := int(kpb.ReplicaCount)
				if replicas == 0 {
					// 2 replicas in order to ensure the primary cell has a primary and a replica
					replicas = 2
				}
				rdonlys := int(kpb.RdonlyCount)
				if rdonlys == 0 {
					rdonlys = 1
				}

				if ensureDatabase {
					// Create Database if not exist
					conn, err := mysqld.GetDbaConnection(context.TODO())
					if err != nil {
						return 0, fmt.Errorf("GetConnection failed: %v", err)
					}
					defer conn.Close()

					_, err = conn.ExecuteFetch("CREATE DATABASE IF NOT EXISTS `"+dbname+"`", 1, false)
					if err != nil {
						return 0, fmt.Errorf("error ensuring database exists: %v", err)
					}

				}
				if cell == tpb.Cells[0] {
					replicas--

					// create the primary
					if err := CreateTablet(ctx, ts, cell, uid, keyspace, shard, dbname, topodatapb.TabletType_PRIMARY, mysqld, dbcfgs.Clone()); err != nil {
						return 0, err
					}
					uid++
				}

				for i := 0; i < replicas; i++ {
					// create a replica tablet
					if err := CreateTablet(ctx, ts, cell, uid, keyspace, shard, dbname, topodatapb.TabletType_REPLICA, mysqld, dbcfgs.Clone()); err != nil {
						return 0, err
					}
					uid++
				}

				for i := 0; i < rdonlys; i++ {
					// create a rdonly tablet
					if err := CreateTablet(ctx, ts, cell, uid, keyspace, shard, dbname, topodatapb.TabletType_RDONLY, mysqld, dbcfgs.Clone()); err != nil {
						return 0, err
					}
					uid++
				}
			}
		}
	}

	// vschema for the keyspace
	if schemaDir != "" {
		f := path.Join(schemaDir, keyspace, "vschema.json")
		if _, err := os.Stat(f); err == nil {
			// load the vschema
			formal, err := vindexes.LoadFormalKeyspace(f)
			if err != nil {
				return 0, fmt.Errorf("cannot load vschema file %v for keyspace %v: %v", f, keyspace, err)
			}

			if err := ts.SaveVSchema(ctx, keyspace, formal); err != nil {
				return 0, fmt.Errorf("SaveVSchema(%v) failed: %v", keyspace, err)
			}
		} else {
			log.Infof("File %v doesn't exist, skipping vschema for keyspace %v", f, keyspace)
		}
	}

	// Rebuild the SrvKeyspace object, so we can support
	// range-based sharding queries, and export the redirects.
	if err := topotools.RebuildKeyspace(ctx, wr.Logger(), wr.TopoServer(), keyspace, nil, false); err != nil {
		return 0, fmt.Errorf("cannot rebuild %v: %v", keyspace, err)
	}
	return uid, nil
}

//
// TabletConn implementation
//

// dialer is our tabletconn.Dialer
func dialer(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "connection refused")
	}

	return &internalTabletConn{
		tablet:     t,
		topoTablet: tablet,
	}, nil
}

// internalTabletConn implements queryservice.QueryService by forwarding everything
// to the tablet
type internalTabletConn struct {
	tablet     *comboTablet
	topoTablet *topodatapb.Tablet
}

var _ queryservice.QueryService = (*internalTabletConn)(nil)

// Execute is part of queryservice.QueryService
// We need to copy the bind variables as tablet server will change them.
func (itc *internalTabletConn) Execute(
	ctx context.Context,
	target *querypb.Target,
	query string,
	bindVars map[string]*querypb.BindVariable,
	transactionID, reservedID int64,
	options *querypb.ExecuteOptions,
) (*sqltypes.Result, error) {
	bindVars = sqltypes.CopyBindVariables(bindVars)
	reply, err := itc.tablet.qsc.QueryService().Execute(ctx, target, query, bindVars, transactionID, reservedID, options)
	if err != nil {
		return nil, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
	}
	return reply, nil
}

// StreamExecute is part of queryservice.QueryService
// We need to copy the bind variables as tablet server will change them.
func (itc *internalTabletConn) StreamExecute(
	ctx context.Context,
	target *querypb.Target,
	query string,
	bindVars map[string]*querypb.BindVariable,
	transactionID int64,
	reservedID int64,
	options *querypb.ExecuteOptions,
	callback func(*sqltypes.Result) error,
) error {
	bindVars = sqltypes.CopyBindVariables(bindVars)
	err := itc.tablet.qsc.QueryService().StreamExecute(ctx, target, query, bindVars, transactionID, reservedID, options, callback)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// Begin is part of queryservice.QueryService
func (itc *internalTabletConn) Begin(
	ctx context.Context,
	target *querypb.Target,
	options *querypb.ExecuteOptions,
) (queryservice.TransactionState, error) {
	state, err := itc.tablet.qsc.QueryService().Begin(ctx, target, options)
	return state, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// Commit is part of queryservice.QueryService
func (itc *internalTabletConn) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	rID, err := itc.tablet.qsc.QueryService().Commit(ctx, target, transactionID)
	return rID, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// Rollback is part of queryservice.QueryService
func (itc *internalTabletConn) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	rID, err := itc.tablet.qsc.QueryService().Rollback(ctx, target, transactionID)
	return rID, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// Prepare is part of queryservice.QueryService
func (itc *internalTabletConn) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	err := itc.tablet.qsc.QueryService().Prepare(ctx, target, transactionID, dtid)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// CommitPrepared is part of queryservice.QueryService
func (itc *internalTabletConn) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) error {
	err := itc.tablet.qsc.QueryService().CommitPrepared(ctx, target, dtid)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// RollbackPrepared is part of queryservice.QueryService
func (itc *internalTabletConn) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) error {
	err := itc.tablet.qsc.QueryService().RollbackPrepared(ctx, target, dtid, originalID)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// CreateTransaction is part of queryservice.QueryService
func (itc *internalTabletConn) CreateTransaction(
	ctx context.Context,
	target *querypb.Target,
	dtid string,
	participants []*querypb.Target,
) error {
	err := itc.tablet.qsc.QueryService().CreateTransaction(ctx, target, dtid, participants)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// StartCommit is part of queryservice.QueryService
func (itc *internalTabletConn) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	err := itc.tablet.qsc.QueryService().StartCommit(ctx, target, transactionID, dtid)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// SetRollback is part of queryservice.QueryService
func (itc *internalTabletConn) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) error {
	err := itc.tablet.qsc.QueryService().SetRollback(ctx, target, dtid, transactionID)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// ConcludeTransaction is part of queryservice.QueryService
func (itc *internalTabletConn) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) error {
	err := itc.tablet.qsc.QueryService().ConcludeTransaction(ctx, target, dtid)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// ReadTransaction is part of queryservice.QueryService
func (itc *internalTabletConn) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	metadata, err = itc.tablet.qsc.QueryService().ReadTransaction(ctx, target, dtid)
	return metadata, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// BeginExecute is part of queryservice.QueryService
func (itc *internalTabletConn) BeginExecute(
	ctx context.Context,
	target *querypb.Target,
	preQueries []string,
	query string,
	bindVars map[string]*querypb.BindVariable,
	reserveID int64,
	options *querypb.ExecuteOptions,
) (queryservice.TransactionState, *sqltypes.Result, error) {
	bindVars = sqltypes.CopyBindVariables(bindVars)
	state, result, err := itc.tablet.qsc.QueryService().BeginExecute(ctx, target, preQueries, query, bindVars, reserveID, options)
	return state, result, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// BeginStreamExecute is part of queryservice.QueryService
func (itc *internalTabletConn) BeginStreamExecute(
	ctx context.Context,
	target *querypb.Target,
	preQueries []string,
	query string,
	bindVars map[string]*querypb.BindVariable,
	reservedID int64,
	options *querypb.ExecuteOptions,
	callback func(*sqltypes.Result) error,
) (queryservice.TransactionState, error) {
	bindVars = sqltypes.CopyBindVariables(bindVars)
	state, err := itc.tablet.qsc.QueryService().BeginStreamExecute(ctx, target, preQueries, query, bindVars, reservedID, options, callback)
	return state, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// MessageStream is part of queryservice.QueryService
func (itc *internalTabletConn) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	err := itc.tablet.qsc.QueryService().MessageStream(ctx, target, name, callback)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// MessageAck is part of queryservice.QueryService
func (itc *internalTabletConn) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (int64, error) {
	count, err := itc.tablet.qsc.QueryService().MessageAck(ctx, target, name, ids)
	return count, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// HandlePanic is part of the QueryService interface.
func (itc *internalTabletConn) HandlePanic(err *error) {
}

// ReserveBeginExecute is part of the QueryService interface.
func (itc *internalTabletConn) ReserveBeginExecute(
	ctx context.Context,
	target *querypb.Target,
	preQueries []string,
	postBeginQueries []string,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	options *querypb.ExecuteOptions,
) (queryservice.ReservedTransactionState, *sqltypes.Result, error) {
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	state, result, err := itc.tablet.qsc.QueryService().ReserveBeginExecute(ctx, target, preQueries, postBeginQueries, sql, bindVariables, options)
	return state, result, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// ReserveBeginStreamExecute is part of the QueryService interface.
func (itc *internalTabletConn) ReserveBeginStreamExecute(
	ctx context.Context,
	target *querypb.Target,
	preQueries []string,
	postBeginQueries []string,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	options *querypb.ExecuteOptions,
	callback func(*sqltypes.Result) error,
) (queryservice.ReservedTransactionState, error) {
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	state, err := itc.tablet.qsc.QueryService().ReserveBeginStreamExecute(ctx, target, preQueries, postBeginQueries, sql, bindVariables, options, callback)
	return state, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// ReserveExecute is part of the QueryService interface.
func (itc *internalTabletConn) ReserveExecute(
	ctx context.Context,
	target *querypb.Target,
	preQueries []string,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	transactionID int64,
	options *querypb.ExecuteOptions,
) (queryservice.ReservedState, *sqltypes.Result, error) {
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	state, result, err := itc.tablet.qsc.QueryService().ReserveExecute(ctx, target, preQueries, sql, bindVariables, transactionID, options)
	return state, result, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// ReserveStreamExecute is part of the QueryService interface.
func (itc *internalTabletConn) ReserveStreamExecute(
	ctx context.Context,
	target *querypb.Target,
	preQueries []string,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	transactionID int64,
	options *querypb.ExecuteOptions,
	callback func(*sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	state, err := itc.tablet.qsc.QueryService().ReserveStreamExecute(ctx, target, preQueries, sql, bindVariables, transactionID, options, callback)
	return state, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// Release is part of the QueryService interface.
func (itc *internalTabletConn) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	err := itc.tablet.qsc.QueryService().Release(ctx, target, transactionID, reservedID)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// GetSchema is part of the QueryService interface.
func (itc *internalTabletConn) GetSchema(ctx context.Context, target *querypb.Target, tableType querypb.SchemaTableType, tableNames []string, callback func(schemaRes *querypb.GetSchemaResponse) error) error {
	err := itc.tablet.qsc.QueryService().GetSchema(ctx, target, tableType, tableNames, callback)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// Close is part of queryservice.QueryService
func (itc *internalTabletConn) Close(ctx context.Context) error {
	return nil
}

// Tablet is part of queryservice.QueryService
func (itc *internalTabletConn) Tablet() *topodatapb.Tablet {
	return itc.topoTablet
}

// StreamHealth is part of queryservice.QueryService
func (itc *internalTabletConn) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	err := itc.tablet.qsc.QueryService().StreamHealth(ctx, callback)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// VStream is part of queryservice.QueryService.
func (itc *internalTabletConn) VStream(
	ctx context.Context,
	request *binlogdatapb.VStreamRequest,
	send func([]*binlogdatapb.VEvent) error,
) error {
	err := itc.tablet.qsc.QueryService().VStream(ctx, request, send)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// VStreamRows is part of the QueryService interface.
func (itc *internalTabletConn) VStreamRows(
	ctx context.Context,
	request *binlogdatapb.VStreamRowsRequest,
	send func(*binlogdatapb.VStreamRowsResponse) error,
) error {
	err := itc.tablet.qsc.QueryService().VStreamRows(ctx, request, send)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// VStreamResults is part of the QueryService interface.
func (itc *internalTabletConn) VStreamResults(
	ctx context.Context,
	target *querypb.Target,
	query string,
	send func(*binlogdatapb.VStreamResultsResponse) error,
) error {
	err := itc.tablet.qsc.QueryService().VStreamResults(ctx, target, query, send)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

//
// TabletManagerClient implementation
//

// internalTabletManagerClient implements tmclient.TabletManagerClient
type internalTabletManagerClient struct{}

func (itmc *internalTabletManagerClient) VDiff(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	return nil, fmt.Errorf("VDiff not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) LockTables(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) UnlockTables(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) Ping(ctx context.Context, tablet *topodatapb.Tablet) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	t.tm.Ping(ctx, "payload")
	return nil
}

func (itmc *internalTabletManagerClient) GetSchema(
	ctx context.Context,
	tablet *topodatapb.Tablet,
	request *tabletmanagerdatapb.GetSchemaRequest,
) (*tabletmanagerdatapb.SchemaDefinition, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.tm.GetSchema(ctx, request)
}

func (itmc *internalTabletManagerClient) GetPermissions(ctx context.Context, tablet *topodatapb.Tablet) (*tabletmanagerdatapb.Permissions, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.tm.GetPermissions(ctx)
}

func (itmc *internalTabletManagerClient) SetReadOnly(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SetReadWrite(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType, semiSync bool) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	t.tm.ChangeType(ctx, dbType, semiSync)
	return nil
}

func (itmc *internalTabletManagerClient) Sleep(ctx context.Context, tablet *topodatapb.Tablet, duration time.Duration) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	t.tm.Sleep(ctx, duration)
	return nil
}

func (itmc *internalTabletManagerClient) ExecuteHook(ctx context.Context, tablet *topodatapb.Tablet, hk *hook.Hook) (*hook.HookResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.tm.RefreshState(ctx)
}

func (itmc *internalTabletManagerClient) RunHealthCheck(ctx context.Context, tablet *topodatapb.Tablet) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	t.tm.RunHealthCheck(ctx)
	return nil
}

func (itmc *internalTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, waitPosition string) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.tm.ReloadSchema(ctx, waitPosition)
}

func (itmc *internalTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topodatapb.Tablet, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.tm.PreflightSchema(ctx, changes)
}

func (itmc *internalTabletManagerClient) ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.tm.ApplySchema(ctx, change)
}

func (itmc *internalTabletManagerClient) ExecuteQuery(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.ExecuteQueryRequest) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ExecuteFetchAsDba(context.Context, *topodatapb.Tablet, bool, *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ExecuteFetchAsAllPrivs(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ExecuteFetchAsApp(context.Context, *topodatapb.Tablet, bool, *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PrimaryStatus(context.Context, *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PrimaryPosition(context.Context, *topodatapb.Tablet) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) WaitForPosition(context.Context, *topodatapb.Tablet, string) error {
	return fmt.Errorf("not implemented in vtcombo")
}

//
// VReplication related methods
//

func (itmc *internalTabletManagerClient) CreateVReplicationWorkflow(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) (*tabletmanagerdatapb.CreateVReplicationWorkflowResponse, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) DeleteVReplicationWorkflow(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.DeleteVReplicationWorkflowRequest) (*tabletmanagerdatapb.DeleteVReplicationWorkflowResponse, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ReadVReplicationWorkflow(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) VReplicationExec(context.Context, *topodatapb.Tablet, string) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) VReplicationWaitForPos(context.Context, *topodatapb.Tablet, int32, string) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) UpdateVReplicationWorkflow(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ResetReplication(context.Context, *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) InitPrimary(context.Context, *topodatapb.Tablet, bool) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PopulateReparentJournal(context.Context, *topodatapb.Tablet, int64, string, *topodatapb.TabletAlias, string) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) DemotePrimary(context.Context, *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) UndoDemotePrimary(context.Context, *topodatapb.Tablet, bool) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SetReplicationSource(context.Context, *topodatapb.Tablet, *topodatapb.TabletAlias, int64, string, bool, bool) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopReplicationAndGetStatus(context.Context, *topodatapb.Tablet, replicationdatapb.StopReplicationMode) (*replicationdatapb.StopReplicationStatus, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PromoteReplica(context.Context, *topodatapb.Tablet, bool) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) Backup(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.BackupRequest) (logutil.EventStream, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) RestoreFromBackup(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.RestoreFromBackupRequest) (logutil.EventStream, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) CheckThrottler(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.CheckThrottlerRequest) (*tabletmanagerdatapb.CheckThrottlerResponse, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) Close() {
}

func (itmc *internalTabletManagerClient) ReplicationStatus(context.Context, *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) FullStatus(context.Context, *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopReplication(context.Context, *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopReplicationMinimum(context.Context, *topodatapb.Tablet, string, time.Duration) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StartReplication(context.Context, *topodatapb.Tablet, bool) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StartReplicationUntilAfter(context.Context, *topodatapb.Tablet, string, time.Duration) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) GetReplicas(context.Context, *topodatapb.Tablet) ([]string, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) InitReplica(context.Context, *topodatapb.Tablet, *topodatapb.TabletAlias, string, int64, bool) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ReplicaWasPromoted(context.Context, *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ResetReplicationParameters(context.Context, *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ReplicaWasRestarted(context.Context, *topodatapb.Tablet, *topodatapb.TabletAlias) error {
	return fmt.Errorf("not implemented in vtcombo")
}
func (itmc *internalTabletManagerClient) ResetSequences(ctx context.Context, tablet *topodatapb.Tablet, tables []string) error {
	return fmt.Errorf("not implemented in vtcombo")
}
