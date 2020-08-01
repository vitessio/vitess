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
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
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

	// objects built at construction time
	qsc tabletserver.Controller
	tm  *tabletmanager.TabletManager
}

// tabletMap maps the tablet uid to the tablet record
var tabletMap map[uint32]*comboTablet

// CreateTablet creates an individual tablet, with its tm, and adds
// it to the map. If it's a master tablet, it also issues a TER.
func CreateTablet(ctx context.Context, ts *topo.Server, cell string, uid uint32, keyspace, shard, dbname string, tabletType topodatapb.TabletType, mysqld mysqlctl.MysqlDaemon, dbcfgs *dbconfigs.DBConfigs) error {
	alias := &topodatapb.TabletAlias{
		Cell: cell,
		Uid:  uid,
	}
	log.Infof("Creating %v tablet %v for %v/%v", tabletType, topoproto.TabletAliasString(alias), keyspace, shard)
	flag.Set("debug-url-prefix", fmt.Sprintf("/debug-%d", uid))

	controller := tabletserver.NewServer(topoproto.TabletAliasString(alias), ts, *alias)
	initTabletType := tabletType
	if tabletType == topodatapb.TabletType_MASTER {
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

	if tabletType == topodatapb.TabletType_MASTER {
		if err := tm.ChangeType(ctx, topodatapb.TabletType_MASTER); err != nil {
			return fmt.Errorf("TabletExternallyReparented failed on master %v: %v", topoproto.TabletAliasString(alias), err)
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

		qsc: controller,
		tm:  tm,
	}
	return nil
}

// InitTabletMap creates the action tms and associated data structures
// for all tablets, based on the vttest proto parameter.
func InitTabletMap(ts *topo.Server, tpb *vttestpb.VTTestTopology, mysqld mysqlctl.MysqlDaemon, dbcfgs *dbconfigs.DBConfigs, schemaDir string, mycnf *mysqlctl.Mycnf, ensureDatabase bool) error {
	tabletMap = make(map[uint32]*comboTablet)

	ctx := context.Background()

	// Register the tablet manager client factory for tablet manager
	// Do this before any tablets are created so that they respect the protocol,
	// otherwise it defaults to grpc
	tmclient.RegisterTabletManagerClientFactory("internal", func() tmclient.TabletManagerClient {
		return &internalTabletManagerClient{}
	})
	*tmclient.TabletManagerProtocol = "internal"

	// iterate through the keyspaces
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, nil)
	var uid uint32 = 1
	for _, kpb := range tpb.Keyspaces {
		keyspace := kpb.Name

		// First parse the ShardingColumnType.
		// Note if it's empty, we will return 'UNSET'.
		sct, err := key.ParseKeyspaceIDType(kpb.ShardingColumnType)
		if err != nil {
			return fmt.Errorf("parseKeyspaceIDType(%v) failed: %v", kpb.ShardingColumnType, err)
		}

		if kpb.ServedFrom != "" {
			// if we have a redirect, create a completely redirected
			// keyspace and no tablet
			if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{
				ShardingColumnName: kpb.ShardingColumnName,
				ShardingColumnType: sct,
				ServedFroms: []*topodatapb.Keyspace_ServedFrom{
					{
						TabletType: topodatapb.TabletType_MASTER,
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
				return fmt.Errorf("CreateKeyspace(%v) failed: %v", keyspace, err)
			}
		} else {
			// create a regular keyspace
			if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{
				ShardingColumnName: kpb.ShardingColumnName,
				ShardingColumnType: sct,
			}); err != nil {
				return fmt.Errorf("CreateKeyspace(%v) failed: %v", keyspace, err)
			}

			// iterate through the shards
			for _, spb := range kpb.Shards {
				shard := spb.Name
				ts.CreateShard(ctx, keyspace, shard)
				if err != nil {
					return fmt.Errorf("CreateShard(%v:%v) failed: %v", keyspace, shard, err)
				}

				for _, cell := range tpb.Cells {
					dbname := spb.DbNameOverride
					if dbname == "" {
						dbname = fmt.Sprintf("vt_%v_%v", keyspace, shard)
					}

					replicas := int(kpb.ReplicaCount)
					if replicas == 0 {
						// 2 replicas in order to ensure the master cell has a master and a replica
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
							return fmt.Errorf("GetConnection failed: %v", err)
						}
						defer conn.Close()

						_, err = conn.ExecuteFetch("CREATE DATABASE IF NOT EXISTS `"+dbname+"`", 1, false)
						if err != nil {
							return fmt.Errorf("error ensuring database exists: %v", err)
						}

					}
					if cell == tpb.Cells[0] {
						replicas--

						// create the master
						if err := CreateTablet(ctx, ts, cell, uid, keyspace, shard, dbname, topodatapb.TabletType_MASTER, mysqld, dbcfgs.Clone()); err != nil {
							return err
						}
						uid++
					}

					for i := 0; i < replicas; i++ {
						// create a replica tablet
						if err := CreateTablet(ctx, ts, cell, uid, keyspace, shard, dbname, topodatapb.TabletType_REPLICA, mysqld, dbcfgs.Clone()); err != nil {
							return err
						}
						uid++
					}

					for i := 0; i < rdonlys; i++ {
						// create a rdonly tablet
						if err := CreateTablet(ctx, ts, cell, uid, keyspace, shard, dbname, topodatapb.TabletType_RDONLY, mysqld, dbcfgs.Clone()); err != nil {
							return err
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
					return fmt.Errorf("cannot load vschema file %v for keyspace %v: %v", f, keyspace, err)
				}

				if err := ts.SaveVSchema(ctx, keyspace, formal); err != nil {
					return fmt.Errorf("SaveVSchema(%v) failed: %v", keyspace, err)
				}
			} else {
				log.Infof("File %v doesn't exist, skipping vschema for keyspace %v", f, keyspace)
			}
		}

		// Rebuild the SrvKeyspace object, so we can support
		// range-based sharding queries, and export the redirects.
		if err := wr.RebuildKeyspaceGraph(ctx, keyspace, nil); err != nil {
			return fmt.Errorf("cannot rebuild %v: %v", keyspace, err)
		}
	}

	// Rebuild the SrvVSchema object
	if err := ts.RebuildSrvVSchema(ctx, tpb.Cells); err != nil {
		return fmt.Errorf("RebuildVSchemaGraph failed: %v", err)
	}

	// Register the tablet dialer for tablet server
	tabletconn.RegisterDialer("internal", dialer)
	*tabletconn.TabletProtocol = "internal"

	// run healthcheck on all vttablets
	tmc := tmclient.NewTabletManagerClient()
	for _, tablet := range tabletMap {
		tabletInfo, err := ts.GetTablet(ctx, tablet.alias)
		if err != nil {
			return fmt.Errorf("cannot find tablet: %+v", tablet.alias)
		}
		tmc.RunHealthCheck(ctx, tabletInfo.Tablet)
	}

	return nil
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
func (itc *internalTabletConn) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	bindVars = sqltypes.CopyBindVariables(bindVars)
	reply, err := itc.tablet.qsc.QueryService().Execute(ctx, target, query, bindVars, transactionID, reservedID, options)
	if err != nil {
		return nil, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
	}
	return reply, nil
}

// ExecuteBatch is part of queryservice.QueryService
// We need to copy the bind variables as tablet server will change them.
func (itc *internalTabletConn) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	q := make([]*querypb.BoundQuery, len(queries))
	for i, query := range queries {
		q[i] = &querypb.BoundQuery{
			Sql:           query.Sql,
			BindVariables: sqltypes.CopyBindVariables(query.BindVariables),
		}
	}
	results, err := itc.tablet.qsc.QueryService().ExecuteBatch(ctx, target, q, asTransaction, transactionID, options)
	if err != nil {
		return nil, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
	}
	return results, nil
}

// StreamExecute is part of queryservice.QueryService
// We need to copy the bind variables as tablet server will change them.
func (itc *internalTabletConn) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	bindVars = sqltypes.CopyBindVariables(bindVars)
	err := itc.tablet.qsc.QueryService().StreamExecute(ctx, target, query, bindVars, transactionID, options, callback)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// Begin is part of queryservice.QueryService
func (itc *internalTabletConn) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (int64, *topodatapb.TabletAlias, error) {
	transactionID, alias, err := itc.tablet.qsc.QueryService().Begin(ctx, target, options)
	if err != nil {
		return 0, nil, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
	}
	return transactionID, alias, nil
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
func (itc *internalTabletConn) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) error {
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
func (itc *internalTabletConn) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, query string, bindVars map[string]*querypb.BindVariable, reserveID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	bindVars = sqltypes.CopyBindVariables(bindVars)
	result, transactionID, tabletAlias, err := itc.tablet.qsc.QueryService().BeginExecute(ctx, target, preQueries, query, bindVars, reserveID, options)
	return result, transactionID, tabletAlias, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// BeginExecuteBatch is part of queryservice.QueryService
func (itc *internalTabletConn) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	transactionID, alias, err := itc.Begin(ctx, target, options)
	if err != nil {
		return nil, 0, nil, err
	}
	results, err := itc.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
	return results, transactionID, alias, err
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

// Handle panic is part of the QueryService interface.
func (itc *internalTabletConn) HandlePanic(err *error) {
}

//ReserveBeginExecute is part of the QueryService interface.
func (itc *internalTabletConn) ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, int64, *topodatapb.TabletAlias, error) {
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	res, transactionID, reservedID, alias, err := itc.tablet.qsc.QueryService().ReserveBeginExecute(ctx, target, preQueries, sql, bindVariables, options)
	return res, transactionID, reservedID, alias, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

//ReserveBeginExecute is part of the QueryService interface.
func (itc *internalTabletConn) ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	res, reservedID, alias, err := itc.tablet.qsc.QueryService().ReserveExecute(ctx, target, preQueries, sql, bindVariables, transactionID, options)
	return res, reservedID, alias, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

//Release is part of the QueryService interface.
func (itc *internalTabletConn) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	err := itc.tablet.qsc.QueryService().Release(ctx, target, transactionID, reservedID)
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
func (itc *internalTabletConn) VStream(ctx context.Context, target *querypb.Target, startPos string, tableLastPKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	err := itc.tablet.qsc.QueryService().VStream(ctx, target, startPos, tableLastPKs, filter, send)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// VStreamRows is part of the QueryService interface.
func (itc *internalTabletConn) VStreamRows(ctx context.Context, target *querypb.Target, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	err := itc.tablet.qsc.QueryService().VStreamRows(ctx, target, query, lastpk, send)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

// VStreamResults is part of the QueryService interface.
func (itc *internalTabletConn) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	err := itc.tablet.qsc.QueryService().VStreamResults(ctx, target, query, send)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

//
// TabletManagerClient implementation
//

// internalTabletManagerClient implements tmclient.TabletManagerClient
type internalTabletManagerClient struct{}

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

func (itmc *internalTabletManagerClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.tm.GetSchema(ctx, tables, excludeTables, includeViews)
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

func (itmc *internalTabletManagerClient) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	t.tm.ChangeType(ctx, dbType)
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

func (itmc *internalTabletManagerClient) IgnoreHealthError(ctx context.Context, tablet *topodatapb.Tablet, pattern string) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	t.tm.IgnoreHealthError(ctx, pattern)
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

func (itmc *internalTabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, query []byte, maxRows int, disableBinlogs, reloadSchema bool) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ExecuteFetchAsAllPrivs(ctx context.Context, tablet *topodatapb.Tablet, query []byte, maxRows int, reloadSchema bool) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, query []byte, maxRows int) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

// Deprecated
func (itmc *internalTabletManagerClient) SlaveStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) MasterStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.MasterStatus, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) MasterPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) WaitForPosition(ctx context.Context, tablet *topodatapb.Tablet, pos string) error {
	return fmt.Errorf("not implemented in vtcombo")
}

// Deprecated
func (itmc *internalTabletManagerClient) StopSlave(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

// Deprecated
func (itmc *internalTabletManagerClient) StopSlaveMinimum(ctx context.Context, tablet *topodatapb.Tablet, stopPos string, waitTime time.Duration) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

// Deprecated
func (itmc *internalTabletManagerClient) StartSlave(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

// Deprecated
func (itmc *internalTabletManagerClient) StartSlaveUntilAfter(ctx context.Context, tablet *topodatapb.Tablet, position string, duration time.Duration) error {
	return fmt.Errorf("not implemented in vtcombo")
}

// Deprecated
func (itmc *internalTabletManagerClient) GetSlaves(ctx context.Context, tablet *topodatapb.Tablet) ([]string, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int, pos string) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ResetReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) InitMaster(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PopulateReparentJournal(ctx context.Context, tablet *topodatapb.Tablet, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, pos string) error {
	return fmt.Errorf("not implemented in vtcombo")
}

// Deprecated
func (itmc *internalTabletManagerClient) InitSlave(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) DemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.MasterStatus, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) UndoDemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

// Deprecated
func (itmc *internalTabletManagerClient) SlaveWasPromoted(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SetMaster(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartSlave bool) error {
	return fmt.Errorf("not implemented in vtcombo")
}

// Deprecated
func (itmc *internalTabletManagerClient) SlaveWasRestarted(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet, stopReplicationMode replicationdatapb.StopReplicationMode) (*replicationdatapb.Status, *replicationdatapb.StopReplicationStatus, error) {
	return nil, nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PromoteReplica(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) Backup(ctx context.Context, tablet *topodatapb.Tablet, concurrency int, allowMaster bool) (logutil.EventStream, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) RestoreFromBackup(ctx context.Context, tablet *topodatapb.Tablet) (logutil.EventStream, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) Close() {
}

func (itmc *internalTabletManagerClient) ReplicationStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopReplicationMinimum(ctx context.Context, tablet *topodatapb.Tablet, stopPos string, waitTime time.Duration) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StartReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StartReplicationUntilAfter(ctx context.Context, tablet *topodatapb.Tablet, position string, duration time.Duration) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) GetReplicas(ctx context.Context, tablet *topodatapb.Tablet) ([]string, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) InitReplica(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ReplicaWasPromoted(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ReplicaWasRestarted(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias) error {
	return fmt.Errorf("not implemented in vtcombo")
}
