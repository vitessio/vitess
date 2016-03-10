package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// tablet contains all the data for an individual tablet.
type tablet struct {
	// configuration parameters
	keyspace   string
	shard      string
	tabletType topodatapb.TabletType
	dbname     string

	// objects built at construction time
	qsc   tabletserver.Controller
	agent *tabletmanager.ActionAgent
}

// tabletMap maps the tablet uid to the tablet record
var tabletMap map[uint32]*tablet

// initTabletMap creates the action agents and associated data structures
// for all tablets
func initTabletMap(ts topo.Server, topology string, mysqld mysqlctl.MysqlDaemon, dbcfgs dbconfigs.DBConfigs, mycnf *mysqlctl.Mycnf) {
	tabletMap = make(map[uint32]*tablet)

	ctx := context.Background()

	// disable publishing of stats from query service
	flag.Lookup("queryserver-config-enable-publish-stats").Value.Set("false")

	var uid uint32 = 1
	keyspaceMap := make(map[string]bool)
	for _, entry := range strings.Split(topology, ",") {
		slash := strings.IndexByte(entry, '/')
		column := strings.IndexByte(entry, ':')
		if slash == -1 || column == -1 {
			log.Fatalf("invalid topology entry: %v", entry)
		}

		keyspace := entry[:slash]
		shard := entry[slash+1 : column]
		dbname := entry[column+1:]
		dbcfgs.App.DbName = dbname

		// create the keyspace if necessary, so we can set the
		// ShardingColumnName and ShardingColumnType
		if _, ok := keyspaceMap[keyspace]; !ok {
			// only set for sharding key info for sharded keyspaces
			scn := ""
			sct := topodatapb.KeyspaceIdType_UNSET
			if shard != "0" {
				var err error
				sct, err = key.ParseKeyspaceIDType(*shardingColumnType)
				if err != nil {
					log.Fatalf("parseKeyspaceIDType(%v) failed: %v", *shardingColumnType, err)
				}
				scn = *shardingColumnName
			}

			if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{
				ShardingColumnName: scn,
				ShardingColumnType: sct,
			}); err != nil {
				log.Fatalf("CreateKeyspace(%v) failed: %v", keyspace, err)
			}
			keyspaceMap[keyspace] = true
		}

		// create the master
		alias := &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		}
		log.Infof("Creating master tablet %v for %v/%v", topoproto.TabletAliasString(alias), keyspace, shard)
		flag.Lookup("debug-url-prefix").Value.Set(fmt.Sprintf("/debug-%d", uid))
		masterController := tabletserver.NewServer()
		masterAgent := tabletmanager.NewComboActionAgent(ctx, ts, alias, int32(8000+uid), int32(9000+uid), masterController, dbcfgs, mysqld, keyspace, shard, dbname, "replica")
		if err := masterAgent.TabletExternallyReparented(ctx, ""); err != nil {
			log.Fatalf("TabletExternallyReparented failed on master: %v", err)
		}
		tabletMap[uid] = &tablet{
			keyspace:   keyspace,
			shard:      shard,
			tabletType: topodatapb.TabletType_MASTER,
			dbname:     dbname,

			qsc:   masterController,
			agent: masterAgent,
		}
		uid++

		// create a replica slave
		alias = &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		}
		log.Infof("Creating replica tablet %v for %v/%v", topoproto.TabletAliasString(alias), keyspace, shard)
		flag.Lookup("debug-url-prefix").Value.Set(fmt.Sprintf("/debug-%d", uid))
		replicaController := tabletserver.NewServer()
		tabletMap[uid] = &tablet{
			keyspace:   keyspace,
			shard:      shard,
			tabletType: topodatapb.TabletType_REPLICA,
			dbname:     dbname,

			qsc:   replicaController,
			agent: tabletmanager.NewComboActionAgent(ctx, ts, alias, int32(8000+uid), int32(9000+uid), replicaController, dbcfgs, mysqld, keyspace, shard, dbname, "replica"),
		}
		uid++

		// create a rdonly slave
		alias = &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		}
		log.Infof("Creating rdonly tablet %v for %v/%v", topoproto.TabletAliasString(alias), keyspace, shard)
		flag.Lookup("debug-url-prefix").Value.Set(fmt.Sprintf("/debug-%d", uid))
		rdonlyController := tabletserver.NewServer()
		tabletMap[uid] = &tablet{
			keyspace:   keyspace,
			shard:      shard,
			tabletType: topodatapb.TabletType_RDONLY,
			dbname:     dbname,

			qsc:   rdonlyController,
			agent: tabletmanager.NewComboActionAgent(ctx, ts, alias, int32(8000+uid), int32(9000+uid), rdonlyController, dbcfgs, mysqld, keyspace, shard, dbname, "rdonly"),
		}
		uid++
	}

	// Rebuild the SrvKeyspace objects, we we can support range-based
	// sharding queries.
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, nil)
	for keyspace := range keyspaceMap {
		if err := wr.RebuildKeyspaceGraph(ctx, keyspace, nil, true); err != nil {
			log.Fatalf("cannot rebuild %v: %v", keyspace, err)
		}
	}

	// Register the tablet dialer for tablet server
	tabletconn.RegisterDialer("internal", dialer)
	*tabletconn.TabletProtocol = "internal"

	// Register the tablet manager client factory for tablet manager
	tmclient.RegisterTabletManagerClientFactory("internal", func() tmclient.TabletManagerClient {
		return &internalTabletManagerClient{}
	})
	*tmclient.TabletManagerProtocol = "internal"
}

//
// TabletConn implementation
//

// dialer is our tabletconn.Dialer
func dialer(ctx context.Context, endPoint *topodatapb.EndPoint, keyspace, shard string, tabletType topodatapb.TabletType, timeout time.Duration) (tabletconn.TabletConn, error) {
	tablet, ok := tabletMap[endPoint.Uid]
	if !ok {
		return nil, tabletconn.OperationalError("connection refused")
	}

	return &internalTabletConn{
		tablet:   tablet,
		endPoint: endPoint,
	}, nil
}

// internalTabletConn implements tabletconn.TabletConn by forwarding everything
// to the tablet
type internalTabletConn struct {
	tablet   *tablet
	endPoint *topodatapb.EndPoint
}

// Execute is part of tabletconn.TabletConn
// We need to copy the bind variables as tablet server will change them.
func (itc *internalTabletConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	bv, err := querytypes.BindVariablesToProto3(bindVars)
	if err != nil {
		return nil, err
	}
	bindVars, err = querytypes.Proto3ToBindVariables(bv)
	if err != nil {
		return nil, err
	}
	reply, err := itc.tablet.qsc.QueryService().Execute(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, query, bindVars, 0, transactionID)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return reply, nil
}

// ExecuteBatch is part of tabletconn.TabletConn
// We need to copy the bind variables as tablet server will change them.
func (itc *internalTabletConn) ExecuteBatch(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	q := make([]querytypes.BoundQuery, len(queries))
	for i, query := range queries {
		bv, err := querytypes.BindVariablesToProto3(query.BindVariables)
		if err != nil {
			return nil, err
		}
		bindVars, err := querytypes.Proto3ToBindVariables(bv)
		if err != nil {
			return nil, err
		}
		q[i].Sql = query.Sql
		q[i].BindVariables = bindVars
	}
	results, err := itc.tablet.qsc.QueryService().ExecuteBatch(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, q, 0, asTransaction, transactionID)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return results, nil
}

// StreamExecute is part of tabletconn.TabletConn
// We need to copy the bind variables as tablet server will change them.
func (itc *internalTabletConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc, error) {
	bv, err := querytypes.BindVariablesToProto3(bindVars)
	if err != nil {
		return nil, nil, err
	}
	bindVars, err = querytypes.Proto3ToBindVariables(bv)
	if err != nil {
		return nil, nil, err
	}
	result := make(chan *sqltypes.Result, 10)
	var finalErr error

	go func() {
		finalErr = itc.tablet.qsc.QueryService().StreamExecute(ctx, &querypb.Target{
			Keyspace:   itc.tablet.keyspace,
			Shard:      itc.tablet.shard,
			TabletType: itc.tablet.tabletType,
		}, query, bindVars, 0, func(reply *sqltypes.Result) error {
			// We need to deep-copy the reply before returning,
			// because the underlying buffers are reused.
			result <- reply.Copy()
			return nil
		})

		// the client will only access finalErr after the
		// channel is closed, and then it's already set.
		close(result)
	}()

	return result, func() error {
		return tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(finalErr))
	}, nil
}

// Begin is part of tabletconn.TabletConn
func (itc *internalTabletConn) Begin(ctx context.Context) (int64, error) {
	transactionID, err := itc.tablet.qsc.QueryService().Begin(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, 0)
	if err != nil {
		return 0, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return transactionID, nil
}

// Commit is part of tabletconn.TabletConn
func (itc *internalTabletConn) Commit(ctx context.Context, transactionID int64) error {
	err := itc.tablet.qsc.QueryService().Commit(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, 0, transactionID)
	return tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
}

// Rollback is part of tabletconn.TabletConn
func (itc *internalTabletConn) Rollback(ctx context.Context, transactionID int64) error {
	err := itc.tablet.qsc.QueryService().Rollback(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, 0, transactionID)
	return tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
}

// Close is part of tabletconn.TabletConn
func (itc *internalTabletConn) Close() {
}

// SetTarget is part of tabletconn.TabletConn
func (itc *internalTabletConn) SetTarget(keyspace, shard string, tabletType topodatapb.TabletType) error {
	return nil
}

// EndPoint is part of tabletconn.TabletConn
func (itc *internalTabletConn) EndPoint() *topodatapb.EndPoint {
	return itc.endPoint
}

// SplitQuery is part of tabletconn.TabletConn
func (itc *internalTabletConn) SplitQuery(ctx context.Context, query querytypes.BoundQuery, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error) {
	splits, err := itc.tablet.qsc.QueryService().SplitQuery(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, query.Sql, query.BindVariables, splitColumn, splitCount, 0)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return splits, nil
}

type streamHealthReader struct {
	c       <-chan *querypb.StreamHealthResponse
	errFunc tabletconn.ErrFunc
}

// Recv implements tabletconn.StreamHealthReader.
// It returns one response from the chan.
func (r *streamHealthReader) Recv() (*querypb.StreamHealthResponse, error) {
	resp, ok := <-r.c
	if !ok {
		return nil, r.errFunc()
	}
	return resp, nil
}

// StreamHealth is part of tabletconn.TabletConn
func (itc *internalTabletConn) StreamHealth(ctx context.Context) (tabletconn.StreamHealthReader, error) {
	result := make(chan *querypb.StreamHealthResponse, 10)

	id, err := itc.tablet.qsc.QueryService().StreamHealthRegister(result)
	if err != nil {
		return nil, err
	}

	var finalErr error
	go func() {
		select {
		case <-ctx.Done():
		}

		finalErr = itc.tablet.qsc.QueryService().StreamHealthUnregister(id)
		close(result)
	}()

	errFunc := func() error {
		return tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(finalErr))
	}

	return &streamHealthReader{
		c:       result,
		errFunc: errFunc,
	}, nil
}

//
// TabletManagerClient implementation
//

// internalTabletManagerClient implements tmclient.TabletManagerClient
type internalTabletManagerClient struct{}

func (itmc *internalTabletManagerClient) Ping(ctx context.Context, tablet *topo.TabletInfo) error {
	t, ok := tabletMap[tablet.Tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Tablet.Alias.Uid)
	}
	return t.agent.RPCWrap(ctx, actionnode.TabletActionPing, nil, nil, func() error {
		t.agent.Ping(ctx, "payload")
		return nil
	})
}

func (itmc *internalTabletManagerClient) GetSchema(ctx context.Context, tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	t, ok := tabletMap[tablet.Tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Tablet.Alias.Uid)
	}
	var result *tabletmanagerdatapb.SchemaDefinition
	if err := t.agent.RPCWrap(ctx, actionnode.TabletActionGetSchema, nil, nil, func() error {
		sd, err := t.agent.GetSchema(ctx, tables, excludeTables, includeViews)
		if err == nil {
			result = sd
		}
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (itmc *internalTabletManagerClient) GetPermissions(ctx context.Context, tablet *topo.TabletInfo) (*tabletmanagerdatapb.Permissions, error) {
	t, ok := tabletMap[tablet.Tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Tablet.Alias.Uid)
	}
	var result *tabletmanagerdatapb.Permissions
	if err := t.agent.RPCWrap(ctx, actionnode.TabletActionGetPermissions, nil, nil, func() error {
		p, err := t.agent.GetPermissions(ctx)
		if err == nil {
			result = p
		}
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (itmc *internalTabletManagerClient) SetReadOnly(ctx context.Context, tablet *topo.TabletInfo) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SetReadWrite(ctx context.Context, tablet *topo.TabletInfo) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ChangeType(ctx context.Context, tablet *topo.TabletInfo, dbType topodatapb.TabletType) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) Sleep(ctx context.Context, tablet *topo.TabletInfo, duration time.Duration) error {
	t, ok := tabletMap[tablet.Tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Tablet.Alias.Uid)
	}
	return t.agent.RPCWrapLockAction(ctx, actionnode.TabletActionSleep, nil, nil, true, func() error {
		t.agent.Sleep(ctx, duration)
		return nil
	})
}

func (itmc *internalTabletManagerClient) ExecuteHook(ctx context.Context, tablet *topo.TabletInfo, hk *hook.Hook) (*hook.HookResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) RefreshState(ctx context.Context, tablet *topo.TabletInfo) error {
	t, ok := tabletMap[tablet.Tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Tablet.Alias.Uid)
	}
	return t.agent.RPCWrapLockAction(ctx, actionnode.TabletActionRefreshState, nil, nil, true, func() error {
		t.agent.RefreshState(ctx)
		return nil
	})
}

func (itmc *internalTabletManagerClient) RunHealthCheck(ctx context.Context, tablet *topo.TabletInfo, targetTabletType topodatapb.TabletType) error {
	t, ok := tabletMap[tablet.Tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Tablet.Alias.Uid)
	}
	return t.agent.RPCWrap(ctx, actionnode.TabletActionRunHealthCheck, nil, nil, func() error {
		t.agent.RunHealthCheck(ctx, targetTabletType)
		return nil
	})
}

func (itmc *internalTabletManagerClient) IgnoreHealthError(ctx context.Context, tablet *topo.TabletInfo, pattern string) error {
	t, ok := tabletMap[tablet.Tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Tablet.Alias.Uid)
	}
	return t.agent.RPCWrap(ctx, actionnode.TabletActionIgnoreHealthError, nil, nil, func() error {
		t.agent.IgnoreHealthError(ctx, pattern)
		return nil
	})
}

func (itmc *internalTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topo.TabletInfo) error {
	t, ok := tabletMap[tablet.Tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Tablet.Alias.Uid)
	}
	return t.agent.RPCWrapLockAction(ctx, actionnode.TabletActionReloadSchema, nil, nil, true, func() error {
		t.agent.ReloadSchema(ctx)
		return nil
	})
}

func (itmc *internalTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topo.TabletInfo, change string) (*tmutils.SchemaChangeResult, error) {
	t, ok := tabletMap[tablet.Tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Tablet.Alias.Uid)
	}
	var result *tmutils.SchemaChangeResult
	if err := t.agent.RPCWrapLockAction(ctx, actionnode.TabletActionPreflightSchema, nil, nil, true, func() error {
		scr, err := t.agent.PreflightSchema(ctx, change)
		if err == nil {
			result = scr
		}
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (itmc *internalTabletManagerClient) ApplySchema(ctx context.Context, tablet *topo.TabletInfo, change *tmutils.SchemaChange) (*tmutils.SchemaChangeResult, error) {
	t, ok := tabletMap[tablet.Tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Tablet.Alias.Uid)
	}
	var result *tmutils.SchemaChangeResult
	if err := t.agent.RPCWrapLockAction(ctx, actionnode.TabletActionApplySchema, nil, nil, true, func() error {
		scr, err := t.agent.ApplySchema(ctx, change)
		if err == nil {
			result = scr
		}
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (itmc *internalTabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int, disableBinlogs, reloadSchema bool) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ExecuteFetchAsApp(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SlaveStatus(ctx context.Context, tablet *topo.TabletInfo) (*replicationdatapb.Status, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) MasterPosition(ctx context.Context, tablet *topo.TabletInfo) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopSlaveMinimum(ctx context.Context, tablet *topo.TabletInfo, stopPos string, waitTime time.Duration) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StartSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) TabletExternallyReparented(ctx context.Context, tablet *topo.TabletInfo, externalID string) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) GetSlaves(ctx context.Context, tablet *topo.TabletInfo) ([]string, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) WaitBlpPosition(ctx context.Context, tablet *topo.TabletInfo, blpPosition *tabletmanagerdatapb.BlpPosition, waitTime time.Duration) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopBlp(ctx context.Context, tablet *topo.TabletInfo) ([]*tabletmanagerdatapb.BlpPosition, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StartBlp(ctx context.Context, tablet *topo.TabletInfo) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) RunBlpUntil(ctx context.Context, tablet *topo.TabletInfo, positions []*tabletmanagerdatapb.BlpPosition, waitTime time.Duration) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ResetReplication(ctx context.Context, tablet *topo.TabletInfo) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) InitMaster(ctx context.Context, tablet *topo.TabletInfo) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PopulateReparentJournal(ctx context.Context, tablet *topo.TabletInfo, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, pos string) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) InitSlave(ctx context.Context, tablet *topo.TabletInfo, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) DemoteMaster(ctx context.Context, tablet *topo.TabletInfo) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PromoteSlaveWhenCaughtUp(ctx context.Context, tablet *topo.TabletInfo, pos string) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SlaveWasPromoted(ctx context.Context, tablet *topo.TabletInfo) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SetMaster(ctx context.Context, tablet *topo.TabletInfo, parent *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SlaveWasRestarted(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopReplicationAndGetStatus(ctx context.Context, tablet *topo.TabletInfo) (*replicationdatapb.Status, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PromoteSlave(ctx context.Context, tablet *topo.TabletInfo) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) Backup(ctx context.Context, tablet *topo.TabletInfo, concurrency int) (<-chan *logutilpb.Event, tmclient.ErrFunc, error) {
	return nil, nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) IsTimeoutError(err error) bool {
	return false
}
