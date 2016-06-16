package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/wrangler"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vttestpb "github.com/youtube/vitess/go/vt/proto/vttest"
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

// createTablet creates an individual tablet, with its agent, and adds
// it to the map. If it's a master tablet, it also issues a TER.
func createTablet(ctx context.Context, ts topo.Server, cell string, uid uint32, keyspace, shard, dbname string, tabletType topodatapb.TabletType, mysqld mysqlctl.MysqlDaemon, dbcfgs dbconfigs.DBConfigs) error {
	alias := &topodatapb.TabletAlias{
		Cell: cell,
		Uid:  uid,
	}
	log.Infof("Creating %v tablet %v for %v/%v", tabletType, topoproto.TabletAliasString(alias), keyspace, shard)
	flag.Set("debug-url-prefix", fmt.Sprintf("/debug-%d", uid))

	controller := tabletserver.NewServer()
	initTabletType := tabletType
	if tabletType == topodatapb.TabletType_MASTER {
		initTabletType = topodatapb.TabletType_REPLICA
	}
	agent := tabletmanager.NewComboActionAgent(ctx, ts, alias, int32(8000+uid), int32(9000+uid), controller, dbcfgs, mysqld, keyspace, shard, dbname, strings.ToLower(initTabletType.String()))
	if tabletType == topodatapb.TabletType_MASTER {
		if err := agent.TabletExternallyReparented(ctx, ""); err != nil {
			return fmt.Errorf("TabletExternallyReparented failed on master %v: %v", topoproto.TabletAliasString(alias), err)
		}
	}
	tabletMap[uid] = &tablet{
		keyspace:   keyspace,
		shard:      shard,
		tabletType: tabletType,
		dbname:     dbname,

		qsc:   controller,
		agent: agent,
	}
	return nil
}

// initTabletMap creates the action agents and associated data structures
// for all tablets, based on the vttest proto parameter.
func initTabletMap(ts topo.Server, topoProto string, mysqld mysqlctl.MysqlDaemon, dbcfgs dbconfigs.DBConfigs, schemaDir string, mycnf *mysqlctl.Mycnf) error {
	// parse the input topology
	tpb := &vttestpb.VTTestTopology{}
	if err := proto.UnmarshalText(topoProto, tpb); err != nil {
		return fmt.Errorf("cannot parse topology: %v", err)
	}

	tabletMap = make(map[uint32]*tablet)

	ctx := context.Background()

	// disable publishing of stats from query service
	flag.Set("queryserver-config-enable-publish-stats", "false")

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
				dbname := spb.DbNameOverride
				if dbname == "" {
					dbname = fmt.Sprintf("vt_%v_%v", keyspace, shard)
				}
				dbcfgs.App.DbName = dbname

				// create the master
				if err := createTablet(ctx, ts, cell, uid, keyspace, shard, dbname, topodatapb.TabletType_MASTER, mysqld, dbcfgs); err != nil {
					return err
				}
				uid++

				// create a replica slave
				if err := createTablet(ctx, ts, cell, uid, keyspace, shard, dbname, topodatapb.TabletType_REPLICA, mysqld, dbcfgs); err != nil {
					return err
				}
				uid++

				// create a rdonly slave
				if err := createTablet(ctx, ts, cell, uid, keyspace, shard, dbname, topodatapb.TabletType_RDONLY, mysqld, dbcfgs); err != nil {
					return err
				}
				uid++
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
	if err := topotools.RebuildVSchema(ctx, wr.Logger(), ts, []string{cell}); err != nil {
		return fmt.Errorf("RebuildVSchemaGraph failed: %v", err)
	}

	// Register the tablet dialer for tablet server
	tabletconn.RegisterDialer("internal", dialer)
	*tabletconn.TabletProtocol = "internal"

	// Register the tablet manager client factory for tablet manager
	tmclient.RegisterTabletManagerClientFactory("internal", func() tmclient.TabletManagerClient {
		return &internalTabletManagerClient{}
	})
	*tmclient.TabletManagerProtocol = "internal"

	// run healthcheck on all vttablets
	tmc := tmclient.NewTabletManagerClient()
	for _, tablet := range tabletMap {
		tabletInfo, err := ts.GetTablet(ctx, tablet.agent.TabletAlias)
		if err != nil {
			return fmt.Errorf("cannot find tablet: %+v", tablet.agent.TabletAlias)
		}
		tmc.RunHealthCheck(ctx, tabletInfo.Tablet)
	}

	return nil
}

//
// TabletConn implementation
//

// dialer is our tabletconn.Dialer
func dialer(ctx context.Context, tablet *topodatapb.Tablet, timeout time.Duration) (tabletconn.TabletConn, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, tabletconn.OperationalError("connection refused")
	}

	return &internalTabletConn{
		tablet:     t,
		topoTablet: tablet,
	}, nil
}

// internalTabletConn implements tabletconn.TabletConn by forwarding everything
// to the tablet
type internalTabletConn struct {
	tablet     *tablet
	topoTablet *topodatapb.Tablet
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
	}, query, bindVars, transactionID)
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
	}, q, asTransaction, transactionID)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return results, nil
}

type streamExecuteAdapter struct {
	c   chan *sqltypes.Result
	err *error
}

func (a *streamExecuteAdapter) Recv() (*sqltypes.Result, error) {
	r, ok := <-a.c
	if !ok {
		if *a.err == nil {
			return nil, io.EOF
		}
		return nil, *a.err
	}
	return r, nil
}

// StreamExecute is part of tabletconn.TabletConn
// We need to copy the bind variables as tablet server will change them.
func (itc *internalTabletConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}) (sqltypes.ResultStream, error) {
	bv, err := querytypes.BindVariablesToProto3(bindVars)
	if err != nil {
		return nil, err
	}
	bindVars, err = querytypes.Proto3ToBindVariables(bv)
	if err != nil {
		return nil, err
	}
	result := make(chan *sqltypes.Result, 10)
	var finalErr error

	go func() {
		finalErr = itc.tablet.qsc.QueryService().StreamExecute(ctx, &querypb.Target{
			Keyspace:   itc.tablet.keyspace,
			Shard:      itc.tablet.shard,
			TabletType: itc.tablet.tabletType,
		}, query, bindVars, func(reply *sqltypes.Result) error {
			// We need to deep-copy the reply before returning,
			// because the underlying buffers are reused.
			result <- reply.Copy()
			return nil
		})

		// the client will only access finalErr after the
		// channel is closed, and then it's already set.
		close(result)
	}()

	return &streamExecuteAdapter{result, &finalErr}, nil
}

// Begin is part of tabletconn.TabletConn
func (itc *internalTabletConn) Begin(ctx context.Context) (int64, error) {
	transactionID, err := itc.tablet.qsc.QueryService().Begin(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	})
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
	}, transactionID)
	return tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
}

// Rollback is part of tabletconn.TabletConn
func (itc *internalTabletConn) Rollback(ctx context.Context, transactionID int64) error {
	err := itc.tablet.qsc.QueryService().Rollback(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, transactionID)
	return tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
}

// BeginExecute is part of tabletconn.TabletConn
func (itc *internalTabletConn) BeginExecute(ctx context.Context, query string, bindVars map[string]interface{}) (*sqltypes.Result, int64, error) {
	transactionID, err := itc.Begin(ctx)
	if err != nil {
		return nil, 0, err
	}
	result, err := itc.Execute(ctx, query, bindVars, transactionID)
	return result, transactionID, err
}

// BeginExecuteBatch is part of tabletconn.TabletConn
func (itc *internalTabletConn) BeginExecuteBatch(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool) ([]sqltypes.Result, int64, error) {
	transactionID, err := itc.Begin(ctx)
	if err != nil {
		return nil, 0, err
	}
	results, err := itc.ExecuteBatch(ctx, queries, asTransaction, transactionID)
	return results, transactionID, err
}

// Close is part of tabletconn.TabletConn
func (itc *internalTabletConn) Close() {
}

// SetTarget is part of tabletconn.TabletConn
func (itc *internalTabletConn) SetTarget(keyspace, shard string, tabletType topodatapb.TabletType) error {
	return nil
}

// Tablet is part of tabletconn.TabletConn
func (itc *internalTabletConn) Tablet() *topodatapb.Tablet {
	return itc.topoTablet
}

// SplitQuery is part of tabletconn.TabletConn
func (itc *internalTabletConn) SplitQuery(ctx context.Context, query querytypes.BoundQuery, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error) {
	splits, err := itc.tablet.qsc.QueryService().SplitQuery(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, query.Sql, query.BindVariables, splitColumn, splitCount)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return splits, nil
}

// SplitQueryV2 is part of tabletconn.TabletConn
// TODO(erez): Rename to SplitQuery once the migration to SplitQuery V2 is done.
func (itc *internalTabletConn) SplitQueryV2(
	ctx context.Context,
	query querytypes.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]querytypes.QuerySplit, error) {

	splits, err := itc.tablet.qsc.QueryService().SplitQueryV2(
		ctx,
		&querypb.Target{
			Keyspace:   itc.tablet.keyspace,
			Shard:      itc.tablet.shard,
			TabletType: itc.tablet.tabletType,
		},
		query.Sql,
		query.BindVariables,
		splitColumns,
		splitCount,
		numRowsPerQueryPart,
		algorithm)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return splits, nil
}

type streamHealthReader struct {
	c   <-chan *querypb.StreamHealthResponse
	err *error
}

// Recv implements tabletconn.StreamHealthReader.
// It returns one response from the chan.
func (r *streamHealthReader) Recv() (*querypb.StreamHealthResponse, error) {
	resp, ok := <-r.c
	if !ok {
		return nil, *r.err
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

		// We populate finalErr before closing the channel.
		// The consumer first waits on the channel closure,
		// then read finalErr
		finalErr = itc.tablet.qsc.QueryService().StreamHealthUnregister(id)
		finalErr = tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(finalErr))
		close(result)
	}()

	return &streamHealthReader{
		c:   result,
		err: &finalErr,
	}, nil
}

//
// TabletManagerClient implementation
//

// internalTabletManagerClient implements tmclient.TabletManagerClient
type internalTabletManagerClient struct{}

func (itmc *internalTabletManagerClient) Ping(ctx context.Context, tablet *topodatapb.Tablet) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.agent.RPCWrap(ctx, tabletmanager.TabletActionPing, nil, nil, func() error {
		t.agent.Ping(ctx, "payload")
		return nil
	})
}

func (itmc *internalTabletManagerClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	var result *tabletmanagerdatapb.SchemaDefinition
	if err := t.agent.RPCWrap(ctx, tabletmanager.TabletActionGetSchema, nil, nil, func() error {
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

func (itmc *internalTabletManagerClient) GetPermissions(ctx context.Context, tablet *topodatapb.Tablet) (*tabletmanagerdatapb.Permissions, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	var result *tabletmanagerdatapb.Permissions
	if err := t.agent.RPCWrap(ctx, tabletmanager.TabletActionGetPermissions, nil, nil, func() error {
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

func (itmc *internalTabletManagerClient) SetReadOnly(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SetReadWrite(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) Sleep(ctx context.Context, tablet *topodatapb.Tablet, duration time.Duration) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionSleep, nil, nil, true, func() error {
		t.agent.Sleep(ctx, duration)
		return nil
	})
}

func (itmc *internalTabletManagerClient) ExecuteHook(ctx context.Context, tablet *topodatapb.Tablet, hk *hook.Hook) (*hook.HookResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionRefreshState, nil, nil, true, func() error {
		t.agent.RefreshState(ctx)
		return nil
	})
}

func (itmc *internalTabletManagerClient) RunHealthCheck(ctx context.Context, tablet *topodatapb.Tablet) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.agent.RPCWrap(ctx, tabletmanager.TabletActionRunHealthCheck, nil, nil, func() error {
		t.agent.RunHealthCheck(ctx)
		return nil
	})
}

func (itmc *internalTabletManagerClient) IgnoreHealthError(ctx context.Context, tablet *topodatapb.Tablet, pattern string) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.agent.RPCWrap(ctx, tabletmanager.TabletActionIgnoreHealthError, nil, nil, func() error {
		t.agent.IgnoreHealthError(ctx, pattern)
		return nil
	})
}

func (itmc *internalTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, waitPosition string) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionReloadSchema, nil, nil, true, func() error {
		return t.agent.ReloadSchema(ctx, waitPosition)
	})
}

func (itmc *internalTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topodatapb.Tablet, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	var results []*tabletmanagerdatapb.SchemaChangeResult
	if err := t.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionPreflightSchema, nil, nil, true, func() error {
		r, err := t.agent.PreflightSchema(ctx, changes)
		if err == nil {
			results = r
		}
		return err
	}); err != nil {
		return nil, err
	}
	return results, nil
}

func (itmc *internalTabletManagerClient) ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	var result *tabletmanagerdatapb.SchemaChangeResult
	if err := t.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionApplySchema, nil, nil, true, func() error {
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

func (itmc *internalTabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, query string, maxRows int, disableBinlogs, reloadSchema bool) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, query string, maxRows int) (*querypb.QueryResult, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SlaveStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) MasterPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopSlave(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopSlaveMinimum(ctx context.Context, tablet *topodatapb.Tablet, stopPos string, waitTime time.Duration) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StartSlave(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) TabletExternallyReparented(ctx context.Context, tablet *topodatapb.Tablet, externalID string) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) GetSlaves(ctx context.Context, tablet *topodatapb.Tablet) ([]string, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) WaitBlpPosition(ctx context.Context, tablet *topodatapb.Tablet, blpPosition *tabletmanagerdatapb.BlpPosition, waitTime time.Duration) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopBlp(ctx context.Context, tablet *topodatapb.Tablet) ([]*tabletmanagerdatapb.BlpPosition, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StartBlp(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) RunBlpUntil(ctx context.Context, tablet *topodatapb.Tablet, positions []*tabletmanagerdatapb.BlpPosition, waitTime time.Duration) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
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

func (itmc *internalTabletManagerClient) InitSlave(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) DemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PromoteSlaveWhenCaughtUp(ctx context.Context, tablet *topodatapb.Tablet, pos string) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SlaveWasPromoted(ctx context.Context, tablet *topodatapb.Tablet) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SetMaster(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) SlaveWasRestarted(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias) error {
	return fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) PromoteSlave(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return "", fmt.Errorf("not implemented in vtcombo")
}

func (itmc *internalTabletManagerClient) Backup(ctx context.Context, tablet *topodatapb.Tablet, concurrency int) (logutil.EventStream, error) {
	return nil, fmt.Errorf("not implemented in vtcombo")
}
