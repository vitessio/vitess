package main

import (
	"flag"
	"fmt"
	"io"
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

// initTabletMap creates the action agents and associated data structures
// for all tablets
func initTabletMap(ts topo.Server, topology string, mysqld mysqlctl.MysqlDaemon, dbcfgs dbconfigs.DBConfigs, formal *vindexes.VSchemaFormal, mycnf *mysqlctl.Mycnf) {
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
			vs := formal.Keyspaces[keyspace]
			if err := ts.SaveVSchema(ctx, keyspace, &vs); err != nil {
				log.Fatalf("SaveVSchema failed: %v", err)
			}
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
		if err := wr.RebuildKeyspaceGraph(ctx, keyspace, nil); err != nil {
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

	// run healthcheck on all vttablets
	tmc := tmclient.NewTabletManagerClient()
	for _, tablet := range tabletMap {
		tabletInfo, err := ts.GetTablet(ctx, tablet.agent.TabletAlias)
		if err != nil {
			log.Fatalf("cannot find tablet: %+v", tablet.agent.TabletAlias)
		}
		tmc.RunHealthCheck(ctx, tabletInfo.Tablet)
	}
}

// initTabletMapProto creates the action agents and associated data structures
// for all tablets, based on the vttest proto parameter.
func initTabletMapProto(ts topo.Server, topoProto string, mysqld mysqlctl.MysqlDaemon, dbcfgs dbconfigs.DBConfigs, formal *vindexes.VSchemaFormal, mycnf *mysqlctl.Mycnf) {
	// parse the input topology
	var tpb *vttestpb.VTTestTopology
	if err := proto.UnmarshalText(topoProto, tpb); err != nil {
		log.Fatalf("cannot parse topology: %v", err)
	}

	tabletMap = make(map[uint32]*tablet)

	ctx := context.Background()

	// disable publishing of stats from query service
	flag.Lookup("queryserver-config-enable-publish-stats").Value.Set("false")

	// iterate through the keyspaces
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, nil)
	var uid uint32 = 1
	for _, kpb := range tpb.Keyspaces {
		keyspace := kpb.Name

		// if we have a redirect, create a completely redirected
		// keyspace and move on
		if kpb.ServedFrom != "" {
			if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{
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
				log.Fatalf("CreateKeyspace(%v) failed: %v", keyspace, err)
			}
			continue
		}

		// create a regular keyspace
		sct, err := key.ParseKeyspaceIDType(kpb.ShardingColumnType)
		if err != nil {
			log.Fatalf("parseKeyspaceIDType(%v) failed: %v", kpb.ShardingColumnType, err)
		}
		if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{
			ShardingColumnName: kpb.ShardingColumnName,
			ShardingColumnType: sct,
		}); err != nil {
			log.Fatalf("CreateKeyspace(%v) failed: %v", keyspace, err)
		}

		// iterate through the shards
		for _, spb := range kpb.Shards {
			shard := spb.Name
			dbname := spb.DbName
			dbcfgs.App.DbName = dbname

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

		// Rebuild the SrvKeyspace object, we we can support
		// range-based sharding queries.
		if err := wr.RebuildKeyspaceGraph(ctx, keyspace, nil); err != nil {
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

	// run healthcheck on all vttablets
	tmc := tmclient.NewTabletManagerClient()
	for _, tablet := range tabletMap {
		tabletInfo, err := ts.GetTablet(ctx, tablet.agent.TabletAlias)
		if err != nil {
			log.Fatalf("cannot find tablet: %+v", tablet.agent.TabletAlias)
		}
		tmc.RunHealthCheck(ctx, tabletInfo.Tablet)
	}
}

//
// TabletConn implementation
//

// dialer is our tabletconn.Dialer
func dialer(ctx context.Context, tablet *topodatapb.Tablet, keyspace, shard string, tabletType topodatapb.TabletType, timeout time.Duration) (tabletconn.TabletConn, error) {
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

func (itmc *internalTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet) error {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	return t.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionReloadSchema, nil, nil, true, func() error {
		t.agent.ReloadSchema(ctx)
		return nil
	})
}

func (itmc *internalTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topodatapb.Tablet, change string) (*tmutils.SchemaChangeResult, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	var result *tmutils.SchemaChangeResult
	if err := t.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionPreflightSchema, nil, nil, true, func() error {
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

func (itmc *internalTabletManagerClient) ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tmutils.SchemaChangeResult, error) {
	t, ok := tabletMap[tablet.Alias.Uid]
	if !ok {
		return nil, fmt.Errorf("tmclient: cannot find tablet %v", tablet.Alias.Uid)
	}
	var result *tmutils.SchemaChangeResult
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

func (itmc *internalTabletManagerClient) IsTimeoutError(err error) bool {
	return false
}
