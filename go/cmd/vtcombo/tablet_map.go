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
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletserver"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
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

	// Register the tablet dialer
	tabletconn.RegisterDialer("internal", dialer)
	*tabletconn.TabletProtocol = "internal"
}

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
	bv, err := tproto.BindVariablesToProto3(bindVars)
	if err != nil {
		return nil, err
	}
	bindVars, err = tproto.Proto3ToBindVariables(bv)
	if err != nil {
		return nil, err
	}
	reply := &sqltypes.Result{}
	if err := itc.tablet.qsc.QueryService().Execute(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: transactionID,
	}, reply); err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return reply, nil
}

// ExecuteBatch is part of tabletconn.TabletConn
// We need to copy the bind variables as tablet server will change them.
func (itc *internalTabletConn) ExecuteBatch(ctx context.Context, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	q := make([]tproto.BoundQuery, len(queries))
	for i, query := range queries {
		bv, err := tproto.BindVariablesToProto3(query.BindVariables)
		if err != nil {
			return nil, err
		}
		bindVars, err := tproto.Proto3ToBindVariables(bv)
		if err != nil {
			return nil, err
		}
		q[i].Sql = query.Sql
		q[i].BindVariables = bindVars
	}
	reply := &tproto.QueryResultList{}
	if err := itc.tablet.qsc.QueryService().ExecuteBatch(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, &tproto.QueryList{
		Queries:       q,
		AsTransaction: asTransaction,
		TransactionId: transactionID,
	}, reply); err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return reply.List, nil
}

// StreamExecute is part of tabletconn.TabletConn
// We need to copy the bind variables as tablet server will change them.
func (itc *internalTabletConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc, error) {
	bv, err := tproto.BindVariablesToProto3(bindVars)
	if err != nil {
		return nil, nil, err
	}
	bindVars, err = tproto.Proto3ToBindVariables(bv)
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
		}, &tproto.Query{
			Sql:           query,
			BindVariables: bindVars,
			TransactionId: transactionID,
		}, func(reply *sqltypes.Result) error {
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
func (itc *internalTabletConn) Begin(ctx context.Context) (transactionID int64, err error) {
	result := &tproto.TransactionInfo{}
	if err := itc.tablet.qsc.QueryService().Begin(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, &tproto.Session{}, result); err != nil {
		return 0, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return result.TransactionId, nil
}

// Commit is part of tabletconn.TabletConn
func (itc *internalTabletConn) Commit(ctx context.Context, transactionID int64) error {
	err := itc.tablet.qsc.QueryService().Commit(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, &tproto.Session{
		TransactionId: transactionID,
	})
	return tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
}

// Rollback is part of tabletconn.TabletConn
func (itc *internalTabletConn) Rollback(ctx context.Context, transactionID int64) error {
	err := itc.tablet.qsc.QueryService().Rollback(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, &tproto.Session{
		TransactionId: transactionID,
	})
	return tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
}

// Execute2 is part of tabletconn.TabletConn
func (itc *internalTabletConn) Execute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	return itc.Execute(ctx, query, bindVars, transactionID)
}

// ExecuteBatch2 is part of tabletconn.TabletConn
func (itc *internalTabletConn) ExecuteBatch2(ctx context.Context, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	return itc.ExecuteBatch(ctx, queries, asTransaction, transactionID)
}

// Begin2 is part of tabletconn.TabletConn
func (itc *internalTabletConn) Begin2(ctx context.Context) (transactionID int64, err error) {
	return itc.Begin(ctx)
}

// Commit2 is part of tabletconn.TabletConn
func (itc *internalTabletConn) Commit2(ctx context.Context, transactionID int64) error {
	return itc.Commit(ctx, transactionID)
}

// Rollback2 is part of tabletconn.TabletConn
func (itc *internalTabletConn) Rollback2(ctx context.Context, transactionID int64) error {
	return itc.Rollback(ctx, transactionID)
}

// StreamExecute2 is part of tabletconn.TabletConn
func (itc *internalTabletConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc, error) {
	return itc.StreamExecute(ctx, query, bindVars, transactionID)
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
func (itc *internalTabletConn) SplitQuery(ctx context.Context, query tproto.BoundQuery, splitColumn string, splitCount int) ([]tproto.QuerySplit, error) {
	reply := &tproto.SplitQueryResult{}
	if err := itc.tablet.qsc.QueryService().SplitQuery(ctx, &querypb.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, &tproto.SplitQueryRequest{
		Query:       query,
		SplitColumn: splitColumn,
		SplitCount:  splitCount,
	}, reply); err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(err))
	}
	return reply.Queries, nil
}

// StreamHealth is part of tabletconn.TabletConn
func (itc *internalTabletConn) StreamHealth(ctx context.Context) (<-chan *querypb.StreamHealthResponse, tabletconn.ErrFunc, error) {
	result := make(chan *querypb.StreamHealthResponse, 10)

	id, err := itc.tablet.qsc.QueryService().StreamHealthRegister(result)
	if err != nil {
		return nil, nil, err
	}

	var finalErr error
	go func() {
		select {
		case <-ctx.Done():
		}

		finalErr = itc.tablet.qsc.QueryService().StreamHealthUnregister(id)
		close(result)
	}()

	return result, func() error {
		return tabletconn.TabletErrorFromGRPC(tabletserver.ToGRPCError(finalErr))
	}, nil
}
