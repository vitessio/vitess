package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletserver"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	pbq "github.com/youtube/vitess/go/vt/proto/query"
	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// tablet contains all the data for an individual tablet.
type tablet struct {
	// configuration parameters
	keyspace   string
	shard      string
	tabletType pb.TabletType
	dbname     string

	// objects built at construction time
	qsc   tabletserver.QueryServiceControl
	agent *tabletmanager.ActionAgent
}

// tabletMap maps the tablet uid to the tablet record
var tabletMap map[uint32]*tablet

// initTabletMap creates the action agents and associated data structures
// for all tablets
func initTabletMap(ts topo.Server, topology string, mysqld mysqlctl.MysqlDaemon, dbcfgs *dbconfigs.DBConfigs, mycnf *mysqlctl.Mycnf) {
	tabletMap = make(map[uint32]*tablet)

	ctx := context.Background()

	// disable publishing of stats from query service
	flag.Lookup("queryserver-config-enable-publish-stats").Value.Set("false")

	var uid uint32 = 1
	for _, entry := range strings.Split(topology, ",") {
		slash := strings.IndexByte(entry, '/')
		column := strings.IndexByte(entry, ':')
		if slash == -1 || column == -1 {
			log.Fatalf("invalid topology entry: %v", entry)
		}

		keyspace := entry[:slash]
		shard := entry[slash+1 : column]
		dbname := entry[column+1:]

		// create the master
		alias := &pb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		}
		log.Infof("Creating master tablet %v for %v/%v", topoproto.TabletAliasString(alias), keyspace, shard)
		flag.Lookup("debug-url-prefix").Value.Set(fmt.Sprintf("/debug-%d", uid))
		masterQueryServiceControl := tabletserver.NewQueryServiceControl()
		masterAgent := tabletmanager.NewComboActionAgent(ctx, ts, alias, int32(8000+uid), int32(9000+uid), masterQueryServiceControl, dbcfgs, mysqld, keyspace, shard, dbname, "replica")
		if err := masterAgent.TabletExternallyReparented(ctx, ""); err != nil {
			log.Fatalf("TabletExternallyReparented failed on master: %v", err)
		}
		tabletMap[uid] = &tablet{
			keyspace:   keyspace,
			shard:      shard,
			tabletType: pb.TabletType_MASTER,
			dbname:     dbname,

			qsc:   masterQueryServiceControl,
			agent: masterAgent,
		}
		uid++

		// create a replica slave
		alias = &pb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		}
		log.Infof("Creating replica tablet %v for %v/%v", topoproto.TabletAliasString(alias), keyspace, shard)
		flag.Lookup("debug-url-prefix").Value.Set(fmt.Sprintf("/debug-%d", uid))
		replicaQueryServiceControl := tabletserver.NewQueryServiceControl()
		tabletMap[uid] = &tablet{
			keyspace:   keyspace,
			shard:      shard,
			tabletType: pb.TabletType_REPLICA,
			dbname:     dbname,

			qsc:   replicaQueryServiceControl,
			agent: tabletmanager.NewComboActionAgent(ctx, ts, alias, int32(8000+uid), int32(9000+uid), replicaQueryServiceControl, dbcfgs, mysqld, keyspace, shard, dbname, "replica"),
		}
		uid++

		// create a rdonly slave
		alias = &pb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		}
		log.Infof("Creating rdonly tablet %v for %v/%v", topoproto.TabletAliasString(alias), keyspace, shard)
		flag.Lookup("debug-url-prefix").Value.Set(fmt.Sprintf("/debug-%d", uid))
		rdonlyQueryServiceControl := tabletserver.NewQueryServiceControl()
		tabletMap[uid] = &tablet{
			keyspace:   keyspace,
			shard:      shard,
			tabletType: pb.TabletType_RDONLY,
			dbname:     dbname,

			qsc:   rdonlyQueryServiceControl,
			agent: tabletmanager.NewComboActionAgent(ctx, ts, alias, int32(8000+uid), int32(9000+uid), rdonlyQueryServiceControl, dbcfgs, mysqld, keyspace, shard, dbname, "rdonly"),
		}
		uid++
	}

	// Register the tablet dialer
	tabletconn.RegisterDialer("internal", dialer)
	*tabletconn.TabletProtocol = "internal"
}

// dialer is our tabletconn.Dialer
func dialer(ctx context.Context, endPoint *pb.EndPoint, keyspace, shard string, tabletType pb.TabletType, timeout time.Duration) (tabletconn.TabletConn, error) {
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
	endPoint *pb.EndPoint
}

// Execute is part of tabletconn.TabletConn,
func (itc *internalTabletConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*mproto.QueryResult, error) {
	reply := &mproto.QueryResult{}
	if err := itc.tablet.qsc.QueryService().Execute(ctx, &pbq.Target{
		Keyspace:   itc.tablet.keyspace,
		Shard:      itc.tablet.shard,
		TabletType: itc.tablet.tabletType,
	}, &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: transactionID,
	}, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// ExecuteBatch is part of tabletconn.TabletConn,
func (itc *internalTabletConn) ExecuteBatch(ctx context.Context, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) (*tproto.QueryResultList, error) {
	return nil, nil
}

// StreamExecute is part of tabletconn.TabletConn,
func (itc *internalTabletConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc, error) {
	return nil, nil, nil
}

// Begin is part of tabletconn.TabletConn,
func (itc *internalTabletConn) Begin(ctx context.Context) (transactionID int64, err error) {
	return 0, nil
}

// Commit is part of tabletconn.TabletConn,
func (itc *internalTabletConn) Commit(ctx context.Context, transactionID int64) error {
	return nil
}

// Rollback is part of tabletconn.TabletConn,
func (itc *internalTabletConn) Rollback(ctx context.Context, transactionID int64) error {
	return nil
}

// Execute2 is part of tabletconn.TabletConn,
func (itc *internalTabletConn) Execute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*mproto.QueryResult, error) {
	return itc.Execute(ctx, query, bindVars, transactionID)
}

// ExecuteBatch2 is part of tabletconn.TabletConn,
func (itc *internalTabletConn) ExecuteBatch2(ctx context.Context, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) (*tproto.QueryResultList, error) {
	return itc.ExecuteBatch(ctx, queries, asTransaction, transactionID)
}

// Begin2 is part of tabletconn.TabletConn,
func (itc *internalTabletConn) Begin2(ctx context.Context) (transactionID int64, err error) {
	return itc.Begin(ctx)
}

// Commit2 is part of tabletconn.TabletConn,
func (itc *internalTabletConn) Commit2(ctx context.Context, transactionID int64) error {
	return itc.Commit(ctx, transactionID)
}

// Rollback2 is part of tabletconn.TabletConn,
func (itc *internalTabletConn) Rollback2(ctx context.Context, transactionID int64) error {
	return itc.Rollback(ctx, transactionID)
}

// StreamExecute2 is part of tabletconn.TabletConn,
func (itc *internalTabletConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc, error) {
	return itc.StreamExecute(ctx, query, bindVars, transactionID)
}

// Close is part of tabletconn.TabletConn,
func (itc *internalTabletConn) Close() {
}

// SetTarget is part of tabletconn.TabletConn,
func (itc *internalTabletConn) SetTarget(keyspace, shard string, tabletType pb.TabletType) error {
	return nil
}

// EndPoint is part of tabletconn.TabletConn,
func (itc *internalTabletConn) EndPoint() *pb.EndPoint {
	return itc.endPoint
}

// SplitQuery is part of tabletconn.TabletConn,
func (itc *internalTabletConn) SplitQuery(ctx context.Context, query tproto.BoundQuery, splitColumn string, splitCount int) ([]tproto.QuerySplit, error) {
	return nil, nil
}

// StreamHealth is part of tabletconn.TabletConn,
func (itc *internalTabletConn) StreamHealth(ctx context.Context) (<-chan *pbq.StreamHealthResponse, tabletconn.ErrFunc, error) {
	return nil, nil, nil
}
