package main

import (
	"flag"
	"fmt"
	"strings"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// tablet contains all the data for an individual tablet.
type tablet struct {
	// configuration parameters

	keyspace string
	shard    string
	dbname   string

	// objects built at construction time
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

		alias := &pb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		}

		flag.Lookup("debug-url-prefix").Value.Set(fmt.Sprintf("/debug-%d", uid))
		masterQueryServiceControl := tabletserver.NewQueryServiceControl()
		masterAgent := tabletmanager.NewComboActionAgent(ctx, ts, alias, int32(8000+uid), int32(9000+uid), masterQueryServiceControl, dbcfgs, mysqld, keyspace, shard, dbname, "replica")
		if err := masterAgent.TabletExternallyReparented(ctx, ""); err != nil {
			log.Fatalf("TabletExternallyReparented failed on master: %v", err)
		}
		tabletMap[uid] = &tablet{
			keyspace: keyspace,
			shard:    shard,
			dbname:   dbname,

			agent: masterAgent,
		}
		uid++

		flag.Lookup("debug-url-prefix").Value.Set(fmt.Sprintf("/debug-%d", uid))
		replicaQueryServiceControl := tabletserver.NewQueryServiceControl()
		tabletMap[uid] = &tablet{
			keyspace: keyspace,
			shard:    shard,
			dbname:   dbname,

			agent: tabletmanager.NewComboActionAgent(ctx, ts, alias, int32(8000+uid), int32(9000+uid), replicaQueryServiceControl, dbcfgs, mysqld, keyspace, shard, dbname, "replica"),
		}
		uid++

		flag.Lookup("debug-url-prefix").Value.Set(fmt.Sprintf("/debug-%d", uid))
		rdonlyQueryServiceControl := tabletserver.NewQueryServiceControl()
		tabletMap[uid] = &tablet{
			keyspace: keyspace,
			shard:    shard,
			dbname:   dbname,

			agent: tabletmanager.NewComboActionAgent(ctx, ts, alias, int32(8000+uid), int32(9000+uid), rdonlyQueryServiceControl, dbcfgs, mysqld, keyspace, shard, dbname, "rdonly"),
		}
		uid++
	}
}
