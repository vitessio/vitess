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

/*
Package tabletmanager exports the TabletManager object. It keeps the local tablet
state, starts / stops all associated services (query service,
update stream, binlog players, ...), and handles tabletmanager RPCs
to update the state.

The tm is responsible for maintaining the tablet record in the
topology server. Only 'vtctl DeleteTablet'
should be run by other processes, everything else should ask
the tablet server to make the change.

Most RPC calls lock the actionMutex, except the easy read-only ones.
RPC calls that change the tablet record will also call updateState.

See rpc_server.go for all cases, and which actions take the actionMutex,
and which run changeCallback.
*/
package tabletmanager

import (
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path"
	"regexp"
	"sync"
	"time"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/dbconnpool"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/health"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// The following flags initialize the tablet record.
	tabletHostname     = flag.String("tablet_hostname", "", "if not empty, this hostname will be assumed instead of trying to resolve it")
	initKeyspace       = flag.String("init_keyspace", "", "(init parameter) keyspace to use for this tablet")
	initShard          = flag.String("init_shard", "", "(init parameter) shard to use for this tablet")
	initTabletType     = flag.String("init_tablet_type", "", "(init parameter) the tablet type to use for this tablet.")
	initDbNameOverride = flag.String("init_db_name_override", "", "(init parameter) override the name of the db used by vttablet. Without this flag, the db name defaults to vt_<keyspacename>")
	initTags           flagutil.StringMapValue

	initPopulateMetadata = flag.Bool("init_populate_metadata", false, "(init parameter) populate metadata tables even if restore_from_backup is disabled. If restore_from_backup is enabled, metadata tables are always populated regardless of this flag.")
	initTimeout          = flag.Duration("init_timeout", 1*time.Minute, "(init parameter) timeout to use for the init phase.")

	// statsTabletType is set to expose the current tablet type.
	statsTabletType *stats.String

	// statsTabletTypeCount exposes the current tablet type as a label,
	// with the value counting the occurrences of the respective tablet type.
	// Useful for Prometheus which doesn't support exporting strings as stat values.
	statsTabletTypeCount *stats.CountersWithSingleLabel

	// statsBackupIsRunning is set to 1 (true) if a backup is running.
	statsBackupIsRunning *stats.GaugesWithMultiLabels

	statsKeyspace      = stats.NewString("TabletKeyspace")
	statsShard         = stats.NewString("TabletShard")
	statsKeyRangeStart = stats.NewString("TabletKeyRangeStart")
	statsKeyRangeEnd   = stats.NewString("TabletKeyRangeEnd")
	statsAlias         = stats.NewString("TabletAlias")

	// The following variables can be changed to speed up tests.
	mysqlPortRetryInterval       = 1 * time.Second
	rebuildKeyspaceRetryInterval = 1 * time.Second

	// demoteMasterType is deprecated.
	// TODO(sougou); remove after release 7.0.
	demoteMasterType = flag.String("demote_master_type", "REPLICA", "DEPRECATED: the tablet type a demoted master will transition to")
)

func init() {
	flag.Var(&initTags, "init_tags", "(init parameter) comma separated list of key:value pairs used to tag the tablet")

	statsTabletType = stats.NewString("TabletType")
	statsTabletTypeCount = stats.NewCountersWithSingleLabel("TabletTypeCount", "Number of times the tablet changed to the labeled type", "type")
	statsBackupIsRunning = stats.NewGaugesWithMultiLabels("BackupIsRunning", "Whether a backup is running", []string{"mode"})
}

// TabletManager is the main class for the tablet manager.
type TabletManager struct {
	// The following fields are set during creation
	BatchCtx            context.Context
	TopoServer          *topo.Server
	Cnf                 *mysqlctl.Mycnf
	MysqlDaemon         mysqlctl.MysqlDaemon
	DBConfigs           *dbconfigs.DBConfigs
	QueryServiceControl tabletserver.Controller
	UpdateStream        binlog.UpdateStreamControl
	VREngine            *vreplication.Engine

	// replManager manages replication.
	replManager *replManager

	// HealthReporter initiates healthchecks.
	HealthReporter health.Reporter

	// tabletAlias is saved away from tablet for read-only access
	tabletAlias *topodatapb.TabletAlias

	// baseTabletType is the tablet type we revert back to
	// when we transition back from something like MASTER.
	baseTabletType topodatapb.TabletType

	// History of the health checks, public so status
	// pages can display it
	History *history.History

	// actionMutex is there to run only one action at a time. If
	// both tm.actionMutex and tm.mutex needs to be taken,
	// take actionMutex first.
	actionMutex sync.Mutex

	// actionMutexLocked is set to true after we acquire actionMutex,
	// and reset to false when we release it.
	// It is meant as a sanity check to make sure the methods that need
	// to have the actionMutex have it.
	actionMutexLocked bool

	// orc is an optional client for Orchestrator HTTP API calls.
	// If this is nil, those calls will be skipped.
	// It's only set once in NewTabletManager() and never modified after that.
	orc *orcClient

	// mutex protects all the following fields (that start with '_'),
	// only hold the mutex to update the fields, nothing else.
	mutex sync.Mutex

	// _shardInfo and _srvKeyspace are cached and refreshed on RefreshState.
	_shardInfo   *topo.ShardInfo
	_srvKeyspace *topodatapb.SrvKeyspace

	// _shardSyncChan is a channel for informing the shard sync goroutine that
	// it should wake up and recheck the tablet state, to make sure it and the
	// shard record are in sync.
	//
	// Call tm.notifyShardSync() instead of sending directly to this channel.
	_shardSyncChan chan struct{}

	// _shardSyncDone is a channel for waiting until the shard sync goroutine
	// has really finished after _shardSyncCancel was called.
	_shardSyncDone chan struct{}

	// _shardSyncCancel is the function to stop the background shard sync goroutine.
	_shardSyncCancel context.CancelFunc

	// _disallowQueryService is set to the reason we should be
	// disallowing queries from being served. It is set from changeCallback,
	// and used by healthcheck. If empty, we should allow queries.
	// It is set if the current type is not serving, if a TabletControl
	// tells us not to serve, or if filtered replication is running.
	_disallowQueryService string

	// _blacklistedTables has the list of tables we are currently
	// blacklisting.
	_blacklistedTables []string

	// if the tm is healthy, this is nil. Otherwise it contains
	// the reason we're not healthy.
	_healthy error

	// this is the last time health check ran
	_healthyTime time.Time

	// replication delay the last time we got it
	_replicationDelay time.Duration

	// _ignoreHealthErrorExpr can be set by RPC to selectively disable certain
	// healthcheck errors. It should only be accessed while holding actionMutex.
	_ignoreHealthErrorExpr *regexp.Regexp

	// _replicationStopped remembers if we've been told to stop replicating.
	// If it's nil, we'll try to check for the replicationStoppedFile.
	_replicationStopped *bool

	// _lockTablesConnection is used to get and release the table read locks to pause replication
	_lockTablesConnection *dbconnpool.DBConnection
	_lockTablesTimer      *time.Timer
	// _isBackupRunning tells us whether there is a backup that is currently running
	_isBackupRunning bool

	pubMu sync.Mutex
	// tablet has the Tablet record we last read from the topology server.
	tablet       *topodatapb.Tablet
	isPublishing bool
}

// InitConfig is a temp function to keep things working during the refactor.
func InitConfig(config *tabletenv.TabletConfig) {
	healthCheckInterval = config.Healthcheck.IntervalSeconds.Get()
	degradedThreshold = config.Healthcheck.DegradedThresholdSeconds.Get()
	unhealthyThreshold = config.Healthcheck.UnhealthyThresholdSeconds.Get()

	enableReplicationReporter = config.ReplicationTracker.Mode == tabletenv.Polling
}

// BuildTabletFromInput builds a tablet record from input parameters.
func BuildTabletFromInput(alias *topodatapb.TabletAlias, port, grpcPort int32) (*topodatapb.Tablet, error) {
	hostname := *tabletHostname
	if hostname == "" {
		var err error
		hostname, err = netutil.FullyQualifiedHostname()
		if err != nil {
			return nil, err
		}
		log.Infof("Using detected machine hostname: %v, to change this, fix your machine network configuration or override it with -tablet_hostname.", hostname)
	} else {
		log.Infof("Using hostname: %v from -tablet_hostname flag.", hostname)
	}

	if *initKeyspace == "" || *initShard == "" {
		return nil, fmt.Errorf("init_keyspace and init_shard must be specified")
	}

	// parse and validate shard name
	shard, keyRange, err := topo.ValidateShardName(*initShard)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot validate shard name %v", *initShard)
	}

	tabletType, err := topoproto.ParseTabletType(*initTabletType)
	if err != nil {
		return nil, err
	}
	switch tabletType {
	case topodatapb.TabletType_SPARE, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
	default:
		return nil, fmt.Errorf("invalid init_tablet_type %v; can only be REPLICA, RDONLY or SPARE", tabletType)
	}

	return &topodatapb.Tablet{
		Alias:    alias,
		Hostname: hostname,
		PortMap: map[string]int32{
			"vt":   port,
			"grpc": grpcPort,
		},
		Keyspace:       *initKeyspace,
		Shard:          shard,
		KeyRange:       keyRange,
		Type:           tabletType,
		DbNameOverride: *initDbNameOverride,
		Tags:           initTags,
	}, nil
}

// Start starts the TabletManager.
func (tm *TabletManager) Start(tablet *topodatapb.Tablet) error {
	tm.setTablet(tablet)
	tm.DBConfigs.DBName = topoproto.TabletDbName(tablet)
	tm.replManager = newReplManager(tm.BatchCtx, tm, healthCheckInterval)
	tm.History = history.New(historyLength)
	tm.tabletAlias = tablet.Alias
	demoteType, err := topoproto.ParseTabletType(*demoteMasterType)
	if err != nil {
		return err
	}
	if demoteType != tablet.Type {
		log.Warningf("deprecated demote_master_type %v must match init_tablet_type %v", demoteType, tablet.Type)
	}
	tm.baseTabletType = tablet.Type
	tm._healthy = fmt.Errorf("healthcheck not run yet")

	ctx, cancel := context.WithTimeout(tm.BatchCtx, *initTimeout)
	defer cancel()
	if err := tm.createKeyspaceShard(ctx); err != nil {
		return err
	}
	if err := tm.checkMastership(ctx); err != nil {
		return err
	}
	if err := tm.checkMysql(ctx); err != nil {
		return err
	}
	if err := tm.initTablet(ctx); err != nil {
		return err
	}

	err = tm.QueryServiceControl.InitDBConfig(querypb.Target{
		Keyspace:   tablet.Keyspace,
		Shard:      tablet.Shard,
		TabletType: tablet.Type,
	}, tm.DBConfigs, tm.MysqlDaemon)
	if err != nil {
		return vterrors.Wrap(err, "failed to InitDBConfig")
	}
	tm.QueryServiceControl.RegisterQueryRuleSource(blacklistQueryRules)

	if tm.UpdateStream != nil {
		tm.UpdateStream.InitDBConfig(tm.DBConfigs)
		servenv.OnRun(tm.UpdateStream.RegisterService)
		servenv.OnTerm(tm.UpdateStream.Disable)
	}

	if tm.VREngine != nil {
		tm.VREngine.InitDBConfig(tm.DBConfigs)
		servenv.OnTerm(tm.VREngine.Close)
	}

	// The following initializations don't need to be done
	// in any specific order.
	tm.startShardSync()
	tm.exportStats()
	orc, err := newOrcClient()
	if err != nil {
		return err
	}
	if orc != nil {
		tm.orc = orc
		go tm.orc.DiscoverLoop(tm)
	}
	servenv.OnRun(tm.registerTabletManager)

	restoring, err := tm.handleRestore(tm.BatchCtx)
	if err != nil {
		return err
	}
	if restoring {
		// If restore was triggered, it will take care
		// of updating the tablet state.
		return nil
	}

	if err := tm.lock(tm.BatchCtx); err != nil {
		return err
	}
	defer tm.unlock()
	tablet = tm.Tablet()
	tm.changeCallback(tm.BatchCtx, tablet, tablet)

	return nil
}

// Close prepares a tablet for shutdown. First we check our tablet ownership and
// then prune the tablet topology entry of all post-init fields. This prevents
// stale identifiers from hanging around in topology.
func (tm *TabletManager) Close() {
	// Stop the shard sync loop and wait for it to exit. We do this in Close()
	// rather than registering it as an OnTerm hook so the shard sync loop keeps
	// running during lame duck.
	tm.stopShardSync()

	// cleanup initialized fields in the tablet entry
	f := func(tablet *topodatapb.Tablet) error {
		if err := topotools.CheckOwnership(tm.Tablet(), tablet); err != nil {
			return err
		}
		tablet.Hostname = ""
		tablet.MysqlHostname = ""
		tablet.PortMap = nil
		return nil
	}

	updateCtx, updateCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
	defer updateCancel()

	if _, err := tm.TopoServer.UpdateTabletFields(updateCtx, tm.tabletAlias, f); err != nil {
		log.Warningf("Failed to update tablet record, may contain stale identifiers: %v", err)
	}
}

// Stop shuts down the tm. Normally this is not necessary, since we use
// servenv OnTerm and OnClose hooks to coordinate shutdown automatically,
// while taking lameduck into account. However, this may be useful for tests,
// when you want to clean up an tm immediately.
func (tm *TabletManager) Stop() {
	// Stop the shard sync loop and wait for it to exit. This needs to be done
	// here in addition to in Close() because tests do not call Close().
	tm.stopShardSync()

	if tm.UpdateStream != nil {
		tm.UpdateStream.Disable()
	}

	if tm.VREngine != nil {
		tm.VREngine.Close()
	}

	tm.MysqlDaemon.Close()
}

func (tm *TabletManager) createKeyspaceShard(ctx context.Context) error {
	// mutex is needed because we set _shardInfo and _srvKeyspace
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tablet := tm.Tablet()
	log.Infof("Reading/creating keyspace and shard records for %v/%v", tablet.Keyspace, tablet.Shard)

	// Read the shard, create it if necessary.
	if err := tm.withRetry(ctx, "creating keyspace and shard", func() error {
		var err error
		tm._shardInfo, err = tm.TopoServer.GetOrCreateShard(ctx, tablet.Keyspace, tablet.Shard)
		return err
	}); err != nil {
		return vterrors.Wrap(err, "createKeyspaceShard: cannot GetOrCreateShard shard")
	}

	// Rebuild keyspace if this the first tablet in this keyspace/cell
	srvKeyspace, err := tm.TopoServer.GetSrvKeyspace(ctx, tm.tabletAlias.Cell, tablet.Keyspace)
	switch {
	case err == nil:
		tm._srvKeyspace = srvKeyspace
	case topo.IsErrType(err, topo.NoNode):
		go tm.rebuildKeyspace(tablet.Keyspace, rebuildKeyspaceRetryInterval)
	default:
		return vterrors.Wrap(err, "initeKeyspaceShardTopo: failed to read SrvKeyspace")
	}

	// Rebuild vschema graph if this is the first tablet in this keyspace/cell.
	srvVSchema, err := tm.TopoServer.GetSrvVSchema(ctx, tm.tabletAlias.Cell)
	switch {
	case err == nil:
		// Check if vschema was rebuilt after the initial creation of the keyspace.
		if _, keyspaceExists := srvVSchema.GetKeyspaces()[tablet.Keyspace]; !keyspaceExists {
			if err := tm.TopoServer.RebuildSrvVSchema(ctx, []string{tm.tabletAlias.Cell}); err != nil {
				return vterrors.Wrap(err, "initeKeyspaceShardTopo: failed to RebuildSrvVSchema")
			}
		}
	case topo.IsErrType(err, topo.NoNode):
		// There is no SrvSchema in this cell at all, so we definitely need to rebuild.
		if err := tm.TopoServer.RebuildSrvVSchema(ctx, []string{tm.tabletAlias.Cell}); err != nil {
			return vterrors.Wrap(err, "initeKeyspaceShardTopo: failed to RebuildSrvVSchema")
		}
	default:
		return vterrors.Wrap(err, "initeKeyspaceShardTopo: failed to read SrvVSchema")
	}
	return nil
}

func (tm *TabletManager) rebuildKeyspace(keyspace string, retryInterval time.Duration) {
	var srvKeyspace *topodatapb.SrvKeyspace
	defer func() {
		log.Infof("Keyspace rebuilt: %v", keyspace)
		tm.mutex.Lock()
		defer tm.mutex.Unlock()
		tm._srvKeyspace = srvKeyspace
	}()

	// RebuildKeyspace will fail until at least one tablet is up for every shard.
	firstTime := true
	var err error
	for {
		if !firstTime {
			// If keyspace was rebuilt by someone else, we can just exit.
			srvKeyspace, err = tm.TopoServer.GetSrvKeyspace(tm.BatchCtx, tm.tabletAlias.Cell, keyspace)
			if err == nil {
				return
			}
		}
		err = topotools.RebuildKeyspace(tm.BatchCtx, logutil.NewConsoleLogger(), tm.TopoServer, keyspace, []string{tm.tabletAlias.Cell})
		if err == nil {
			srvKeyspace, err = tm.TopoServer.GetSrvKeyspace(tm.BatchCtx, tm.tabletAlias.Cell, keyspace)
			if err == nil {
				return
			}
		}
		if firstTime {
			log.Warningf("rebuildKeyspace failed, will retry every %v: %v", retryInterval, err)
		}
		firstTime = false
		time.Sleep(retryInterval)
	}
}

func (tm *TabletManager) checkMastership(ctx context.Context) error {
	tm.mutex.Lock()
	si := tm._shardInfo
	tm.mutex.Unlock()

	if si.MasterAlias != nil && topoproto.TabletAliasEqual(si.MasterAlias, tm.tabletAlias) {
		// We're marked as master in the shard record, which could mean the master
		// tablet process was just restarted. However, we need to check if a new
		// master is in the process of taking over. In that case, it will let us
		// know by forcibly updating the old master's tablet record.
		oldTablet, err := tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
		switch {
		case topo.IsErrType(err, topo.NoNode):
			// There's no existing tablet record, so we can assume
			// no one has left us a message to step down.
			tm.updateTablet(func(tablet *topodatapb.Tablet) {
				tablet.Type = topodatapb.TabletType_MASTER
				// Update the master term start time (current value is 0) because we
				// assume that we are actually the MASTER and in case of a tiebreak,
				// vtgate should prefer us.
				tablet.MasterTermStartTime = logutil.TimeToProto(time.Now())
			})
		case err == nil:
			if oldTablet.Type == topodatapb.TabletType_MASTER {
				// We're marked as master in the shard record,
				// and our existing tablet record agrees.
				tm.updateTablet(func(tablet *topodatapb.Tablet) {
					tablet.Type = topodatapb.TabletType_MASTER
					tablet.MasterTermStartTime = oldTablet.MasterTermStartTime
				})
			}
		default:
			return vterrors.Wrap(err, "InitTablet failed to read existing tablet record")
		}
	} else {
		oldTablet, err := tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
		switch {
		case topo.IsErrType(err, topo.NoNode):
			// There's no existing tablet record, so there is nothing to do
		case err == nil:
			if oldTablet.Type == topodatapb.TabletType_MASTER {
				// Our existing tablet type is master, but the shard record does not agree.
				// Only take over if our master_term_start_time is after what is in the shard record
				oldMasterTermStartTime := oldTablet.GetMasterTermStartTime()
				currentShardTime := si.GetMasterTermStartTime()
				if oldMasterTermStartTime.After(currentShardTime) {
					tm.updateTablet(func(tablet *topodatapb.Tablet) {
						tablet.Type = topodatapb.TabletType_MASTER
						tablet.MasterTermStartTime = oldTablet.MasterTermStartTime
					})
				}
			}
		default:
			return vterrors.Wrap(err, "InitTablet failed to read existing tablet record")
		}
	}
	return nil
}

func (tm *TabletManager) checkMysql(ctx context.Context) error {
	if appConfig, _ := tm.DBConfigs.AppWithDB().MysqlParams(); appConfig.Host != "" {
		tm.updateTablet(func(tablet *topodatapb.Tablet) {
			tablet.MysqlHostname = appConfig.Host
			tablet.MysqlPort = int32(appConfig.Port)
		})
	} else {
		// Assume unix socket was specified and try to get the port from mysqld
		tm.updateTablet(func(tablet *topodatapb.Tablet) {
			tablet.MysqlHostname = tablet.Hostname
		})
		mysqlPort, err := tm.MysqlDaemon.GetMysqlPort()
		if err != nil {
			log.Warningf("Cannot get current mysql port, will keep retrying every %v: %v", mysqlPortRetryInterval, err)
			go tm.findMysqlPort(mysqlPortRetryInterval)
		} else {
			tm.updateTablet(func(tablet *topodatapb.Tablet) {
				tablet.MysqlPort = mysqlPort
			})
		}
	}
	return nil
}

func (tm *TabletManager) findMysqlPort(retryInterval time.Duration) {
	for {
		time.Sleep(retryInterval)
		mport, err := tm.MysqlDaemon.GetMysqlPort()
		if err != nil {
			continue
		}
		// We need to get the action lock to make sure no one
		// else is updating the tablet.
		if err := tm.lock(tm.BatchCtx); err != nil {
			continue
		}
		defer tm.unlock()
		log.Infof("Identified mysql port: %v", mport)
		tm.pubMu.Lock()
		tm.tablet.MysqlPort = mport
		tm.pubMu.Unlock()
		tm.publishState(tm.BatchCtx)
		return
	}
}

func (tm *TabletManager) initTablet(ctx context.Context) error {
	tablet := tm.Tablet()
	err := tm.TopoServer.CreateTablet(ctx, tablet)
	switch {
	case err == nil:
		// It worked, we're good.
	case topo.IsErrType(err, topo.NodeExists):
		// The node already exists, will just try to update
		// it. So we read it first.
		oldTablet, err := tm.TopoServer.GetTablet(ctx, tablet.Alias)
		if err != nil {
			return vterrors.Wrap(err, "initTablet failed to read existing tablet record")
		}

		// Sanity check the keyspace and shard
		if oldTablet.Keyspace != tablet.Keyspace || oldTablet.Shard != tablet.Shard {
			return fmt.Errorf("initTablet failed because existing tablet keyspace and shard %v/%v differ from the provided ones %v/%v", oldTablet.Keyspace, oldTablet.Shard, tablet.Keyspace, tablet.Shard)
		}

		// Update ShardReplication in any case, to be sure.  This is
		// meant to fix the case when a Tablet record was created, but
		// then the ShardReplication record was not (because for
		// instance of a startup timeout). Upon running this code
		// again, we want to fix ShardReplication.
		if updateErr := topo.UpdateTabletReplicationData(ctx, tm.TopoServer, tablet); updateErr != nil {
			return vterrors.Wrap(updateErr, "UpdateTabletReplicationData failed")
		}

		// Then overwrite everything, ignoring version mismatch.
		if err := tm.TopoServer.UpdateTablet(ctx, topo.NewTabletInfo(tablet, nil)); err != nil {
			return vterrors.Wrap(err, "UpdateTablet failed")
		}
	default:
		return vterrors.Wrap(err, "CreateTablet failed")
	}
	return nil
}

func (tm *TabletManager) handleRestore(ctx context.Context) (bool, error) {
	tablet := tm.Tablet()
	// Sanity check for inconsistent flags
	if tm.Cnf == nil && *restoreFromBackup {
		return false, fmt.Errorf("you cannot enable -restore_from_backup without a my.cnf file")
	}

	// two cases then:
	// - restoreFromBackup is set: we restore, then initHealthCheck, all
	//   in the background
	// - restoreFromBackup is not set: we initHealthCheck right away
	if *restoreFromBackup {
		go func() {
			// restoreFromBackup will just be a regular action
			// (same as if it was triggered remotely)
			if err := tm.RestoreData(ctx, logutil.NewConsoleLogger(), *waitForBackupInterval, false /* deleteBeforeRestore */); err != nil {
				log.Exitf("RestoreFromBackup failed: %v", err)
			}
		}()
		return true, nil
	}

	// optionally populate metadata records
	if *initPopulateMetadata {
		localMetadata := tm.getLocalMetadataValues(tablet.Type)
		if tm.Cnf != nil { // we are managing mysqld
			// we'll use batchCtx here because we are still initializing and can't proceed unless this succeeds
			if err := tm.MysqlDaemon.Wait(ctx, tm.Cnf); err != nil {
				return false, err
			}
		}
		err := mysqlctl.PopulateMetadataTables(tm.MysqlDaemon, localMetadata, topoproto.TabletDbName(tablet))
		if err != nil {
			return false, vterrors.Wrap(err, "failed to -init_populate_metadata")
		}
	}
	return false, nil
}

func (tm *TabletManager) exportStats() {
	tablet := tm.Tablet()
	statsKeyspace.Set(tablet.Keyspace)
	statsShard.Set(tablet.Shard)
	if key.KeyRangeIsPartial(tablet.KeyRange) {
		statsKeyRangeStart.Set(hex.EncodeToString(tablet.KeyRange.Start))
		statsKeyRangeEnd.Set(hex.EncodeToString(tablet.KeyRange.End))
	}
	statsAlias.Set(topoproto.TabletAliasString(tablet.Alias))
}

// withRetry will exponentially back off and retry a function upon
// failure, until the context is Done(), or the function returned with
// no error. We use this at startup with a context timeout set to the
// value of the init_timeout flag, so we can try to modify the
// topology over a longer period instead of dying right away.
func (tm *TabletManager) withRetry(ctx context.Context, description string, work func() error) error {
	backoff := 1 * time.Second
	for {
		err := work()
		if err == nil || err == context.Canceled || err == context.DeadlineExceeded {
			return err
		}

		log.Warningf("%v failed (%v), backing off %v before retrying", description, err, backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// Exponential backoff with 1.3 as a factor,
			// and randomized down by at most 20
			// percent. The generated time series looks
			// good.  Also note rand.Seed is called at
			// init() time in binlog_players.go.
			f := float64(backoff) * 1.3
			f -= f * 0.2 * rand.Float64()
			backoff = time.Duration(f)
		}
	}
}

func (tm *TabletManager) setTablet(tablet *topodatapb.Tablet) {
	tm.pubMu.Lock()
	tm.tablet = proto.Clone(tablet).(*topodatapb.Tablet)
	tm.pubMu.Unlock()

	// Notify the shard sync loop that the tablet state changed.
	tm.notifyShardSync()
}

func (tm *TabletManager) updateTablet(update func(tablet *topodatapb.Tablet)) {
	tm.pubMu.Lock()
	update(tm.tablet)
	tm.pubMu.Unlock()

	// Notify the shard sync loop that the tablet state changed.
	tm.notifyShardSync()
}

// Tablet reads the stored Tablet from the tm.
func (tm *TabletManager) Tablet() *topodatapb.Tablet {
	tm.pubMu.Lock()
	tablet := proto.Clone(tm.tablet).(*topodatapb.Tablet)
	tm.pubMu.Unlock()
	return tablet
}

// Healthy reads the result of the latest healthcheck, protected by mutex.
// If that status is too old, it means healthcheck hasn't run for a while,
// and is probably stuck, this is not good, we're not healthy.
func (tm *TabletManager) Healthy() (time.Duration, error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	healthy := tm._healthy
	if healthy == nil {
		timeSinceLastCheck := time.Since(tm._healthyTime)
		if timeSinceLastCheck > healthCheckInterval*3 {
			healthy = fmt.Errorf("last health check is too old: %s > %s", timeSinceLastCheck, healthCheckInterval*3)
		}
	}

	return tm._replicationDelay, healthy
}

// BlacklistedTables returns the list of currently blacklisted tables.
func (tm *TabletManager) BlacklistedTables() []string {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	return tm._blacklistedTables
}

// DisallowQueryService returns the reason the query service should be
// disabled, if any.
func (tm *TabletManager) DisallowQueryService() string {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	return tm._disallowQueryService
}

func (tm *TabletManager) replicationStopped() bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// If we already know the value, don't bother checking the file.
	if tm._replicationStopped != nil {
		return *tm._replicationStopped
	}

	// If there's no Cnf file, don't read state.
	if tm.Cnf == nil {
		return false
	}

	// If the marker file exists, we're stopped.
	// Treat any read error as if the file doesn't exist.
	_, err := os.Stat(path.Join(tm.Cnf.TabletDir(), replicationStoppedFile))
	replicationStopped := err == nil
	tm._replicationStopped = &replicationStopped
	return replicationStopped
}

func (tm *TabletManager) setReplicationStopped(stopped bool) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tm._replicationStopped = &stopped

	// Make a best-effort attempt to persist the value across tablet restarts.
	// We store a marker in the filesystem so it works regardless of whether
	// mysqld is running, and so it's tied to this particular instance of the
	// tablet data dir (the one that's paused at a known replication position).
	if tm.Cnf == nil {
		return
	}
	tabletDir := tm.Cnf.TabletDir()
	if tabletDir == "" {
		return
	}
	markerFile := path.Join(tabletDir, replicationStoppedFile)
	if stopped {
		file, err := os.Create(markerFile)
		if err == nil {
			file.Close()
		}
	} else {
		os.Remove(markerFile)
	}
}

func (tm *TabletManager) setServicesDesiredState(disallowQueryService string) {
	tm.mutex.Lock()
	tm._disallowQueryService = disallowQueryService
	tm.mutex.Unlock()
}

func (tm *TabletManager) setBlacklistedTables(value []string) {
	tm.mutex.Lock()
	tm._blacklistedTables = value
	tm.mutex.Unlock()
}

// hookExtraEnv returns the map to pass to local hooks
func (tm *TabletManager) hookExtraEnv() map[string]string {
	return map[string]string{"TABLET_ALIAS": topoproto.TabletAliasString(tm.tabletAlias)}
}
