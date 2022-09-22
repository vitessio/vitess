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

Most RPC calls obtain the actionSema, except the easy read-only ones.
RPC calls that change the tablet record will also call updateState.

See rpc_server.go for all cases, and which actions take the actionSema,
and which run changeCallback.
*/
package tabletmanager

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Query rules from denylist
const denyListQueryList string = "DenyListQueryRules"

var (
	// The following flags initialize the tablet record.
	tabletHostname     string
	initKeyspace       string
	initShard          string
	initTabletType     string
	initDbNameOverride string
	skipBuildInfoTags  = "/.*/"
	initTags           flagutil.StringMapValue

	initPopulateMetadata bool
	initTimeout          = 1 * time.Minute
)

func registerInitFlags(fs *pflag.FlagSet) {
	fs.StringVar(&tabletHostname, "tablet_hostname", tabletHostname, "if not empty, this hostname will be assumed instead of trying to resolve it")
	fs.StringVar(&initKeyspace, "init_keyspace", initKeyspace, "(init parameter) keyspace to use for this tablet")
	fs.StringVar(&initShard, "init_shard", initShard, "(init parameter) shard to use for this tablet")
	fs.StringVar(&initTabletType, "init_tablet_type", initTabletType, "(init parameter) the tablet type to use for this tablet.")
	fs.StringVar(&initDbNameOverride, "init_db_name_override", initDbNameOverride, "(init parameter) override the name of the db used by vttablet. Without this flag, the db name defaults to vt_<keyspacename>")
	fs.StringVar(&skipBuildInfoTags, "vttablet_skip_buildinfo_tags", skipBuildInfoTags, "comma-separated list of buildinfo tags to skip from merging with --init_tags. each tag is either an exact match or a regular expression of the form '/regexp/'.")
	fs.Var(&initTags, "init_tags", "(init parameter) comma separated list of key:value pairs used to tag the tablet")

	fs.BoolVar(&initPopulateMetadata, "init_populate_metadata", initPopulateMetadata, "(init parameter) populate metadata tables even if restore_from_backup is disabled. If restore_from_backup is enabled, metadata tables are always populated regardless of this flag.")
	fs.DurationVar(&initTimeout, "init_timeout", initTimeout, "(init parameter) timeout to use for the init phase.")
}

var (
	// statsTabletType is set to expose the current tablet type.
	statsTabletType *stats.String

	// statsTabletTypeCount exposes the current tablet type as a label,
	// with the value counting the occurrences of the respective tablet type.
	// Useful for Prometheus which doesn't support exporting strings as stat values.
	statsTabletTypeCount *stats.CountersWithSingleLabel

	// statsBackupIsRunning is set to 1 (true) if a backup is running.
	statsBackupIsRunning *stats.GaugesWithMultiLabels

	// statsIsInSrvKeyspace is set to 1 (true), 0 (false) whether the tablet is in the serving keyspace
	statsIsInSrvKeyspace *stats.Gauge

	statsKeyspace      = stats.NewString("TabletKeyspace")
	statsShard         = stats.NewString("TabletShard")
	statsKeyRangeStart = stats.NewString("TabletKeyRangeStart")
	statsKeyRangeEnd   = stats.NewString("TabletKeyRangeEnd")
	statsAlias         = stats.NewString("TabletAlias")

	// The following variables can be changed to speed up tests.
	mysqlPortRetryInterval       = 1 * time.Second
	rebuildKeyspaceRetryInterval = 1 * time.Second
)

func init() {
	servenv.OnParseFor("vtcombo", registerInitFlags)
	servenv.OnParseFor("vttablet", registerInitFlags)

	statsTabletType = stats.NewString("TabletType")
	statsTabletTypeCount = stats.NewCountersWithSingleLabel("TabletTypeCount", "Number of times the tablet changed to the labeled type", "type")
	statsBackupIsRunning = stats.NewGaugesWithMultiLabels("BackupIsRunning", "Whether a backup is running", []string{"mode"})
	statsIsInSrvKeyspace = stats.NewGauge("IsInSrvKeyspace", "Whether the vttablet is in the serving keyspace (1 = true / 0 = false)")
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
	VDiffEngine         *vdiff.Engine

	// MetadataManager manages the local metadata tables for a tablet. It
	// exists, and is exported, to support swapping a nil pointer in test code,
	// in which case metadata creation/population is skipped.
	MetadataManager *mysqlctl.MetadataManager

	// tmState manages the TabletManager state.
	tmState *tmState

	// replManager manages replication.
	replManager *replManager

	// tabletAlias is saved away from tablet for read-only access
	tabletAlias *topodatapb.TabletAlias

	// baseTabletType is the tablet type we revert back to
	// when we transition back from something like PRIMARY.
	baseTabletType topodatapb.TabletType

	// actionSema is there to run only one action at a time.
	// This semaphore can be held for long periods of time (hours),
	// like in the case of a restore. This semaphore must be obtained
	// first before other mutexes.
	actionSema *sync2.Semaphore

	// orc is an optional client for Orchestrator HTTP API calls.
	// If this is nil, those calls will be skipped.
	// It's only set once in NewTabletManager() and never modified after that.
	orc *orcClient

	// mutex protects all the following fields (that start with '_'),
	// only hold the mutex to update the fields, nothing else.
	mutex sync.Mutex

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

	// _rebuildKeyspaceDone is a channel for waiting until the current keyspace
	// has been rebuilt
	_rebuildKeyspaceDone chan struct{}

	// _rebuildKeyspaceCancel is the function to stop a keyspace rebuild currently
	// in progress
	_rebuildKeyspaceCancel context.CancelFunc

	// _lockTablesConnection is used to get and release the table read locks to pause replication
	_lockTablesConnection *dbconnpool.DBConnection
	_lockTablesTimer      *time.Timer
	// _isBackupRunning tells us whether there is a backup that is currently running
	_isBackupRunning bool
}

// BuildTabletFromInput builds a tablet record from input parameters.
func BuildTabletFromInput(alias *topodatapb.TabletAlias, port, grpcPort int32, dbServerVersion string, db *dbconfigs.DBConfigs) (*topodatapb.Tablet, error) {
	hostname := tabletHostname
	if hostname == "" {
		var err error
		hostname, err = netutil.FullyQualifiedHostname()
		if err != nil {
			return nil, err
		}
		log.Infof("Using detected machine hostname: %v, to change this, fix your machine network configuration or override it with --tablet_hostname.", hostname)
	} else {
		log.Infof("Using hostname: %v from --tablet_hostname flag.", hostname)
	}

	if initKeyspace == "" || initShard == "" {
		return nil, fmt.Errorf("init_keyspace and init_shard must be specified")
	}

	// parse and validate shard name
	shard, keyRange, err := topo.ValidateShardName(initShard)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot validate shard name %v", initShard)
	}

	tabletType, err := topoproto.ParseTabletType(initTabletType)
	if err != nil {
		return nil, err
	}
	switch tabletType {
	case topodatapb.TabletType_SPARE, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
	default:
		return nil, fmt.Errorf("invalid init_tablet_type %v; can only be REPLICA, RDONLY or SPARE", tabletType)
	}

	buildTags, err := getBuildTags(servenv.AppVersion.ToStringMap(), skipBuildInfoTags)
	if err != nil {
		return nil, err
	}

	var charset uint8
	if db != nil && db.Charset != "" {
		charset, err = collations.Local().ParseConnectionCharset(db.Charset)
		if err != nil {
			return nil, err
		}
	} else {
		charset = collations.Local().DefaultConnectionCharset()
	}

	return &topodatapb.Tablet{
		Alias:    alias,
		Hostname: hostname,
		PortMap: map[string]int32{
			"vt":   port,
			"grpc": grpcPort,
		},
		Keyspace:             initKeyspace,
		Shard:                shard,
		KeyRange:             keyRange,
		Type:                 tabletType,
		DbNameOverride:       initDbNameOverride,
		Tags:                 mergeTags(buildTags, initTags),
		DbServerVersion:      dbServerVersion,
		DefaultConnCollation: uint32(charset),
	}, nil
}

func getBuildTags(buildTags map[string]string, skipTagsCSV string) (map[string]string, error) {
	if skipTagsCSV == "" {
		return buildTags, nil
	}

	skipTags := strings.Split(skipTagsCSV, ",")
	skippers := make([]func(string) bool, len(skipTags))
	for i, skipTag := range skipTags {
		skipTag := skipTag // copy to preserve iteration scope in the closures below
		if strings.HasPrefix(skipTag, "/") && strings.HasSuffix(skipTag, "/") && len(skipTag) > 1 {
			// regexp mode
			tagRegexp, err := regexp.Compile(skipTag[1 : len(skipTag)-1])
			if err != nil {
				return nil, err
			}

			skippers[i] = func(s string) bool {
				return tagRegexp.MatchString(s)
			}
		} else {
			skippers[i] = func(s string) bool {
				log.Warningf(skipTag)
				return s == skipTag
			}
		}
	}

	skippedTags := sets.NewString()
	for tag := range buildTags {
		for _, skipFn := range skippers {
			if skipFn(tag) {
				skippedTags.Insert(tag)
				break
			}
		}
	}

	result := make(map[string]string, len(buildTags)-skippedTags.Len())
	for tag, val := range buildTags {
		if skippedTags.Has(tag) {
			continue
		}

		result[tag] = val
	}

	return result, nil
}

func mergeTags(a, b map[string]string) map[string]string {
	maxCap := len(a)
	if x := len(b); x > maxCap {
		maxCap = x
	}

	result := make(map[string]string, maxCap)
	for k, v := range a {
		result[k] = v
	}

	for k, v := range b {
		result[k] = v
	}

	return result
}

// Start starts the TabletManager.
func (tm *TabletManager) Start(tablet *topodatapb.Tablet, healthCheckInterval time.Duration) error {
	tm.DBConfigs.DBName = topoproto.TabletDbName(tablet)
	tm.replManager = newReplManager(tm.BatchCtx, tm, healthCheckInterval)
	tm.tabletAlias = tablet.Alias
	tm.tmState = newTMState(tm, tablet)
	tm.actionSema = sync2.NewSemaphore(1, 0)

	tm.baseTabletType = tablet.Type

	ctx, cancel := context.WithTimeout(tm.BatchCtx, initTimeout)
	defer cancel()
	si, err := tm.createKeyspaceShard(ctx)
	if err != nil {
		return err
	}
	if err := tm.checkPrimaryShip(ctx, si); err != nil {
		return err
	}
	if err := tm.checkMysql(ctx); err != nil {
		return err
	}
	if err := tm.initTablet(ctx); err != nil {
		return err
	}

	err = tm.QueryServiceControl.InitDBConfig(&querypb.Target{
		Keyspace:   tablet.Keyspace,
		Shard:      tablet.Shard,
		TabletType: tablet.Type,
	}, tm.DBConfigs, tm.MysqlDaemon)
	if err != nil {
		return vterrors.Wrap(err, "failed to InitDBConfig")
	}
	tm.QueryServiceControl.RegisterQueryRuleSource(denyListQueryList)

	if tm.UpdateStream != nil {
		tm.UpdateStream.InitDBConfig(tm.DBConfigs)
		servenv.OnRun(tm.UpdateStream.RegisterService)
		servenv.OnTerm(tm.UpdateStream.Disable)
	}

	if tm.VREngine != nil {
		tm.VREngine.InitDBConfig(tm.DBConfigs)
		servenv.OnTerm(tm.VREngine.Close)
	}

	if tm.VDiffEngine != nil {
		tm.VDiffEngine.InitDBConfig(tm.DBConfigs)
		servenv.OnTerm(tm.VDiffEngine.Close)
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
		// of updating the tablet state and initializing replication.
		return nil
	}

	// We should be re-read the tablet from tabletManager and use the type specified there.
	// We shouldn't use the base tablet type directly, since the type could have changed to PRIMARY
	// earlier in tm.checkPrimaryShip code.
	_, err = tm.initializeReplication(ctx, tm.Tablet().Type)
	tm.tmState.Open()
	return err
}

// Close prepares a tablet for shutdown. First we check our tablet ownership and
// then prune the tablet topology entry of all post-init fields. This prevents
// stale identifiers from hanging around in topology.
func (tm *TabletManager) Close() {
	// Stop the shard sync loop and wait for it to exit. We do this in Close()
	// rather than registering it as an OnTerm hook so the shard sync loop keeps
	// running during lame duck.
	tm.stopShardSync()
	tm.stopRebuildKeyspace()

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

	tm.tmState.Close()
}

// Stop shuts down the tm. Normally this is not necessary, since we use
// servenv OnTerm and OnClose hooks to coordinate shutdown automatically,
// while taking lameduck into account. However, this may be useful for tests,
// when you want to clean up an tm immediately.
func (tm *TabletManager) Stop() {
	// Stop the shard sync loop and wait for it to exit. This needs to be done
	// here in addition to in Close() because tests do not call Close().
	tm.stopShardSync()
	tm.stopRebuildKeyspace()

	if tm.UpdateStream != nil {
		tm.UpdateStream.Disable()
	}

	if tm.VREngine != nil {
		tm.VREngine.Close()
	}

	if tm.VDiffEngine != nil {
		tm.VDiffEngine.Close()
	}

	tm.MysqlDaemon.Close()
	tm.tmState.Close()
}

func (tm *TabletManager) createKeyspaceShard(ctx context.Context) (*topo.ShardInfo, error) {
	// mutex is needed because we set _shardInfo and _srvKeyspace
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tablet := tm.Tablet()
	log.Infof("Reading/creating keyspace and shard records for %v/%v", tablet.Keyspace, tablet.Shard)

	// Read the shard, create it if necessary.
	var shardInfo *topo.ShardInfo
	if err := tm.withRetry(ctx, "creating keyspace and shard", func() error {
		var err error
		shardInfo, err = tm.TopoServer.GetOrCreateShard(ctx, tablet.Keyspace, tablet.Shard)
		return err
	}); err != nil {
		return nil, vterrors.Wrap(err, "createKeyspaceShard: cannot GetOrCreateShard shard")
	}
	tm.tmState.RefreshFromTopoInfo(ctx, shardInfo, nil)

	// Rebuild keyspace if this the first tablet in this keyspace/cell
	srvKeyspace, err := tm.TopoServer.GetSrvKeyspace(ctx, tm.tabletAlias.Cell, tablet.Keyspace)
	switch {
	case err == nil:
		tm.tmState.RefreshFromTopoInfo(ctx, nil, srvKeyspace)
	case topo.IsErrType(err, topo.NoNode):
		var rebuildKsCtx context.Context
		rebuildKsCtx, tm._rebuildKeyspaceCancel = context.WithCancel(tm.BatchCtx)
		tm._rebuildKeyspaceDone = make(chan struct{})
		go tm.rebuildKeyspace(rebuildKsCtx, tm._rebuildKeyspaceDone, tablet.Keyspace, rebuildKeyspaceRetryInterval)
	default:
		return nil, vterrors.Wrap(err, "initeKeyspaceShardTopo: failed to read SrvKeyspace")
	}

	// Rebuild vschema graph if this is the first tablet in this keyspace/cell.
	srvVSchema, err := tm.TopoServer.GetSrvVSchema(ctx, tm.tabletAlias.Cell)
	switch {
	case err == nil:
		// Check if vschema was rebuilt after the initial creation of the keyspace.
		if _, keyspaceExists := srvVSchema.GetKeyspaces()[tablet.Keyspace]; !keyspaceExists {
			if err := tm.TopoServer.RebuildSrvVSchema(ctx, []string{tm.tabletAlias.Cell}); err != nil {
				return nil, vterrors.Wrap(err, "initeKeyspaceShardTopo: failed to RebuildSrvVSchema")
			}
		}
	case topo.IsErrType(err, topo.NoNode):
		// There is no SrvSchema in this cell at all, so we definitely need to rebuild.
		if err := tm.TopoServer.RebuildSrvVSchema(ctx, []string{tm.tabletAlias.Cell}); err != nil {
			return nil, vterrors.Wrap(err, "initeKeyspaceShardTopo: failed to RebuildSrvVSchema")
		}
	default:
		return nil, vterrors.Wrap(err, "initeKeyspaceShardTopo: failed to read SrvVSchema")
	}
	return shardInfo, nil
}

func (tm *TabletManager) stopRebuildKeyspace() {
	var doneChan <-chan struct{}

	tm.mutex.Lock()
	if tm._rebuildKeyspaceCancel != nil {
		tm._rebuildKeyspaceCancel()
	}
	doneChan = tm._rebuildKeyspaceDone
	tm.mutex.Unlock()

	if doneChan != nil {
		<-doneChan
	}
}

func (tm *TabletManager) rebuildKeyspace(ctx context.Context, done chan<- struct{}, keyspace string, retryInterval time.Duration) {
	var srvKeyspace *topodatapb.SrvKeyspace

	defer func() {
		log.Infof("Keyspace rebuilt: %v", keyspace)
		if ctx.Err() == nil {
			tm.tmState.RefreshFromTopoInfo(tm.BatchCtx, nil, srvKeyspace)
		}
		close(done)
	}()

	// RebuildKeyspace will fail until at least one tablet is up for every shard.
	firstTime := true
	var err error
	for {
		if ctx.Err() != nil {
			return
		}
		if !firstTime {
			// If keyspace was rebuilt by someone else, we can just exit.
			srvKeyspace, err = tm.TopoServer.GetSrvKeyspace(ctx, tm.tabletAlias.Cell, keyspace)
			if err == nil || ctx.Err() != nil {
				return
			}
		}
		err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), tm.TopoServer, keyspace, []string{tm.tabletAlias.Cell}, false)
		if err == nil {
			srvKeyspace, err = tm.TopoServer.GetSrvKeyspace(ctx, tm.tabletAlias.Cell, keyspace)
			if err == nil || ctx.Err() != nil {
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

func (tm *TabletManager) checkPrimaryShip(ctx context.Context, si *topo.ShardInfo) error {
	if si.PrimaryAlias != nil && topoproto.TabletAliasEqual(si.PrimaryAlias, tm.tabletAlias) {
		// We're marked as primary in the shard record, which could mean the primary
		// tablet process was just restarted. However, we need to check if a new
		// primary is in the process of taking over. In that case, it will let us
		// know by forcibly updating the old primary's tablet record.
		oldTablet, err := tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
		switch {
		case topo.IsErrType(err, topo.NoNode):
			// There's no existing tablet record, so we can assume
			// no one has left us a message to step down.
			log.Infof("Shard primary alias matches, but there is no existing tablet record. Switching to primary with 'Now' as time")
			tm.tmState.UpdateTablet(func(tablet *topodatapb.Tablet) {
				tablet.Type = topodatapb.TabletType_PRIMARY
				// Update the primary term start time (current value is 0) because we
				// assume that we are actually the PRIMARY and in case of a tiebreak,
				// vtgate should prefer us.
				tablet.PrimaryTermStartTime = logutil.TimeToProto(time.Now())
			})
		case err == nil:
			if oldTablet.Type == topodatapb.TabletType_PRIMARY {
				log.Infof("Shard primary alias matches, and existing tablet agrees. Switching to primary with tablet's primary term start time: %v", oldTablet.PrimaryTermStartTime)
				// We're marked as primary in the shard record,
				// and our existing tablet record agrees.
				tm.tmState.UpdateTablet(func(tablet *topodatapb.Tablet) {
					tablet.Type = topodatapb.TabletType_PRIMARY
					tablet.PrimaryTermStartTime = oldTablet.PrimaryTermStartTime
				})
			} else {
				log.Warningf("Shard primary alias matches, but existing tablet is not primary. Switching from %v to primary with the shard's primary term start time: %v", oldTablet.Type, si.PrimaryTermStartTime)
				tm.tmState.UpdateTablet(func(tablet *topodatapb.Tablet) {
					tablet.Type = topodatapb.TabletType_PRIMARY
					tablet.PrimaryTermStartTime = si.PrimaryTermStartTime
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
			if oldTablet.Type == topodatapb.TabletType_PRIMARY {
				// Our existing tablet type is primary, but the shard record does not agree.
				// Only take over if our primary_term_start_time is after what is in the shard record
				oldPrimaryTermStartTime := oldTablet.GetPrimaryTermStartTime()
				currentShardTime := si.GetPrimaryTermStartTime()
				if oldPrimaryTermStartTime.After(currentShardTime) {
					log.Infof("Shard primary alias does not match, but the tablet's primary term start time is newer. Switching to primary with tablet's primary term start time: %v", oldTablet.PrimaryTermStartTime)
					tm.tmState.UpdateTablet(func(tablet *topodatapb.Tablet) {
						tablet.Type = topodatapb.TabletType_PRIMARY
						tablet.PrimaryTermStartTime = oldTablet.PrimaryTermStartTime
					})
				} else {
					log.Infof("Existing tablet type is primary, but the shard record has a different primary with a newer timestamp. Remaining a replica")
				}
			}
		default:
			return vterrors.Wrap(err, "InitTablet failed to read existing tablet record")
		}
	}
	return nil
}

func (tm *TabletManager) checkMysql(ctx context.Context) error {
	appConfig, err := tm.DBConfigs.AppWithDB().MysqlParams()
	if err != nil {
		return err
	}
	if appConfig.Host != "" {
		tm.tmState.UpdateTablet(func(tablet *topodatapb.Tablet) {
			tablet.MysqlHostname = appConfig.Host
			tablet.MysqlPort = int32(appConfig.Port)
		})
	} else {
		// Assume unix socket was specified and try to get the port from mysqld
		tm.tmState.UpdateTablet(func(tablet *topodatapb.Tablet) {
			tablet.MysqlHostname = tablet.Hostname
		})
		mysqlPort, err := tm.MysqlDaemon.GetMysqlPort()
		if err != nil {
			log.Warningf("Cannot get current mysql port, will keep retrying every %v: %v", mysqlPortRetryInterval, err)
			go tm.findMysqlPort(mysqlPortRetryInterval)
		} else {
			tm.tmState.UpdateTablet(func(tablet *topodatapb.Tablet) {
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
		log.Infof("Identified mysql port: %v", mport)
		tm.tmState.SetMysqlPort(mport)
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
	if tm.Cnf == nil && restoreFromBackup {
		return false, fmt.Errorf("you cannot enable --restore_from_backup without a my.cnf file")
	}

	// Restore in the background
	if restoreFromBackup {
		go func() {
			// Open the state manager after restore is done.
			defer tm.tmState.Open()

			// Zero date will cause us to use the latest, which is the default
			backupTime := time.Time{}

			// Or if a backup timestamp was specified then we use the last backup taken at or before that time
			if restoreFromBackupTsStr != "" {
				var err error
				backupTime, err = time.Parse(mysqlctl.BackupTimestampFormat, restoreFromBackupTsStr)
				if err != nil {
					log.Exitf(fmt.Sprintf("RestoreFromBackup failed: unable to parse the backup timestamp value provided of '%s'", restoreFromBackupTsStr))
				}
			}

			// restoreFromBackup will just be a regular action
			// (same as if it was triggered remotely)
			if err := tm.RestoreData(ctx, logutil.NewConsoleLogger(), waitForBackupInterval, false /* deleteBeforeRestore */, backupTime); err != nil {
				log.Exitf("RestoreFromBackup failed: %v", err)
			}
		}()
		return true, nil
	}

	// optionally populate metadata records
	if initPopulateMetadata {
		localMetadata := tm.getLocalMetadataValues(tablet.Type)
		if tm.Cnf != nil { // we are managing mysqld
			// we'll use batchCtx here because we are still initializing and can't proceed unless this succeeds
			if err := tm.MysqlDaemon.Wait(ctx, tm.Cnf); err != nil {
				return false, err
			}
		}

		if tm.MetadataManager != nil {
			err := tm.MetadataManager.PopulateMetadataTables(tm.MysqlDaemon, localMetadata, topoproto.TabletDbName(tablet))
			if err != nil {
				return false, vterrors.Wrap(err, "failed to --init_populate_metadata")
			}
		}
	}
	return false, nil
}

func (tm *TabletManager) exportStats() {
	tablet := tm.Tablet()
	statsKeyspace.Set(tablet.Keyspace)
	statsShard.Set(tablet.Shard)
	statsTabletType.Set(topoproto.TabletTypeLString(tm.tmState.tablet.Type))
	statsTabletTypeCount.Add(topoproto.TabletTypeLString(tm.tmState.tablet.Type), 1)
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

// Tablet reads the stored Tablet from the tm.
func (tm *TabletManager) Tablet() *topodatapb.Tablet {
	return tm.tmState.Tablet()
}

// DeniedTables returns the list of currently denied tables.
func (tm *TabletManager) DeniedTables() []string {
	return tm.tmState.DeniedTables()
}

// hookExtraEnv returns the map to pass to local hooks
func (tm *TabletManager) hookExtraEnv() map[string]string {
	tablet := tm.Tablet()

	return map[string]string{
		"TABLET_ALIAS": topoproto.TabletAliasString(tm.tabletAlias),
		"KEYSPACE":     tablet.Keyspace,
		"SHARD":        tablet.Shard,
	}
}

// initializeReplication is used to initialize the replication when the tablet starts.
// It returns the current primary tablet for use externally
func (tm *TabletManager) initializeReplication(ctx context.Context, tabletType topodatapb.TabletType) (primary *topo.TabletInfo, err error) {
	// If active reparents are disabled, we do not touch replication.
	// There is nothing to do
	if mysqlctl.DisableActiveReparents {
		return nil, nil
	}

	// If the desired tablet type is primary, then we shouldn't be setting our replication source.
	// So there is nothing to do.
	if tabletType == topodatapb.TabletType_PRIMARY {
		return nil, nil
	}

	// Read the shard to find the current primary, and its location.
	tablet := tm.Tablet()
	si, err := tm.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, vterrors.Wrap(err, "cannot read shard")
	}
	if si.PrimaryAlias == nil {
		// There's no primary. This is fine, since there might be no primary currently
		log.Warningf("cannot start replication during initialization: shard %v/%v has no primary.", tablet.Keyspace, tablet.Shard)
		return nil, nil
	}
	if topoproto.TabletAliasEqual(si.PrimaryAlias, tablet.Alias) {
		// We used to be the primary before we got restarted,
		// and no other primary has been elected in the meantime.
		// There isn't anything to do here either.
		log.Warningf("cannot start replication during initialization: primary in shard record still points to this tablet.")
		return nil, nil
	}
	currentPrimary, err := tm.TopoServer.GetTablet(ctx, si.PrimaryAlias)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot read primary tablet %v", si.PrimaryAlias)
	}

	durabilityName, err := tm.TopoServer.GetKeyspaceDurability(ctx, tablet.Keyspace)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot read keyspace durability policy %v", tablet.Keyspace)
	}
	log.Infof("Getting a new durability policy for %v", durabilityName)
	durability, err := reparentutil.GetDurabilityPolicy(durabilityName)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot get durability policy %v", durabilityName)
	}
	// If using semi-sync, we need to enable it before connecting to primary.
	// We should set the correct type, since it is used in replica semi-sync
	tablet.Type = tabletType
	if err := tm.fixSemiSync(tabletType, convertBoolToSemiSyncAction(reparentutil.IsReplicaSemiSync(durability, currentPrimary.Tablet, tablet))); err != nil {
		return nil, err
	}

	// Set primary and start replication.
	if err := tm.MysqlDaemon.SetReplicationSource(ctx, currentPrimary.Tablet.MysqlHostname, int(currentPrimary.Tablet.MysqlPort), false /* stopReplicationBefore */, true /* startReplicationAfter */); err != nil {
		return nil, vterrors.Wrap(err, "MysqlDaemon.SetReplicationSource failed")
	}

	return currentPrimary, nil
}
