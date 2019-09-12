/*
Copyright 2017 Google Inc.

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
Package tabletmanager exports the ActionAgent object. It keeps the local tablet
state, starts / stops all associated services (query service,
update stream, binlog players, ...), and handles tabletmanager RPCs
to update the state.

The agent is responsible for maintaining the tablet record in the
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

	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/dbconnpool"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
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
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	// slaveStoppedFile is the file name for the file whose existence informs
	// vttablet to NOT try to repair replication.
	slaveStoppedFile = "do_not_replicate"
)

var (
	tabletHostname = flag.String("tablet_hostname", "", "if not empty, this hostname will be assumed instead of trying to resolve it")
)

// ActionAgent is the main class for the agent.
type ActionAgent struct {
	// The following fields are set during creation
	QueryServiceControl tabletserver.Controller
	UpdateStream        binlog.UpdateStreamControl
	HealthReporter      health.Reporter
	TopoServer          *topo.Server
	TabletAlias         *topodatapb.TabletAlias
	Cnf                 *mysqlctl.Mycnf
	MysqlDaemon         mysqlctl.MysqlDaemon
	DBConfigs           *dbconfigs.DBConfigs
	VREngine            *vreplication.Engine

	// exportStats is set only for production tablet.
	exportStats bool

	// statsTabletType is set to expose the current tablet type,
	// only used if exportStats is true.
	statsTabletType *stats.String

	// statsTabletTypeCount exposes the current tablet type as a label,
	// with the value counting the occurrences of the respective tablet type.
	// Useful for Prometheus which doesn't support exporting strings as stat values
	// only used if exportStats is true.
	statsTabletTypeCount *stats.CountersWithSingleLabel

	// statsBackupIsRunning is set to 1 (true) if a backup is running
	// only used if exportStats is true
	statsBackupIsRunning *stats.GaugesWithMultiLabels

	// batchCtx is given to the agent by its creator, and should be used for
	// any background tasks spawned by the agent.
	batchCtx context.Context

	// finalizeReparentCtx represents the background finalize step of a
	// TabletExternallyReparented call.
	finalizeReparentCtx context.Context

	// History of the health checks, public so status
	// pages can display it
	History *history.History

	// actionMutex is there to run only one action at a time. If
	// both agent.actionMutex and agent.mutex needs to be taken,
	// take actionMutex first.
	actionMutex sync.Mutex

	// actionMutexLocked is set to true after we acquire actionMutex,
	// and reset to false when we release it.
	// It is meant as a sanity check to make sure the methods that need
	// to have the actionMutex have it.
	actionMutexLocked bool

	// waitingForMysql is set to true if mysql is not up when we
	// start. That way, we only log once that we're waiting for
	// mysql.  It is protected by actionMutex.
	waitingForMysql bool

	// gotMysqlPort is set when we got the current MySQL port and
	// successfully saved it into the topology. That way we don't
	// keep writing to the topology server on every heartbeat, but we
	// know to try again if we fail to get it.
	// We clear it when the tablet goes from unhealthy to healthy,
	// so we are sure to get the up-to-date version in case of
	// a MySQL server restart.
	// It is protected by actionMutex.
	gotMysqlPort bool

	// initReplication remembers whether an action has initialized
	// replication.  It is protected by actionMutex.
	initReplication bool

	// initialTablet remembers the state of the tablet record at startup.
	// It can be used to notice, for example, if another tablet has taken
	// over the record.
	initialTablet *topodatapb.Tablet

	// orc is an optional client for Orchestrator HTTP API calls.
	// If this is nil, those calls will be skipped.
	// It's only set once in NewActionAgent() and never modified after that.
	orc *orcClient

	// mutex protects all the following fields (that start with '_'),
	// only hold the mutex to update the fields, nothing else.
	mutex sync.Mutex

	// _tablet has the Tablet record we last read from the topology server.
	_tablet *topodatapb.Tablet

	// _disallowQueryService is set to the reason we should be
	// disallowing queries from being served. It is set from changeCallback,
	// and used by healthcheck. If empty, we should allow queries.
	// It is set if the current type is not serving, if a TabletControl
	// tells us not to serve, or if filtered replication is running.
	_disallowQueryService string

	// _enableUpdateStream is true if we should be running the
	// UpdateStream service. Note if we can't start the query
	// service, or if the server health check fails, we will
	// disable UpdateStream.
	_enableUpdateStream bool

	// _blacklistedTables has the list of tables we are currently
	// blacklisting.
	_blacklistedTables []string

	// if the agent is healthy, this is nil. Otherwise it contains
	// the reason we're not healthy.
	_healthy error

	// this is the last time health check ran
	_healthyTime time.Time

	// replication delay the last time we got it
	_replicationDelay time.Duration

	// last time we ran TabletExternallyReparented
	_tabletExternallyReparentedTime time.Time

	// _ignoreHealthErrorExpr can be set by RPC to selectively disable certain
	// healthcheck errors. It should only be accessed while holding actionMutex.
	_ignoreHealthErrorExpr *regexp.Regexp

	// _slaveStopped remembers if we've been told to stop replicating.
	// If it's nil, we'll try to check for the slaveStoppedFile.
	_slaveStopped *bool

	// _lockTablesConnection is used to get and release the table read locks to pause replication
	_lockTablesConnection *dbconnpool.DBConnection
	_lockTablesTimer      *time.Timer
	// _isBackupRunning tells us whether there is a backup that is currently running
	_isBackupRunning bool
}

// NewActionAgent creates a new ActionAgent and registers all the
// associated services.
//
// batchCtx is the context that the agent will use for any background tasks
// it spawns.
func NewActionAgent(
	batchCtx context.Context,
	ts *topo.Server,
	mysqld mysqlctl.MysqlDaemon,
	queryServiceControl tabletserver.Controller,
	tabletAlias *topodatapb.TabletAlias,
	dbcfgs *dbconfigs.DBConfigs,
	mycnf *mysqlctl.Mycnf,
	port, gRPCPort int32,
) (agent *ActionAgent, err error) {
	orc, err := newOrcClient()
	if err != nil {
		return nil, err
	}

	agent = &ActionAgent{
		QueryServiceControl: queryServiceControl,
		HealthReporter:      health.DefaultAggregator,
		batchCtx:            batchCtx,
		TopoServer:          ts,
		TabletAlias:         tabletAlias,
		Cnf:                 mycnf,
		MysqlDaemon:         mysqld,
		DBConfigs:           dbcfgs,
		History:             history.New(historyLength),
		_healthy:            fmt.Errorf("healthcheck not run yet"),
		orc:                 orc,
	}
	// Sanity check for inconsistent flags
	if agent.Cnf == nil && *restoreFromBackup {
		return nil, fmt.Errorf("you cannot enable -restore_from_backup without a my.cnf file")
	}

	agent.registerQueryRuleSources()

	// try to initialize the tablet if we have to
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		return nil, vterrors.Wrap(err, "agent.InitTablet failed")
	}

	// Create the TabletType stats
	agent.exportStats = true
	agent.statsTabletType = stats.NewString("TabletType")
	agent.statsTabletTypeCount = stats.NewCountersWithSingleLabel("TabletTypeCount", "Number of times the tablet changed to the labeled type", "type")
	agent.statsBackupIsRunning = stats.NewGaugesWithMultiLabels("BackupIsRunning", "Whether a backup is running", []string{"mode"})

	var mysqlHost string
	var mysqlPort int32
	if appConfig := dbcfgs.AppWithDB(); appConfig.Host != "" {
		mysqlHost = appConfig.Host
		mysqlPort = int32(appConfig.Port)
	} else {
		// Assume unix socket was specified and try to get the port from mysqld
		var err error
		mysqlPort, err = mysqld.GetMysqlPort()
		if err != nil {
			log.Warningf("Cannot get current mysql port, will try to get it later: %v", err)
		}
	}

	// Start will get the tablet info, and update our state from it
	if err := agent.Start(batchCtx, mysqlHost, int32(mysqlPort), port, gRPCPort, true); err != nil {
		return nil, err
	}

	// The db name is set by the Start function called above
	agent.VREngine = vreplication.NewEngine(ts, tabletAlias.Cell, mysqld, func() binlogplayer.DBClient {
		return binlogplayer.NewDBClient(agent.DBConfigs.FilteredWithDB())
	}, agent.DBConfigs.FilteredWithDB().DbName)
	servenv.OnTerm(agent.VREngine.Close)

	// Run a background task to rebuild the SrvKeyspace in our cell/keyspace
	// if it doesn't exist yet.
	go agent.maybeRebuildKeyspace(agent.initialTablet.Alias.Cell, agent.initialTablet.Keyspace)

	// register the RPC services from the agent
	servenv.OnRun(func() {
		agent.registerQueryService()
	})

	// two cases then:
	// - restoreFromBackup is set: we restore, then initHealthCheck, all
	//   in the background
	// - restoreFromBackup is not set: we initHealthCheck right away
	if *restoreFromBackup {
		go func() {
			// restoreFromBackup will just be a regular action
			// (same as if it was triggered remotely)
			if err := agent.RestoreData(batchCtx, logutil.NewConsoleLogger(), *waitForBackupInterval, false /* deleteBeforeRestore */); err != nil {
				log.Exitf("RestoreFromBackup failed: %v", err)
			}

			// after the restore is done, start health check
			agent.initHealthCheck()
		}()
	} else {
		// Update our state (need the action lock).
		if err := agent.lock(batchCtx); err != nil {
			return nil, err
		}
		if err := agent.refreshTablet(batchCtx, "Start"); err != nil {
			agent.unlock()
			return nil, err
		}
		agent.unlock()

		// synchronously start health check if needed
		agent.initHealthCheck()
	}

	// Start periodic Orchestrator self-registration, if configured.
	if agent.orc != nil {
		go agent.orc.DiscoverLoop(agent)
	}

	return agent, nil
}

// NewTestActionAgent creates an agent for test purposes. Only a
// subset of features are supported now, but we'll add more over time.
func NewTestActionAgent(batchCtx context.Context, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, vtPort, grpcPort int32, mysqlDaemon mysqlctl.MysqlDaemon, preStart func(*ActionAgent)) *ActionAgent {
	ti, err := ts.GetTablet(batchCtx, tabletAlias)
	if err != nil {
		panic(vterrors.Wrap(err, "failed reading tablet"))
	}
	agent := &ActionAgent{
		QueryServiceControl: tabletservermock.NewController(),
		UpdateStream:        binlog.NewUpdateStreamControlMock(),
		HealthReporter:      health.DefaultAggregator,
		batchCtx:            batchCtx,
		TopoServer:          ts,
		TabletAlias:         tabletAlias,
		Cnf:                 nil,
		MysqlDaemon:         mysqlDaemon,
		DBConfigs:           &dbconfigs.DBConfigs{},
		VREngine:            vreplication.NewEngine(ts, tabletAlias.Cell, mysqlDaemon, binlogplayer.NewFakeDBClient, ti.DbName()),
		History:             history.New(historyLength),
		_healthy:            fmt.Errorf("healthcheck not run yet"),
	}
	if preStart != nil {
		preStart(agent)
	}

	// Start will update the topology and setup services.
	if err := agent.Start(batchCtx, "", 0, vtPort, grpcPort, false); err != nil {
		panic(vterrors.Wrapf(err, "agent.Start(%v) failed", tabletAlias))
	}

	// Update our running state. Need to take action lock.
	if err := agent.lock(batchCtx); err != nil {
		panic(vterrors.Wrap(err, "agent.lock() failed"))
	}
	defer agent.unlock()
	if err := agent.refreshTablet(batchCtx, "Start"); err != nil {
		panic(vterrors.Wrapf(err, "agent.refreshTablet(%v) failed", tabletAlias))
	}

	return agent
}

// NewComboActionAgent creates an agent tailored specifically to run
// within the vtcombo binary. It cannot be called concurrently,
// as it changes the flags.
func NewComboActionAgent(batchCtx context.Context, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, vtPort, grpcPort int32, queryServiceControl tabletserver.Controller, dbcfgs *dbconfigs.DBConfigs, mysqlDaemon mysqlctl.MysqlDaemon, keyspace, shard, dbname, tabletType string) *ActionAgent {
	agent := &ActionAgent{
		QueryServiceControl: queryServiceControl,
		UpdateStream:        binlog.NewUpdateStreamControlMock(),
		HealthReporter:      health.DefaultAggregator,
		batchCtx:            batchCtx,
		TopoServer:          ts,
		TabletAlias:         tabletAlias,
		Cnf:                 nil,
		MysqlDaemon:         mysqlDaemon,
		DBConfigs:           dbcfgs,
		VREngine:            vreplication.NewEngine(nil, "", nil, nil, ""),
		gotMysqlPort:        true,
		History:             history.New(historyLength),
		_healthy:            fmt.Errorf("healthcheck not run yet"),
	}
	agent.registerQueryRuleSources()

	// Initialize the tablet.
	*initDbNameOverride = dbname
	*initKeyspace = keyspace
	*initShard = shard
	*initTabletType = tabletType
	if err := agent.InitTablet(vtPort, grpcPort); err != nil {
		panic(vterrors.Wrap(err, "agent.InitTablet failed"))
	}

	// Start the agent.
	if err := agent.Start(batchCtx, "", 0, vtPort, grpcPort, false); err != nil {
		panic(vterrors.Wrapf(err, "agent.Start(%v) failed", tabletAlias))
	}

	// And update our running state (need to take the Action lock).
	if err := agent.lock(batchCtx); err != nil {
		panic(vterrors.Wrap(err, "agent.lock() failed"))
	}
	defer agent.unlock()
	if err := agent.refreshTablet(batchCtx, "Start"); err != nil {
		panic(vterrors.Wrapf(err, "agent.refreshTablet(%v) failed", tabletAlias))
	}

	return agent
}

// registerQueryRuleSources registers query rule sources under control of agent
func (agent *ActionAgent) registerQueryRuleSources() {
	agent.QueryServiceControl.RegisterQueryRuleSource(blacklistQueryRules)
}

func (agent *ActionAgent) setTablet(tablet *topodatapb.Tablet) {
	agent.mutex.Lock()
	agent._tablet = proto.Clone(tablet).(*topodatapb.Tablet)
	agent.mutex.Unlock()
}

// Tablet reads the stored Tablet from the agent, protected by mutex.
func (agent *ActionAgent) Tablet() *topodatapb.Tablet {
	agent.mutex.Lock()
	tablet := proto.Clone(agent._tablet).(*topodatapb.Tablet)
	agent.mutex.Unlock()
	return tablet
}

// Healthy reads the result of the latest healthcheck, protected by mutex.
// If that status is too old, it means healthcheck hasn't run for a while,
// and is probably stuck, this is not good, we're not healthy.
func (agent *ActionAgent) Healthy() (time.Duration, error) {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()

	healthy := agent._healthy
	if healthy == nil {
		timeSinceLastCheck := time.Since(agent._healthyTime)
		if timeSinceLastCheck > *healthCheckInterval*3 {
			healthy = fmt.Errorf("last health check is too old: %s > %s", timeSinceLastCheck, *healthCheckInterval*3)
		}
	}

	return agent._replicationDelay, healthy
}

// BlacklistedTables returns the list of currently blacklisted tables.
func (agent *ActionAgent) BlacklistedTables() []string {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()
	return agent._blacklistedTables
}

// DisallowQueryService returns the reason the query service should be
// disabled, if any.
func (agent *ActionAgent) DisallowQueryService() string {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()
	return agent._disallowQueryService
}

// EnableUpdateStream returns if we should enable update stream or not
func (agent *ActionAgent) EnableUpdateStream() bool {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()
	return agent._enableUpdateStream
}

func (agent *ActionAgent) slaveStopped() bool {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()

	// If we already know the value, don't bother checking the file.
	if agent._slaveStopped != nil {
		return *agent._slaveStopped
	}

	// If there's no Cnf file, don't read state.
	if agent.Cnf == nil {
		return false
	}

	// If the marker file exists, we're stopped.
	// Treat any read error as if the file doesn't exist.
	_, err := os.Stat(path.Join(agent.Cnf.TabletDir(), slaveStoppedFile))
	slaveStopped := err == nil
	agent._slaveStopped = &slaveStopped
	return slaveStopped
}

func (agent *ActionAgent) setSlaveStopped(slaveStopped bool) {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()

	agent._slaveStopped = &slaveStopped

	// Make a best-effort attempt to persist the value across tablet restarts.
	// We store a marker in the filesystem so it works regardless of whether
	// mysqld is running, and so it's tied to this particular instance of the
	// tablet data dir (the one that's paused at a known replication position).
	if agent.Cnf == nil {
		return
	}
	tabletDir := agent.Cnf.TabletDir()
	if tabletDir == "" {
		return
	}
	markerFile := path.Join(tabletDir, slaveStoppedFile)
	if slaveStopped {
		file, err := os.Create(markerFile)
		if err == nil {
			file.Close()
		}
	} else {
		os.Remove(markerFile)
	}
}

func (agent *ActionAgent) setServicesDesiredState(disallowQueryService string, enableUpdateStream bool) {
	agent.mutex.Lock()
	agent._disallowQueryService = disallowQueryService
	agent._enableUpdateStream = enableUpdateStream
	agent.mutex.Unlock()
}

func (agent *ActionAgent) setBlacklistedTables(value []string) {
	agent.mutex.Lock()
	agent._blacklistedTables = value
	agent.mutex.Unlock()
}

func (agent *ActionAgent) verifyTopology(ctx context.Context) {
	if err := topo.Validate(ctx, agent.TopoServer, agent.TabletAlias); err != nil {
		// Don't stop, it's not serious enough, this is likely transient.
		log.Warningf("tablet validate failed: %v %v", agent.TabletAlias, err)
	}
}

// Start validates and updates the topology records for the tablet, and performs
// the initial state change callback to start tablet services.
// If initUpdateStream is set, update stream service will also be registered.
func (agent *ActionAgent) Start(ctx context.Context, mysqlHost string, mysqlPort, vtPort, gRPCPort int32, initUpdateStream bool) error {
	// find our hostname as fully qualified, and IP
	hostname := *tabletHostname
	if hostname == "" {
		var err error
		hostname, err = netutil.FullyQualifiedHostname()
		if err != nil {
			return err
		}
	}

	// If mysqlHost is not set, a unix socket was specified. So, assume
	// it's the same as the current host.
	if mysqlHost == "" {
		mysqlHost = hostname
	}

	// Update bind addr for mysql and query service in the tablet node.
	f := func(tablet *topodatapb.Tablet) error {
		tablet.Hostname = hostname
		tablet.MysqlHostname = mysqlHost
		if tablet.PortMap == nil {
			tablet.PortMap = make(map[string]int32)
		}
		if mysqlPort != 0 {
			// only overwrite mysql port if we know it, otherwise
			// leave it as is.
			topoproto.SetMysqlPort(tablet, int32(mysqlPort))
		}
		if vtPort != 0 {
			tablet.PortMap["vt"] = vtPort
		} else {
			delete(tablet.PortMap, "vt")
		}
		delete(tablet.PortMap, "vts")
		if gRPCPort != 0 {
			tablet.PortMap["grpc"] = gRPCPort
		} else {
			delete(tablet.PortMap, "grpc")
		}

		// Save the original tablet for ownership tests later.
		agent.initialTablet = tablet
		return nil
	}
	if _, err := agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias, f); err != nil {
		return err
	}

	// Verify the topology is correct.
	agent.verifyTopology(ctx)

	// Get and fix the dbname if necessary, only for real instances.
	if !agent.DBConfigs.IsZero() {
		dbname := topoproto.TabletDbName(agent.initialTablet)
		agent.DBConfigs.DBName.Set(dbname)
	}

	// Create and register the RPC services from UpdateStream.
	// (it needs the dbname, so it has to be delayed up to here,
	// but it has to be before updateState below that may use it)
	if initUpdateStream {
		us := binlog.NewUpdateStream(agent.TopoServer, agent.initialTablet.Keyspace, agent.TabletAlias.Cell, agent.DBConfigs.DbaWithDB(), agent.QueryServiceControl.SchemaEngine())
		agent.UpdateStream = us
		servenv.OnRun(func() {
			us.RegisterService()
		})
	}
	servenv.OnTerm(func() {
		// Disable UpdateStream (if any) upon entering lameduck.
		// We do this regardless of initUpdateStream, since agent.UpdateStream
		// may have been set from elsewhere.
		if agent.UpdateStream != nil {
			agent.UpdateStream.Disable()
		}
	})

	// initialize tablet server
	if err := agent.QueryServiceControl.InitDBConfig(querypb.Target{
		Keyspace:   agent.initialTablet.Keyspace,
		Shard:      agent.initialTablet.Shard,
		TabletType: agent.initialTablet.Type,
	}, agent.DBConfigs); err != nil {
		return vterrors.Wrap(err, "failed to InitDBConfig")
	}

	// export a few static variables
	if agent.exportStats {
		statsKeyspace := stats.NewString("TabletKeyspace")
		statsShard := stats.NewString("TabletShard")
		statsKeyRangeStart := stats.NewString("TabletKeyRangeStart")
		statsKeyRangeEnd := stats.NewString("TabletKeyRangeEnd")
		statsAlias := stats.NewString("TabletAlias")

		statsKeyspace.Set(agent.initialTablet.Keyspace)
		statsShard.Set(agent.initialTablet.Shard)
		if key.KeyRangeIsPartial(agent.initialTablet.KeyRange) {
			statsKeyRangeStart.Set(hex.EncodeToString(agent.initialTablet.KeyRange.Start))
			statsKeyRangeEnd.Set(hex.EncodeToString(agent.initialTablet.KeyRange.End))
		}
		statsAlias.Set(topoproto.TabletAliasString(agent.initialTablet.Alias))
	}

	// Initialize the current tablet to match our current running
	// state: Has most field filled in, but type is UNKNOWN.
	// Subsequents calls to updateState or refreshTablet
	// will then work as expected.
	startingTablet := proto.Clone(agent.initialTablet).(*topodatapb.Tablet)
	startingTablet.Type = topodatapb.TabletType_UNKNOWN
	agent.setTablet(startingTablet)

	return nil
}

// Close prepares a tablet for shutdown. First we check our tablet ownership and
// then prune the tablet topology entry of all post-init fields. This prevents
// stale identifiers from hanging around in topology.
func (agent *ActionAgent) Close() {
	// cleanup initialized fields in the tablet entry
	f := func(tablet *topodatapb.Tablet) error {
		if err := topotools.CheckOwnership(agent.initialTablet, tablet); err != nil {
			return err
		}
		tablet.Hostname = ""
		tablet.MysqlHostname = ""
		tablet.PortMap = nil
		return nil
	}
	if _, err := agent.TopoServer.UpdateTabletFields(context.Background(), agent.TabletAlias, f); err != nil {
		log.Warningf("Failed to update tablet record, may contain stale identifiers: %v", err)
	}
}

// Stop shuts down the agent. Normally this is not necessary, since we use
// servenv OnTerm and OnClose hooks to coordinate shutdown automatically,
// while taking lameduck into account. However, this may be useful for tests,
// when you want to clean up an agent immediately.
func (agent *ActionAgent) Stop() {
	if agent.UpdateStream != nil {
		agent.UpdateStream.Disable()
	}
	agent.VREngine.Close()
	if agent.MysqlDaemon != nil {
		agent.MysqlDaemon.Close()
	}
}

// hookExtraEnv returns the map to pass to local hooks
func (agent *ActionAgent) hookExtraEnv() map[string]string {
	return map[string]string{"TABLET_ALIAS": topoproto.TabletAliasString(agent.TabletAlias)}
}

// checkTabletMysqlPort will check the mysql port for the tablet is good,
// and if not will try to update it. It returns the modified Tablet record,
// if it was updated successfully in the topology server.
//
// We use the agent.waitingForMysql flag to log only the first
// error we get when trying to get the port from MySQL, and store it in the
// topology. That way we don't spam the logs.
//
// The actionMutex lock must be held when calling this function.
func (agent *ActionAgent) checkTabletMysqlPort(ctx context.Context, tablet *topodatapb.Tablet) *topodatapb.Tablet {
	agent.checkLock()
	mport, err := agent.MysqlDaemon.GetMysqlPort()
	if err != nil {
		// Only log the first time, so we don't spam the logs.
		if !agent.waitingForMysql {
			log.Warningf("Cannot get current mysql port, not checking it (will retry at healthcheck interval): %v", err)
			agent.waitingForMysql = true
		}
		return nil
	}

	if mport == topoproto.MysqlPort(tablet) {
		// The topology record contains the right port.
		// Remember we successfully checked it, and that we're
		// not waiting on MySQL to start any more.
		agent.gotMysqlPort = true
		agent.waitingForMysql = false
		return nil
	}

	// Update the port in the topology. Use a shorter timeout, so if
	// the topo server is busy / throttling us, we don't hang forever here.
	// The healthcheck go routine will try again next time.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if !agent.waitingForMysql {
		log.Warningf("MySQL port has changed from %v to %v, updating it in tablet record", topoproto.MysqlPort(tablet), mport)
	}
	newTablet, err := agent.TopoServer.UpdateTabletFields(ctx, tablet.Alias, func(t *topodatapb.Tablet) error {
		if err := topotools.CheckOwnership(agent.initialTablet, tablet); err != nil {
			return err
		}
		topoproto.SetMysqlPort(t, mport)
		return nil
	})
	if err != nil {
		if !agent.waitingForMysql {
			// After this, we will try again on every
			// heartbeat to go through this code, but we
			// won't log it.
			log.Warningf("Failed to update tablet record, may use old mysql port: %v", err)
			agent.waitingForMysql = true
		}
		return nil
	}

	// Update worked, return the new record, so the agent can save it.
	// This should not happen often, so we can log it.
	log.Infof("MySQL port has changed from %v to %v, successfully updated the tablet record in topology", topoproto.MysqlPort(tablet), mport)
	agent.gotMysqlPort = true
	agent.waitingForMysql = false
	return newTablet
}

// withRetry will exponentially back off and retry a function upon
// failure, until the context is Done(), or the function returned with
// no error. We use this at startup with a context timeout set to the
// value of the init_timeout flag, so we can try to modify the
// topology over a longer period instead of dying right away.
func (agent *ActionAgent) withRetry(ctx context.Context, description string, work func() error) error {
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
