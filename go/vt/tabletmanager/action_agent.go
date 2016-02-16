// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/history"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletservermock"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// Query rules from keyrange
const keyrangeQueryRules string = "KeyrangeQueryRules"

var (
	tabletHostname = flag.String("tablet_hostname", "", "if not empty, this hostname will be assumed instead of trying to resolve it")
)

// ActionAgent is the main class for the agent.
type ActionAgent struct {
	// The following fields are set during creation
	QueryServiceControl tabletserver.Controller
	UpdateStream        binlog.UpdateStreamControl
	HealthReporter      health.Reporter
	TopoServer          topo.Server
	TabletAlias         *topodatapb.TabletAlias
	MysqlDaemon         mysqlctl.MysqlDaemon
	DBConfigs           dbconfigs.DBConfigs
	SchemaOverrides     []tabletserver.SchemaOverride
	BinlogPlayerMap     *BinlogPlayerMap

	// exportStats is set only for production tablet.
	exportStats bool

	// statsTabletType is set to expose the current tablet type,
	// only used if exportStats is true.
	statsTabletType *stats.String

	// batchCtx is given to the agent by its creator, and should be used for
	// any background tasks spawned by the agent.
	batchCtx context.Context

	// finalizeReparentCtx represents the background finalize step of a
	// TabletExternallyReparented call.
	finalizeReparentCtx context.Context

	// History of the health checks, public so status
	// pages can display it
	History            *history.History
	lastHealthMapCount *stats.Int

	// actionMutex is there to run only one action at a time. If
	// both agent.actionMutex and agent.mutex needs to be taken,
	// take actionMutex first.
	actionMutex sync.Mutex

	// initReplication remembers whether an action has initialized
	// replication.  It is protected by actionMutex.
	initReplication bool

	// initialTablet remembers the state of the tablet record at startup.
	// It can be used to notice, for example, if another tablet has taken over
	// the record.
	initialTablet *topodatapb.Tablet

	// mutex protects the following fields, only hold the mutex
	// to update the fields, nothing else.
	mutex            sync.Mutex
	_tablet          *topodatapb.Tablet
	_tabletControl   *topodatapb.Shard_TabletControl
	_waitingForMysql bool

	// if the agent is healthy, this is nil. Otherwise it contains
	// the reason we're not healthy.
	_healthy error

	// this is the last time health check ran
	_healthyTime time.Time

	// replication delay the last time we got it
	_replicationDelay time.Duration

	// last time we ran TabletExternallyReparented
	_tabletExternallyReparentedTime time.Time
}

func loadSchemaOverrides(overridesFile string) []tabletserver.SchemaOverride {
	var schemaOverrides []tabletserver.SchemaOverride
	if overridesFile == "" {
		return schemaOverrides
	}
	data, err := ioutil.ReadFile(overridesFile)
	if err != nil {
		log.Warningf("can't read overridesFile %v: %v", overridesFile, err)
		return schemaOverrides
	}
	if err = json.Unmarshal(data, &schemaOverrides); err != nil {
		log.Warningf("can't parse overridesFile %v: %v", overridesFile, err)
		return schemaOverrides
	}
	data, _ = json.MarshalIndent(schemaOverrides, "", "  ")
	log.Infof("schemaOverrides: %s\n", data)
	return schemaOverrides
}

// NewActionAgent creates a new ActionAgent and registers all the
// associated services.
//
// batchCtx is the context that the agent will use for any background tasks
// it spawns.
func NewActionAgent(
	batchCtx context.Context,
	mysqld mysqlctl.MysqlDaemon,
	queryServiceControl tabletserver.Controller,
	tabletAlias *topodatapb.TabletAlias,
	dbcfgs dbconfigs.DBConfigs,
	mycnf *mysqlctl.Mycnf,
	port, gRPCPort int32,
	overridesFile string,
) (agent *ActionAgent, err error) {
	schemaOverrides := loadSchemaOverrides(overridesFile)

	topoServer := topo.GetServer()

	agent = &ActionAgent{
		QueryServiceControl: queryServiceControl,
		HealthReporter:      health.DefaultAggregator,
		batchCtx:            batchCtx,
		TopoServer:          topoServer,
		TabletAlias:         tabletAlias,
		MysqlDaemon:         mysqld,
		DBConfigs:           dbcfgs,
		SchemaOverrides:     schemaOverrides,
		History:             history.New(historyLength),
		lastHealthMapCount:  stats.NewInt("LastHealthMapCount"),
		_healthy:            fmt.Errorf("healthcheck not run yet"),
	}
	agent.registerQueryRuleSources()

	// try to initialize the tablet if we have to
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		return nil, fmt.Errorf("agent.InitTablet failed: %v", err)
	}

	// Publish and set the TargetTabletType. Not a global var
	// since it should never be changed.
	statsTabletType := stats.NewString("TargetTabletType")
	statsTabletType.Set(*targetTabletType)

	// Create the TabletType stats
	agent.exportStats = true
	agent.statsTabletType = stats.NewString("TabletType")

	// Start the binlog player services, not playing at start.
	agent.BinlogPlayerMap = NewBinlogPlayerMap(topoServer, mysqld, func() binlogplayer.VtClient {
		return binlogplayer.NewDbClient(&agent.DBConfigs.Filtered)
	})
	// Stop all binlog players upon entering lameduck.
	servenv.OnTerm(agent.BinlogPlayerMap.StopAllPlayersAndReset)
	RegisterBinlogPlayerMap(agent.BinlogPlayerMap)

	// try to figure out the mysql port
	mysqlPort := mycnf.MysqlPort
	if mysqlPort == 0 {
		// we don't know the port, try to get it from mysqld
		var err error
		mysqlPort, err = mysqld.GetMysqlPort()
		if err != nil {
			log.Warningf("Cannot get current mysql port, will use 0 for now: %v", err)
		}
	}

	// Start will get the tablet info, and update our state from it
	if err := agent.Start(batchCtx, int32(mysqlPort), port, gRPCPort, true); err != nil {
		return nil, err
	}

	// register the RPC services from the agent
	agent.registerQueryService()

	// two cases then:
	// - restoreFromBackup is set: we restore, then initHealthCheck, all
	//   in the background
	// - restoreFromBackup is not set: we initHealthCheck right away
	if *restoreFromBackup {
		go func() {
			// restoreFromBackup wil just be a regular action
			// (same as if it was triggered remotely)
			if err := agent.RestoreFromBackup(batchCtx); err != nil {
				println(fmt.Sprintf("RestoreFromBackup failed: %v", err))
				log.Fatalf("RestoreFromBackup failed: %v", err)
			}

			// after the restore is done, start health check
			agent.initHealthCheck()
		}()
	} else {
		// synchronously start health check if needed
		agent.initHealthCheck()
	}

	return agent, nil
}

// NewTestActionAgent creates an agent for test purposes. Only a
// subset of features are supported now, but we'll add more over time.
func NewTestActionAgent(batchCtx context.Context, ts topo.Server, tabletAlias *topodatapb.TabletAlias, vtPort, grpcPort int32, mysqlDaemon mysqlctl.MysqlDaemon) *ActionAgent {
	agent := &ActionAgent{
		QueryServiceControl: tabletservermock.NewController(),
		UpdateStream:        binlog.NewUpdateStreamControlMock(),
		HealthReporter:      health.DefaultAggregator,
		batchCtx:            batchCtx,
		TopoServer:          ts,
		TabletAlias:         tabletAlias,
		MysqlDaemon:         mysqlDaemon,
		DBConfigs:           dbconfigs.DBConfigs{},
		SchemaOverrides:     nil,
		BinlogPlayerMap:     nil,
		History:             history.New(historyLength),
		lastHealthMapCount:  new(stats.Int),
		_healthy:            fmt.Errorf("healthcheck not run yet"),
	}
	if err := agent.Start(batchCtx, 0, vtPort, grpcPort, false); err != nil {
		panic(fmt.Errorf("agent.Start(%v) failed: %v", tabletAlias, err))
	}
	return agent
}

// NewComboActionAgent creates an agent tailored specifically to run
// within the vtcombo binary. It cannot be called concurrently,
// as it changes the flags.
func NewComboActionAgent(batchCtx context.Context, ts topo.Server, tabletAlias *topodatapb.TabletAlias, vtPort, grpcPort int32, queryServiceControl tabletserver.Controller, dbcfgs dbconfigs.DBConfigs, mysqlDaemon mysqlctl.MysqlDaemon, keyspace, shard, dbname, tabletType string) *ActionAgent {
	agent := &ActionAgent{
		QueryServiceControl: queryServiceControl,
		UpdateStream:        binlog.NewUpdateStreamControlMock(),
		HealthReporter:      health.DefaultAggregator,
		batchCtx:            batchCtx,
		TopoServer:          ts,
		TabletAlias:         tabletAlias,
		MysqlDaemon:         mysqlDaemon,
		DBConfigs:           dbcfgs,
		SchemaOverrides:     nil,
		BinlogPlayerMap:     nil,
		History:             history.New(historyLength),
		lastHealthMapCount:  new(stats.Int),
		_healthy:            fmt.Errorf("healthcheck not run yet"),
	}
	agent.registerQueryRuleSources()

	// initialize the tablet
	*initDbNameOverride = dbname
	*initKeyspace = keyspace
	*initShard = shard
	*initTabletType = tabletType
	if err := agent.InitTablet(vtPort, grpcPort); err != nil {
		panic(fmt.Errorf("agent.InitTablet failed: %v", err))
	}

	// and start the agent
	if err := agent.Start(batchCtx, 0, vtPort, grpcPort, false); err != nil {
		panic(fmt.Errorf("agent.Start(%v) failed: %v", tabletAlias, err))
	}
	return agent
}

// registerQueryRuleSources registers query rule sources under control of agent
func (agent *ActionAgent) registerQueryRuleSources() {
	agent.QueryServiceControl.RegisterQueryRuleSource(blacklistQueryRules)
}

// updateTabletFromTopo will read the tablet record from the topology,
// save it in the agent's tablet record, and return it.
func (agent *ActionAgent) updateTabletFromTopo(ctx context.Context) (*topodatapb.Tablet, error) {
	ti, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return nil, err
	}
	agent.setTablet(ti.Tablet)
	return ti.Tablet, nil
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

// BlacklistedTables reads the list of blacklisted tables from the TabletControl
// record (if any) stored in the agent, protected by mutex.
func (agent *ActionAgent) BlacklistedTables() []string {
	var blacklistedTables []string
	agent.mutex.Lock()
	if agent._tabletControl != nil {
		blacklistedTables = agent._tabletControl.BlacklistedTables
	}
	agent.mutex.Unlock()
	return blacklistedTables
}

// DisableQueryService reads the DisableQueryService field from the TabletControl
// record (if any) stored in the agent, protected by mutex.
func (agent *ActionAgent) DisableQueryService() bool {
	disable := false
	agent.mutex.Lock()
	if agent._tabletControl != nil {
		disable = agent._tabletControl.DisableQueryService
	}
	agent.mutex.Unlock()
	return disable
}

func (agent *ActionAgent) setTabletControl(tc *topodatapb.Shard_TabletControl) {
	agent.mutex.Lock()
	agent._tabletControl = proto.Clone(tc).(*topodatapb.Shard_TabletControl)
	agent.mutex.Unlock()
}

func (agent *ActionAgent) verifyTopology(ctx context.Context) error {
	tablet := agent.Tablet()
	if tablet == nil {
		return fmt.Errorf("agent._tablet is nil")
	}

	if err := topo.Validate(ctx, agent.TopoServer, agent.TabletAlias); err != nil {
		// Don't stop, it's not serious enough, this is likely transient.
		log.Warningf("tablet validate failed: %v %v", agent.TabletAlias, err)
	}

	return nil
}

func (agent *ActionAgent) verifyServingAddrs(ctx context.Context) error {
	tablet := agent.Tablet()
	if !topo.IsRunningQueryService(tablet.Type) {
		return nil
	}

	// Check to see our address is registered in the right place.
	return topotools.UpdateTabletEndpoints(ctx, agent.TopoServer, tablet)
}

// Start validates and updates the topology records for the tablet, and performs
// the initial state change callback to start tablet services.
// If initUpdateStream is set, update stream service will also be registered.
func (agent *ActionAgent) Start(ctx context.Context, mysqlPort, vtPort, gRPCPort int32, initUpdateStream bool) error {
	var err error
	if _, err = agent.updateTabletFromTopo(ctx); err != nil {
		return err
	}

	// find our hostname as fully qualified, and IP
	hostname := *tabletHostname
	if hostname == "" {
		hostname, err = netutil.FullyQualifiedHostname()
		if err != nil {
			return err
		}
	}
	ipAddrs, err := net.LookupHost(hostname)
	if err != nil {
		return err
	}
	ipAddr := ipAddrs[0]

	// Update bind addr for mysql and query service in the tablet node.
	f := func(tablet *topodatapb.Tablet) error {
		tablet.Hostname = hostname
		tablet.Ip = ipAddr
		if tablet.PortMap == nil {
			tablet.PortMap = make(map[string]int32)
		}
		if mysqlPort != 0 {
			// only overwrite mysql port if we know it, otherwise
			// leave it as is.
			tablet.PortMap["mysql"] = mysqlPort
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
		return nil
	}
	if _, err := agent.TopoServer.UpdateTabletFields(ctx, agent.Tablet().Alias, f); err != nil {
		return err
	}

	// Reread to get the changes we just made
	tablet, err := agent.updateTabletFromTopo(ctx)
	if err != nil {
		return err
	}

	// Save the original tablet record as it is now (at startup).
	agent.initialTablet = proto.Clone(tablet).(*topodatapb.Tablet)

	if err = agent.verifyTopology(ctx); err != nil {
		return err
	}

	if err = agent.verifyServingAddrs(ctx); err != nil {
		return err
	}

	// get and fix the dbname if necessary
	if !agent.DBConfigs.IsZero() {
		// Only for real instances
		// Update our DB config to match the info we have in the tablet
		if agent.DBConfigs.App.DbName == "" {
			agent.DBConfigs.App.DbName = topoproto.TabletDbName(tablet)
		}
		if agent.DBConfigs.Filtered.DbName == "" {
			agent.DBConfigs.Filtered.DbName = topoproto.TabletDbName(tablet)
		}
		agent.DBConfigs.App.Keyspace = tablet.Keyspace
		agent.DBConfigs.App.Shard = tablet.Shard
	}

	// create and register the RPC services from UpdateStream
	// (it needs the dbname, so it has to be delayed up to here,
	// but it has to be before updateState below that may use it)
	if initUpdateStream {
		us := binlog.NewUpdateStream(agent.MysqlDaemon, agent.DBConfigs.App.DbName)
		us.RegisterService()
		agent.UpdateStream = us
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
		Keyspace:   tablet.Keyspace,
		Shard:      tablet.Shard,
		TabletType: tablet.Type,
	}, agent.DBConfigs, agent.SchemaOverrides, agent.MysqlDaemon); err != nil {
		return fmt.Errorf("failed to InitDBConfig: %v", err)
	}

	// export a few static variables
	if agent.exportStats {
		statsKeyspace := stats.NewString("TabletKeyspace")
		statsShard := stats.NewString("TabletShard")
		statsKeyRangeStart := stats.NewString("TabletKeyRangeStart")
		statsKeyRangeEnd := stats.NewString("TabletKeyRangeEnd")

		statsKeyspace.Set(tablet.Keyspace)
		statsShard.Set(tablet.Shard)
		if key.KeyRangeIsPartial(tablet.KeyRange) {
			statsKeyRangeStart.Set(hex.EncodeToString(tablet.KeyRange.Start))
			statsKeyRangeEnd.Set(hex.EncodeToString(tablet.KeyRange.End))
		}
	}

	// initialize the key range query rule
	if err := agent.initializeKeyRangeRule(ctx, tablet.Keyspace, tablet.KeyRange); err != nil {
		return err
	}

	// update our state
	oldTablet := &topodatapb.Tablet{}
	if err = agent.updateState(ctx, oldTablet, "Start"); err != nil {
		log.Warningf("Initial updateState failed, will need a state change before running properly: %v", err)
	}

	// run a background task to rebuild the SrvKeyspace in our cell/keyspace
	// if it doesn't exist yet
	go agent.maybeRebuildKeyspace(tablet.Alias.Cell, tablet.Keyspace)

	return nil
}

// Stop shuts down the agent. Normally this is not necessary, since we use
// servenv OnTerm and OnClose hooks to coordinate shutdown automatically,
// while taking lameduck into account. However, this may be useful for tests,
// when you want to clean up an agent immediately.
func (agent *ActionAgent) Stop() {
	if agent.UpdateStream != nil {
		agent.UpdateStream.Disable()
	}
	if agent.BinlogPlayerMap != nil {
		agent.BinlogPlayerMap.StopAllPlayersAndReset()
	}
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
// if it was changed.
func (agent *ActionAgent) checkTabletMysqlPort(ctx context.Context, tablet *topodatapb.Tablet) *topodatapb.Tablet {
	mport, err := agent.MysqlDaemon.GetMysqlPort()
	if err != nil {
		log.Warningf("Cannot get current mysql port, not checking it: %v", err)
		return nil
	}

	if mport == tablet.PortMap["mysql"] {
		return nil
	}

	log.Warningf("MySQL port has changed from %v to %v, updating it in tablet record", tablet.PortMap["mysql"], mport)
	newTablet, err := agent.TopoServer.UpdateTabletFields(ctx, tablet.Alias, func(t *topodatapb.Tablet) error {
		t.PortMap["mysql"] = mport
		return nil
	})
	if err != nil {
		log.Warningf("Failed to update tablet record, may use old mysql port")
		return nil
	}

	// update worked, return the new record
	return newTablet
}

// initializeKeyRangeRule will create and set the key range rules
func (agent *ActionAgent) initializeKeyRangeRule(ctx context.Context, keyspace string, keyRange *topodatapb.KeyRange) error {
	// check we have a partial key range
	if !key.KeyRangeIsPartial(keyRange) {
		log.Infof("Tablet covers the full KeyRange, not adding KeyRange query rule")
		return nil
	}

	// read the keyspace to get the sharding column name
	keyspaceInfo, err := agent.TopoServer.GetKeyspace(ctx, keyspace)
	if err != nil {
		return fmt.Errorf("cannot read keyspace %v to get sharding key: %v", keyspace, err)
	}
	if keyspaceInfo.ShardingColumnName == "" {
		return fmt.Errorf("keyspace %v has an empty ShardingColumnName, cannot setup KeyRange rule", keyspace)
	}

	// create the rules
	log.Infof("Restricting to keyrange: %v", key.KeyRangeString(keyRange))
	keyrangeRules := tabletserver.NewQueryRules()
	dmlPlans := []struct {
		planID   planbuilder.PlanType
		onAbsent bool
	}{
		{planbuilder.PlanInsertPK, true},
		{planbuilder.PlanInsertSubquery, true},
		{planbuilder.PlanPassDML, false},
		{planbuilder.PlanDMLPK, false},
		{planbuilder.PlanDMLSubquery, false},
		{planbuilder.PlanUpsertPK, false},
	}
	for _, plan := range dmlPlans {
		qr := tabletserver.NewQueryRule(
			fmt.Sprintf("enforce %v range for %v", keyspaceInfo.ShardingColumnName, plan.planID),
			fmt.Sprintf("%v_not_in_range_%v", keyspaceInfo.ShardingColumnName, plan.planID),
			tabletserver.QRFail,
		)
		qr.AddPlanCond(plan.planID)
		err := qr.AddBindVarCond(keyspaceInfo.ShardingColumnName, plan.onAbsent, true, tabletserver.QRNotIn, keyRange)
		if err != nil {
			return fmt.Errorf("Unable to add key range rule: %v", err)
		}
		keyrangeRules.Add(qr)
	}

	// and load them
	agent.QueryServiceControl.RegisterQueryRuleSource(keyrangeQueryRules)
	if err := agent.QueryServiceControl.SetQueryRules(keyrangeQueryRules, keyrangeRules); err != nil {
		return fmt.Errorf("failed to load query rule set %s: %s", keyrangeQueryRules, err)
	}
	return nil
}
