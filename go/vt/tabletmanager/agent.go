// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package tabletmanager exports the ActionAgent object. It keeps the local tablet
state, starts / stops all associated services (query service,
update stream, binlog players, ...), and handles tabletmanager RPCs
to update the state.

The agent is responsible for maintaining the tablet record in the
topology server. Only 'ScrapTablet -force' and 'DeleteTablet'
should be run by other processes, everything else should ask
the tablet server to make the change.

Most RPC calls lock the actionMutex, except the easy read-only ones.
RPC calls that change the tablet record will also call updateState.

See rpc_server.go for all cases, and which actions take the actionMutex,
and which run changeCallback.
*/
package tabletmanager

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/history"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager/events"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	tabletHostname = flag.String("tablet_hostname", "", "if not empty, this hostname will be assumed instead of trying to resolve it")
)

// ActionAgent is the main class for the agent.
type ActionAgent struct {
	// The following fields are set during creation
	QueryServiceControl tabletserver.QueryServiceControl
	HealthReporter      health.Reporter
	TopoServer          topo.Server
	TabletAlias         *pb.TabletAlias
	MysqlDaemon         mysqlctl.MysqlDaemon
	DBConfigs           *dbconfigs.DBConfigs
	SchemaOverrides     []tabletserver.SchemaOverride
	BinlogPlayerMap     *BinlogPlayerMap
	LockTimeout         time.Duration
	// batchCtx is given to the agent by its creator, and should be used for
	// any background tasks spawned by the agent.
	batchCtx context.Context
	// finalizeReparentCtx represents the background finalize step of a
	// TabletExternallyReparented call.
	finalizeReparentCtx context.Context

	// This is the History of the health checks, public so status
	// pages can display it
	History            *history.History
	lastHealthMapCount *stats.Int

	// actionMutex is there to run only one action at a time. If
	// both agent.actionMutex and agent.mutex needs to be taken,
	// take actionMutex first.
	actionMutex sync.Mutex

	// initReplication remembers whether an action has initialized replication.
	// It is protected by actionMutex.
	initReplication bool

	// mutex protects the following fields, only hold the mutex
	// to update the fields, nothing else.
	mutex            sync.Mutex
	_tablet          *topo.TabletInfo
	_tabletControl   *pb.Shard_TabletControl
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
	queryServiceControl tabletserver.QueryServiceControl,
	tabletAlias *pb.TabletAlias,
	dbcfgs *dbconfigs.DBConfigs,
	mycnf *mysqlctl.Mycnf,
	port, gRPCPort int32,
	overridesFile string,
	lockTimeout time.Duration,
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
		LockTimeout:         lockTimeout,
		History:             history.New(historyLength),
		lastHealthMapCount:  stats.NewInt("LastHealthMapCount"),
		_healthy:            fmt.Errorf("healthcheck not run yet"),
	}

	// try to initialize the tablet if we have to
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		return nil, fmt.Errorf("agent.InitTablet failed: %v", err)
	}

	// Publish and set the TargetTabletType. Not a global var
	// since it should never be changed.
	statsTabletType := stats.NewString("TargetTabletType")
	statsTabletType.Set(*targetTabletType)

	// Start the binlog player services, not playing at start.
	agent.BinlogPlayerMap = NewBinlogPlayerMap(topoServer, &dbcfgs.Filtered, mysqld)
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

	if err := agent.Start(batchCtx, int32(mysqlPort), port, gRPCPort); err != nil {
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
func NewTestActionAgent(batchCtx context.Context, ts topo.Server, tabletAlias *pb.TabletAlias, vtPort, grpcPort int32, mysqlDaemon mysqlctl.MysqlDaemon) *ActionAgent {
	agent := &ActionAgent{
		QueryServiceControl: tabletserver.NewTestQueryServiceControl(),
		HealthReporter:      health.DefaultAggregator,
		batchCtx:            batchCtx,
		TopoServer:          ts,
		TabletAlias:         tabletAlias,
		MysqlDaemon:         mysqlDaemon,
		DBConfigs:           nil,
		SchemaOverrides:     nil,
		BinlogPlayerMap:     nil,
		History:             history.New(historyLength),
		lastHealthMapCount:  new(stats.Int),
		_healthy:            fmt.Errorf("healthcheck not run yet"),
	}
	if err := agent.Start(batchCtx, 0, vtPort, grpcPort); err != nil {
		panic(fmt.Errorf("agent.Start(%v) failed: %v", tabletAlias, err))
	}
	return agent
}

func (agent *ActionAgent) updateState(ctx context.Context, oldTablet *pb.Tablet, reason string) error {
	agent.mutex.Lock()
	newTablet := agent._tablet.Tablet
	agent.mutex.Unlock()
	log.Infof("Running tablet callback because: %v", reason)
	if err := agent.changeCallback(ctx, oldTablet, newTablet); err != nil {
		return err
	}

	event.Dispatch(&events.StateChange{
		OldTablet: *oldTablet,
		NewTablet: *newTablet,
		Reason:    reason,
	})
	return nil
}

func (agent *ActionAgent) readTablet(ctx context.Context) (*topo.TabletInfo, error) {
	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return nil, err
	}
	agent.mutex.Lock()
	agent._tablet = tablet
	agent.mutex.Unlock()
	return tablet, nil
}

func (agent *ActionAgent) setTablet(tablet *topo.TabletInfo) {
	agent.mutex.Lock()
	agent._tablet = tablet
	agent.mutex.Unlock()
}

// Tablet reads the stored TabletInfo from the agent, protected by mutex.
func (agent *ActionAgent) Tablet() *topo.TabletInfo {
	agent.mutex.Lock()
	tablet := agent._tablet
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

func (agent *ActionAgent) setTabletControl(tc *pb.Shard_TabletControl) {
	agent.mutex.Lock()
	agent._tabletControl = tc
	agent.mutex.Unlock()
}

// refreshTablet needs to be run after an action may have changed the current
// state of the tablet.
func (agent *ActionAgent) refreshTablet(ctx context.Context, reason string) error {
	log.Infof("Executing post-action state refresh")

	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("ActionAgent.refreshTablet")
	span.Annotate("reason", reason)
	defer span.Finish()
	ctx = trace.NewContext(ctx, span)

	// Save the old tablet so callbacks can have a better idea of
	// the precise nature of the transition.
	oldTablet := agent.Tablet().Tablet

	// Actions should have side effects on the tablet, so reload the data.
	ti, err := agent.readTablet(ctx)
	if err != nil {
		log.Warningf("Failed rereading tablet after %v - services may be inconsistent: %v", reason, err)
		return fmt.Errorf("Failed rereading tablet after %v: %v", reason, err)
	}

	if updatedTablet := agent.checkTabletMysqlPort(ctx, ti); updatedTablet != nil {
		agent.mutex.Lock()
		agent._tablet = updatedTablet
		agent.mutex.Unlock()
	}

	if err := agent.updateState(ctx, oldTablet, reason); err != nil {
		return err
	}
	log.Infof("Done with post-action state refresh")
	return nil
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
	ti := agent.Tablet()
	if !topo.IsRunningQueryService(ti.Type) {
		return nil
	}

	// Check to see our address is registered in the right place.
	return topotools.UpdateTabletEndpoints(ctx, agent.TopoServer, ti.Tablet)
}

// Start validates and updates the topology records for the tablet, and performs
// the initial state change callback to start tablet services.
func (agent *ActionAgent) Start(ctx context.Context, mysqlPort, vtPort, gRPCPort int32) error {
	var err error
	if _, err = agent.readTablet(ctx); err != nil {
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
	f := func(tablet *pb.Tablet) error {
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
	if err := agent.TopoServer.UpdateTabletFields(ctx, agent.Tablet().Alias, f); err != nil {
		return err
	}

	// Reread to get the changes we just made
	if _, err := agent.readTablet(ctx); err != nil {
		return err
	}

	if err = agent.verifyTopology(ctx); err != nil {
		return err
	}

	if err = agent.verifyServingAddrs(ctx); err != nil {
		return err
	}

	oldTablet := &pb.Tablet{}
	if err = agent.updateState(ctx, oldTablet, "Start"); err != nil {
		log.Warningf("Initial updateState failed, will need a state change before running properly: %v", err)
	}
	return nil
}

// Stop shutdowns this agent.
func (agent *ActionAgent) Stop() {
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
// and if not will try to update it.
func (agent *ActionAgent) checkTabletMysqlPort(ctx context.Context, tablet *topo.TabletInfo) *topo.TabletInfo {
	mport, err := agent.MysqlDaemon.GetMysqlPort()
	if err != nil {
		log.Warningf("Cannot get current mysql port, not checking it: %v", err)
		return nil
	}

	if mport == tablet.PortMap["mysql"] {
		return nil
	}

	log.Warningf("MySQL port has changed from %v to %v, updating it in tablet record", tablet.PortMap["mysql"], mport)
	tablet.PortMap["mysql"] = mport
	if err := agent.TopoServer.UpdateTablet(ctx, tablet); err != nil {
		log.Warningf("Failed to update tablet record, may use old mysql port")
		return nil
	}

	return tablet
}
