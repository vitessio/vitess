// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"flag"
	"fmt"
	"time"

	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"

	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	enableSemiSync = flag.Bool("enable_semi_sync", false, "Enable semi-sync when configuring replication, on master and replica tablets only (rdonly tablets will not ack).")
)

// SlaveStatus returns the replication status
// Should be called under RPCWrap.
func (agent *ActionAgent) SlaveStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	status, err := agent.MysqlDaemon.SlaveStatus()
	if err != nil {
		return nil, err
	}
	return replication.StatusToProto(status), nil
}

// MasterPosition returns the master position
// Should be called under RPCWrap.
func (agent *ActionAgent) MasterPosition(ctx context.Context) (string, error) {
	pos, err := agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}
	return replication.EncodePosition(pos), nil
}

// StopSlave will stop the replication. Works both when Vitess manages
// replication or not (using hook if not).
// Should be called under RPCWrapLock.
func (agent *ActionAgent) StopSlave(ctx context.Context) error {
	return mysqlctl.StopSlave(agent.MysqlDaemon, agent.hookExtraEnv())
}

// StopSlaveMinimum will stop the slave after it reaches at least the
// provided position. Works both when Vitess manages
// replication or not (using hook if not).
func (agent *ActionAgent) StopSlaveMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error) {
	pos, err := replication.DecodePosition(position)
	if err != nil {
		return "", err
	}
	if err := agent.MysqlDaemon.WaitMasterPos(pos, waitTime); err != nil {
		return "", err
	}
	if err := mysqlctl.StopSlave(agent.MysqlDaemon, agent.hookExtraEnv()); err != nil {
		return "", err
	}
	pos, err = agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}
	return replication.EncodePosition(pos), nil
}

// StartSlave will start the replication. Works both when Vitess manages
// replication or not (using hook if not).
// Should be called under RPCWrapLock.
func (agent *ActionAgent) StartSlave(ctx context.Context) error {
	if *enableSemiSync {
		if err := agent.enableSemiSync(false); err != nil {
			return err
		}
	}
	return mysqlctl.StartSlave(agent.MysqlDaemon, agent.hookExtraEnv())
}

// GetSlaves returns the address of all the slaves
// Should be called under RPCWrap.
func (agent *ActionAgent) GetSlaves(ctx context.Context) ([]string, error) {
	return mysqlctl.FindSlaves(agent.MysqlDaemon)
}

// ResetReplication completely resets the replication on the host.
// All binary and relay logs are flushed. All replication positions are reset.
func (agent *ActionAgent) ResetReplication(ctx context.Context) error {
	cmds, err := agent.MysqlDaemon.ResetReplicationCommands()
	if err != nil {
		return err
	}

	return agent.MysqlDaemon.ExecuteSuperQueryList(cmds)
}

// InitMaster enables writes and returns the replication position.
func (agent *ActionAgent) InitMaster(ctx context.Context) (string, error) {
	// we need to insert something in the binlogs, so we can get the
	// current position. Let's just use the mysqlctl.CreateReparentJournal commands.
	cmds := mysqlctl.CreateReparentJournal()
	if err := agent.MysqlDaemon.ExecuteSuperQueryList(cmds); err != nil {
		return "", err
	}

	// get the current replication position
	pos, err := agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if *enableSemiSync {
		if err := agent.enableSemiSync(true); err != nil {
			return "", err
		}
	}

	// Set the server read-write, from now on we can accept real
	// client writes. Note that if semi-sync replication is enabled,
	// we'll still need some slaves to be able to commit transactions.
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}

	// Change our type to master if not already
	if _, err := agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias, func(tablet *topodatapb.Tablet) error {
		tablet.Type = topodatapb.TabletType_MASTER
		tablet.HealthMap = nil
		return nil
	}); err != nil {
		return "", err
	}

	agent.initReplication = true
	return replication.EncodePosition(pos), nil
}

// PopulateReparentJournal adds an entry into the reparent_journal table.
func (agent *ActionAgent) PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, position string) error {
	pos, err := replication.DecodePosition(position)
	if err != nil {
		return err
	}
	cmds := mysqlctl.CreateReparentJournal()
	cmds = append(cmds, mysqlctl.PopulateReparentJournal(timeCreatedNS, actionName, topoproto.TabletAliasString(masterAlias), pos))

	return agent.MysqlDaemon.ExecuteSuperQueryList(cmds)
}

// InitSlave sets replication master and position, and waits for the
// reparent_journal table entry up to context timeout
func (agent *ActionAgent) InitSlave(ctx context.Context, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64) error {
	pos, err := replication.DecodePosition(position)
	if err != nil {
		return err
	}
	ti, err := agent.TopoServer.GetTablet(ctx, parent)
	if err != nil {
		return err
	}

	// If using semi-sync, we need to enable it before connecting to master.
	if *enableSemiSync {
		if err := agent.enableSemiSync(false); err != nil {
			return err
		}
	}

	cmds, err := agent.MysqlDaemon.SetSlavePositionCommands(pos)
	if err != nil {
		return err
	}
	cmds2, err := agent.MysqlDaemon.SetMasterCommands(ti.Hostname, int(ti.PortMap["mysql"]))
	if err != nil {
		return err
	}
	cmds = append(cmds, cmds2...)
	cmds = append(cmds, "START SLAVE")

	if err := agent.MysqlDaemon.ExecuteSuperQueryList(cmds); err != nil {
		return err
	}
	agent.initReplication = true

	// wait until we get the replicated row, or our context times out
	return agent.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS)
}

// DemoteMaster marks the server read-only, wait until it is done with
// its current transactions, and returns its master position.
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) DemoteMaster(ctx context.Context) (string, error) {
	// Set the server read-only. Note all active connections are not
	// affected.
	if err := agent.MysqlDaemon.SetReadOnly(true); err != nil {
		return "", err
	}

	// Now disallow queries, to make sure nobody is writing to the
	// database.
	tablet := agent.Tablet()
	// We don't care if the QueryService state actually changed because we'll
	// let vtgate keep serving read traffic from this master (see comment below).
	if _ /* state changed */, err := agent.disallowQueries(tablet.Type, "DemoteMaster marks server rdonly"); err != nil {
		return "", fmt.Errorf("disallowQueries failed: %v", err)
	}

	// If using semi-sync, we need to disable master-side.
	if *enableSemiSync {
		if err := agent.enableSemiSync(false); err != nil {
			return "", err
		}
	}

	pos, err := agent.MysqlDaemon.DemoteMaster()
	if err != nil {
		return "", err
	}
	return replication.EncodePosition(pos), nil
	// There is no serving graph update - the master tablet will
	// be replaced. Even though writes may fail, reads will
	// succeed. It will be less noisy to simply leave the entry
	// until we'll promote the master.
}

// PromoteSlaveWhenCaughtUp waits for this slave to be caught up on
// replication up to the provided point, and then makes the slave the
// shard master.
func (agent *ActionAgent) PromoteSlaveWhenCaughtUp(ctx context.Context, position string) (string, error) {
	pos, err := replication.DecodePosition(position)
	if err != nil {
		return "", err
	}

	// TODO(alainjobart) change the flavor API to take the context directly
	// For now, extract the timeout from the context, or wait forever
	var waitTimeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		waitTimeout = deadline.Sub(time.Now())
		if waitTimeout <= 0 {
			waitTimeout = time.Millisecond
		}
	}
	if err := agent.MysqlDaemon.WaitMasterPos(pos, waitTimeout); err != nil {
		return "", err
	}

	pos, err = agent.MysqlDaemon.PromoteSlave(agent.hookExtraEnv())
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if *enableSemiSync {
		if err := agent.enableSemiSync(true); err != nil {
			return "", err
		}
	}

	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}

	if _, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER, topotools.ClearHealthMap); err != nil {
		return "", err
	}

	return replication.EncodePosition(pos), nil
}

// SlaveWasPromoted promotes a slave to master, no questions asked.
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) SlaveWasPromoted(ctx context.Context) error {
	_, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER, topotools.ClearHealthMap)
	return err
}

// SetMaster sets replication master, and waits for the
// reparent_journal table entry up to context timeout
func (agent *ActionAgent) SetMaster(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	parent, err := agent.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}

	// See if we were replicating at all, and should be replicating
	wasReplicating := false
	shouldbeReplicating := false
	rs, err := agent.MysqlDaemon.SlaveStatus()
	if err == nil && (rs.SlaveIORunning || rs.SlaveSQLRunning) {
		wasReplicating = true
		shouldbeReplicating = true
	}
	if forceStartSlave {
		shouldbeReplicating = true
	}

	// If using semi-sync, we need to enable it before connecting to master.
	if *enableSemiSync {
		if err := agent.enableSemiSync(false); err != nil {
			return err
		}
	}

	// Create the list of commands to set the master
	cmds := []string{}
	if wasReplicating {
		cmds = append(cmds, mysqlctl.SQLStopSlave)
	}
	smc, err := agent.MysqlDaemon.SetMasterCommands(parent.Hostname, int(parent.PortMap["mysql"]))
	if err != nil {
		return err
	}
	cmds = append(cmds, smc...)
	if shouldbeReplicating {
		cmds = append(cmds, mysqlctl.SQLStartSlave)
	}
	if err := agent.MysqlDaemon.ExecuteSuperQueryList(cmds); err != nil {
		return err
	}

	// change our type to spare if we used to be the master
	_, err = agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias, func(tablet *topodatapb.Tablet) error {
		if tablet.Type == topodatapb.TabletType_MASTER {
			tablet.Type = topodatapb.TabletType_SPARE
			tablet.HealthMap = nil
			return nil
		}
		return topo.ErrNoUpdateNeeded
	})
	if err != nil {
		return err
	}

	// if needed, wait until we get the replicated row, or our
	// context times out
	if !shouldbeReplicating || timeCreatedNS == 0 {
		return nil
	}
	return agent.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS)
}

// SlaveWasRestarted updates the parent record for a tablet.
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) SlaveWasRestarted(ctx context.Context, swrd *actionnode.SlaveWasRestartedArgs) error {
	// Once this action completes, update authoritative tablet node first.
	_, err := agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias, func(tablet *topodatapb.Tablet) error {
		if tablet.Type == topodatapb.TabletType_MASTER {
			tablet.Type = topodatapb.TabletType_SPARE
			return nil
		}
		return topo.ErrNoUpdateNeeded
	})
	return err
}

// StopReplicationAndGetStatus stops MySQL replication, and returns the
// current status
func (agent *ActionAgent) StopReplicationAndGetStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	// get the status before we stop replication
	rs, err := agent.MysqlDaemon.SlaveStatus()
	if err != nil {
		return nil, fmt.Errorf("before status failed: %v", err)
	}
	if !rs.SlaveIORunning && !rs.SlaveSQLRunning {
		// no replication is running, just return what we got
		return replication.StatusToProto(rs), nil
	}
	if err := mysqlctl.StopSlave(agent.MysqlDaemon, agent.hookExtraEnv()); err != nil {
		return nil, fmt.Errorf("stop slave failed: %v", err)
	}
	// now patch in the current position
	rs.Position, err = agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return nil, fmt.Errorf("after position failed: %v", err)
	}
	return replication.StatusToProto(rs), nil
}

// PromoteSlave makes the current tablet the master
func (agent *ActionAgent) PromoteSlave(ctx context.Context) (string, error) {
	pos, err := agent.MysqlDaemon.PromoteSlave(agent.hookExtraEnv())
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if *enableSemiSync {
		if err := agent.enableSemiSync(true); err != nil {
			return "", err
		}
	}

	// Set the server read-write
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}

	if _, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER, topotools.ClearHealthMap); err != nil {
		return "", err
	}

	return replication.EncodePosition(pos), nil
}

func (agent *ActionAgent) isMasterEligible() (bool, error) {
	switch agent.Tablet().Type {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA:
		return true, nil
	case topodatapb.TabletType_SPARE:
		// If we're SPARE, it could be because healthcheck is enabled.
		tt, err := topoproto.ParseTabletType(*targetTabletType)
		if err != nil {
			return false, fmt.Errorf("can't determine if tablet is master-eligible: currently SPARE and no -target_tablet_type flag specified")
		}
		if tt == topodatapb.TabletType_REPLICA {
			return true, nil
		}
	}

	return false, nil
}

func (agent *ActionAgent) enableSemiSync(master bool) error {
	// Only enable if we're eligible for becoming master (REPLICA type).
	// Ineligible slaves (RDONLY) shouldn't ACK because we'll never promote them.
	masterEligible, err := agent.isMasterEligible()
	if err != nil {
		return fmt.Errorf("can't enable semi-sync: %v", err)
	}
	if !masterEligible {
		return nil
	}

	// Always enable slave-side since it doesn't hurt to keep it on for a master.
	// The master-side needs to be off for a slave, or else it will get stuck.
	return agent.MysqlDaemon.SetSemiSyncEnabled(master, true)
}
