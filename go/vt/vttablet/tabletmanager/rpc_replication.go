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

package tabletmanager

import (
	"flag"
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	enableSemiSync   = flag.Bool("enable_semi_sync", false, "Enable semi-sync when configuring replication, on master and replica tablets only (rdonly tablets will not ack).")
	setSuperReadOnly = flag.Bool("use_super_read_only", false, "Set super_read_only flag when performing planned failover.")
)

// SlaveStatus returns the replication status
func (agent *ActionAgent) SlaveStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	status, err := agent.MysqlDaemon.SlaveStatus()
	if err != nil {
		return nil, err
	}
	return mysql.SlaveStatusToProto(status), nil
}

// MasterPosition returns the master position
func (agent *ActionAgent) MasterPosition(ctx context.Context) (string, error) {
	pos, err := agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// StopSlave will stop the mysql. Works both when Vitess manages
// replication or not (using hook if not).
func (agent *ActionAgent) StopSlave(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	return agent.stopSlaveLocked(ctx)
}

func (agent *ActionAgent) stopSlaveLocked(ctx context.Context) error {

	// Remember that we were told to stop, so we don't try to
	// restart ourselves (in replication_reporter).
	agent.setSlaveStopped(true)

	// Also tell Orchestrator we're stopped on purpose for some Vitess task.
	// Do this in the background, as it's best-effort.
	go func() {
		if agent.orc == nil {
			return
		}
		if err := agent.orc.BeginMaintenance(agent.Tablet(), "vttablet has been told to StopSlave"); err != nil {
			log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
		}
	}()

	return agent.MysqlDaemon.StopSlave(agent.hookExtraEnv())
}

// StopSlaveMinimum will stop the slave after it reaches at least the
// provided position. Works both when Vitess manages
// replication or not (using hook if not).
func (agent *ActionAgent) StopSlaveMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return "", err
	}
	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()
	if err := agent.MysqlDaemon.WaitMasterPos(waitCtx, pos); err != nil {
		return "", err
	}
	if err := agent.stopSlaveLocked(ctx); err != nil {
		return "", err
	}
	pos, err = agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// StartSlave will start the mysql. Works both when Vitess manages
// replication or not (using hook if not).
func (agent *ActionAgent) StartSlave(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	agent.setSlaveStopped(false)

	// Tell Orchestrator we're no longer stopped on purpose.
	// Do this in the background, as it's best-effort.
	go func() {
		if agent.orc == nil {
			return
		}
		if err := agent.orc.EndMaintenance(agent.Tablet()); err != nil {
			log.Warningf("Orchestrator EndMaintenance failed: %v", err)
		}
	}()

	if err := agent.fixSemiSync(agent.Tablet().Type); err != nil {
		return err
	}
	return agent.MysqlDaemon.StartSlave(agent.hookExtraEnv())
}

// StartSlaveUntilAfter will start the replication and let it catch up
// until and including the transactions in `position`
func (agent *ActionAgent) StartSlaveUntilAfter(ctx context.Context, position string, waitTime time.Duration) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}

	return agent.MysqlDaemon.StartSlaveUntilAfter(waitCtx, pos)
}

// GetSlaves returns the address of all the slaves
func (agent *ActionAgent) GetSlaves(ctx context.Context) ([]string, error) {
	return mysqlctl.FindSlaves(agent.MysqlDaemon)
}

// ResetReplication completely resets the replication on the host.
// All binary and relay logs are flushed. All replication positions are reset.
func (agent *ActionAgent) ResetReplication(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	agent.setSlaveStopped(true)
	return agent.MysqlDaemon.ResetReplication(ctx)
}

// InitMaster enables writes and returns the replication position.
func (agent *ActionAgent) InitMaster(ctx context.Context) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	// Initializing as master implies undoing any previous "do not replicate".
	agent.setSlaveStopped(false)

	// we need to insert something in the binlogs, so we can get the
	// current position. Let's just use the mysqlctl.CreateReparentJournal commands.
	cmds := mysqlctl.CreateReparentJournal()
	if err := agent.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return "", err
	}

	// get the current replication position
	pos, err := agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := agent.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	// Set the server read-write, from now on we can accept real
	// client writes. Note that if semi-sync replication is enabled,
	// we'll still need some slaves to be able to commit transactions.
	startTime := time.Now()
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}
	agent.setExternallyReparentedTime(startTime)

	// Change our type to master if not already
	if _, err := agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias, func(tablet *topodatapb.Tablet) error {
		tablet.Type = topodatapb.TabletType_MASTER
		return nil
	}); err != nil {
		return "", err
	}

	// and refresh our state
	agent.initReplication = true
	if err := agent.refreshTablet(ctx, "InitMaster"); err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// PopulateReparentJournal adds an entry into the reparent_journal table.
func (agent *ActionAgent) PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, position string) error {
	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}
	cmds := mysqlctl.CreateReparentJournal()
	cmds = append(cmds, mysqlctl.PopulateReparentJournal(timeCreatedNS, actionName, topoproto.TabletAliasString(masterAlias), pos))

	return agent.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds)
}

// InitSlave sets replication master and position, and waits for the
// reparent_journal table entry up to context timeout
func (agent *ActionAgent) InitSlave(ctx context.Context, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}
	ti, err := agent.TopoServer.GetTablet(ctx, parent)
	if err != nil {
		return err
	}

	agent.setSlaveStopped(false)

	// If using semi-sync, we need to enable it before connecting to master.
	// If we were a master type, we need to switch back to replica settings.
	// Otherwise we won't be able to commit anything.
	tt := agent.Tablet().Type
	if tt == topodatapb.TabletType_MASTER {
		tt = topodatapb.TabletType_REPLICA
	}
	if err := agent.fixSemiSync(tt); err != nil {
		return err
	}

	if err := agent.MysqlDaemon.SetSlavePosition(ctx, pos); err != nil {
		return err
	}
	if err := agent.MysqlDaemon.SetMaster(ctx, topoproto.MysqlHostname(ti.Tablet), int(topoproto.MysqlPort(ti.Tablet)), false /* slaveStopBefore */, true /* slaveStartAfter */); err != nil {
		return err
	}
	agent.initReplication = true

	// If we were a master type, switch our type to replica.  This
	// is used on the old master when using InitShardMaster with
	// -force, and the new master is different from the old master.
	if agent.Tablet().Type == topodatapb.TabletType_MASTER {
		if _, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_REPLICA); err != nil {
			return err
		}

		if err := agent.refreshTablet(ctx, "InitSlave"); err != nil {
			return err
		}
	}

	// wait until we get the replicated row, or our context times out
	return agent.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS)
}

// DemoteMaster marks the server read-only, wait until it is done with
// its current transactions, and returns its master position.
func (agent *ActionAgent) DemoteMaster(ctx context.Context) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	// Tell Orchestrator we're stopped on purpose the demotion.
	// This is a best effort task, so run it in a goroutine.
	go func() {
		if agent.orc == nil {
			return
		}
		if err := agent.orc.BeginMaintenance(agent.Tablet(), "vttablet has been told to DemoteMaster"); err != nil {
			log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
		}
	}()

	// First, disallow queries, to make sure nobody is writing to the
	// database.
	tablet := agent.Tablet()
	// We don't care if the QueryService state actually changed because we'll
	// let vtgate keep serving read traffic from this master (see comment below).
	log.Infof("DemoteMaster disabling query service")
	if _ /* state changed */, err := agent.QueryServiceControl.SetServingType(tablet.Type, false, nil); err != nil {
		return "", vterrors.Wrap(err, "SetServingType(serving=false) failed")
	}

	// Now, set the server read-only. Note all active connections are not
	// affected.
	if *setSuperReadOnly {
		// Setting super_read_only also sets read_only
		if err := agent.MysqlDaemon.SetSuperReadOnly(true); err != nil {
			// if this failed, revert the change to serving
			if _ /* state changed */, err1 := agent.QueryServiceControl.SetServingType(tablet.Type, true, nil); err1 != nil {
				log.Warningf("SetServingType(serving=true) failed after failed SetSuperReadOnly %v", err1)
			}
			return "", err
		}
	} else {
		if err := agent.MysqlDaemon.SetReadOnly(true); err != nil {
			// if this failed, revert the change to serving
			if _ /* state changed */, err1 := agent.QueryServiceControl.SetServingType(tablet.Type, true, nil); err1 != nil {
				log.Warningf("SetServingType(serving=true) failed after failed SetReadOnly %v", err1)
			}
			return "", err
		}
	}

	// If using semi-sync, we need to disable master-side.
	if err := agent.fixSemiSync(topodatapb.TabletType_REPLICA); err != nil {
		// if this failed, set server read-only back to false, set tablet back to serving
		// setting read_only OFF will also set super_read_only OFF if it was set
		if err1 := agent.MysqlDaemon.SetReadOnly(false); err1 != nil {
			log.Warningf("SetReadOnly(false) failed after failed fixSemiSync %v", err1)
		}
		if _ /* state changed */, err1 := agent.QueryServiceControl.SetServingType(tablet.Type, true, nil); err1 != nil {
			log.Warningf("SetServingType(serving=true) failed after failed fixSemiSync %v", err1)
		}
		return "", err
	}

	pos, err := agent.MysqlDaemon.MasterPosition()
	if err != nil {
		// if MasterPosition failed, undo all the steps before
		// 1. set server back to read-only false
		// setting read_only OFF will also set super_read_only OFF if it was set
		if err1 := agent.MysqlDaemon.SetReadOnly(false); err1 != nil {
			log.Warningf("SetReadOnly(false) failed after failed DemoteMaster %v", err1)
		}
		// 2. set tablet back to serving
		if _ /* state changed */, err1 := agent.QueryServiceControl.SetServingType(tablet.Type, true, nil); err1 != nil {
			log.Warningf("SetServingType(serving=true) failed after failed DemoteMaster %v", err1)
		}
		// 3. enable master side again
		if err1 := agent.fixSemiSync(topodatapb.TabletType_MASTER); err1 != nil {
			log.Warningf("fixSemiSync(MASTER) failed after failed DemoteMaster %v", err1)
		}
		return "", err
	}
	return mysql.EncodePosition(pos), nil
	// There is no serving graph update - the master tablet will
	// be replaced. Even though writes may fail, reads will
	// succeed. It will be less noisy to simply leave the entry
	// until we'll promote the master.
}

// UndoDemoteMaster reverts a previous call to DemoteMaster
// it sets read-only to false, fixes semi-sync
// and returns its master position.
func (agent *ActionAgent) UndoDemoteMaster(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	// If using semi-sync, we need to enable master-side.
	if err := agent.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return err
	}

	// Now, set the server read-only false.
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return err
	}

	// Update serving graph
	tablet := agent.Tablet()
	log.Infof("UndoDemoteMaster re-enabling query service")
	if _ /* state changed */, err := agent.QueryServiceControl.SetServingType(tablet.Type, true, nil); err != nil {
		return vterrors.Wrap(err, "SetServingType(serving=true) failed")
	}

	return nil
}

// PromoteSlaveWhenCaughtUp waits for this slave to be caught up on
// replication up to the provided point, and then makes the slave the
// shard master.
func (agent *ActionAgent) PromoteSlaveWhenCaughtUp(ctx context.Context, position string) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return "", err
	}

	if err := agent.MysqlDaemon.WaitMasterPos(ctx, pos); err != nil {
		return "", err
	}

	pos, err = agent.MysqlDaemon.PromoteSlave(agent.hookExtraEnv())
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := agent.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	startTime := time.Now()
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}
	agent.setExternallyReparentedTime(startTime)

	if _, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	if err := agent.refreshTablet(ctx, "PromoteSlaveWhenCaughtUp"); err != nil {
		return "", err
	}

	return mysql.EncodePosition(pos), nil
}

// SlaveWasPromoted promotes a slave to master, no questions asked.
func (agent *ActionAgent) SlaveWasPromoted(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	if _, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER); err != nil {
		return err
	}

	if err := agent.refreshTablet(ctx, "SlaveWasPromoted"); err != nil {
		return err
	}

	return nil
}

// SetMaster sets replication master, and waits for the
// reparent_journal table entry up to context timeout
func (agent *ActionAgent) SetMaster(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	return agent.setMasterLocked(ctx, parentAlias, timeCreatedNS, forceStartSlave)
}

func (agent *ActionAgent) setMasterRepairReplication(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) (err error) {
	parent, err := agent.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}

	ctx, unlock, lockErr := agent.TopoServer.LockShard(ctx, parent.Tablet.GetKeyspace(), parent.Tablet.GetShard(), fmt.Sprintf("repairReplication to %v as parent)", topoproto.TabletAliasString(parentAlias)))
	if lockErr != nil {
		return lockErr
	}

	defer unlock(&err)

	return agent.setMasterLocked(ctx, parentAlias, timeCreatedNS, forceStartSlave)
}

func (agent *ActionAgent) setMasterLocked(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) (err error) {
	parent, err := agent.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}

	// End orchestrator maintenance at the end of fixing replication.
	// This is a best effort operation, so it should happen in a goroutine
	defer func() {
		go func() {
			if agent.orc == nil {
				return
			}
			if err := agent.orc.EndMaintenance(agent.Tablet()); err != nil {
				log.Warningf("Orchestrator EndMaintenance failed: %v", err)
			}
		}()
	}()

	// See if we were replicating at all, and should be replicating
	wasReplicating := false
	shouldbeReplicating := false
	rs, err := agent.MysqlDaemon.SlaveStatus()
	if err == nil && (rs.SlaveIORunning || rs.SlaveSQLRunning) {
		wasReplicating = true
		shouldbeReplicating = true
	} else if err == mysql.ErrNotSlave {
		// If we used to be a master, or if we started as a replica but never
		// found out who the master is, we always try to start replicating once
		// we are told who the new master is via SetMaster.
		shouldbeReplicating = true
	}
	if forceStartSlave {
		shouldbeReplicating = true
	}

	// If using semi-sync, we need to enable it before connecting to master.
	if *enableSemiSync {
		tt := agent.Tablet().Type
		if tt == topodatapb.TabletType_MASTER {
			tt = topodatapb.TabletType_REPLICA
		}
		if err := agent.fixSemiSync(tt); err != nil {
			return err
		}
	}

	// Sets the master.
	if err := agent.MysqlDaemon.SetMaster(ctx, topoproto.MysqlHostname(parent.Tablet), int(topoproto.MysqlPort(parent.Tablet)), wasReplicating, shouldbeReplicating); err != nil {
		return err
	}

	// change our type to REPLICA if we used to be the master
	typeChanged := false
	_, err = agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias, func(tablet *topodatapb.Tablet) error {
		if tablet.Type == topodatapb.TabletType_MASTER {
			tablet.Type = topodatapb.TabletType_REPLICA
			typeChanged = true
			return nil
		}
		return topo.NewError(topo.NoUpdateNeeded, agent.TabletAlias.String())
	})
	if err != nil {
		return err
	}

	// if needed, wait until we get the replicated row, or our
	// context times out
	if !shouldbeReplicating || timeCreatedNS == 0 {
		return nil
	}
	if err := agent.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS); err != nil {
		return err
	}
	if typeChanged {
		if err := agent.refreshTablet(ctx, "SetMaster"); err != nil {
			return err
		}
		agent.runHealthCheckLocked()
	}
	return nil
}

// SlaveWasRestarted updates the parent record for a tablet.
func (agent *ActionAgent) SlaveWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	typeChanged := false

	// Once this action completes, update authoritative tablet node first.
	if _, err := agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias, func(tablet *topodatapb.Tablet) error {
		if tablet.Type == topodatapb.TabletType_MASTER {
			tablet.Type = topodatapb.TabletType_REPLICA
			typeChanged = true
			return nil
		}
		return topo.NewError(topo.NoUpdateNeeded, agent.TabletAlias.String())
	}); err != nil {
		return err
	}

	if typeChanged {
		if err := agent.refreshTablet(ctx, "SlaveWasRestarted"); err != nil {
			return err
		}
		agent.runHealthCheckLocked()
	}
	return nil
}

// StopReplicationAndGetStatus stops MySQL replication, and returns the
// current status.
func (agent *ActionAgent) StopReplicationAndGetStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	if err := agent.lock(ctx); err != nil {
		return nil, err
	}
	defer agent.unlock()

	// get the status before we stop replication
	rs, err := agent.MysqlDaemon.SlaveStatus()
	if err != nil {
		return nil, vterrors.Wrap(err, "before status failed")
	}
	if !rs.SlaveIORunning && !rs.SlaveSQLRunning {
		// no replication is running, just return what we got
		return mysql.SlaveStatusToProto(rs), nil
	}
	if err := agent.stopSlaveLocked(ctx); err != nil {
		return nil, vterrors.Wrap(err, "stop slave failed")
	}
	// now patch in the current position
	rs.Position, err = agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return nil, vterrors.Wrap(err, "after position failed")
	}
	return mysql.SlaveStatusToProto(rs), nil
}

// PromoteSlave makes the current tablet the master
func (agent *ActionAgent) PromoteSlave(ctx context.Context) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	pos, err := agent.MysqlDaemon.PromoteSlave(agent.hookExtraEnv())
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := agent.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	// Set the server read-write
	startTime := time.Now()
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}
	agent.setExternallyReparentedTime(startTime)

	if _, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	if err := agent.refreshTablet(ctx, "PromoteSlave"); err != nil {
		return "", err
	}

	return mysql.EncodePosition(pos), nil
}

func isMasterEligible(tabletType topodatapb.TabletType) bool {
	switch tabletType {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA:
		return true
	}

	return false
}

func (agent *ActionAgent) fixSemiSync(tabletType topodatapb.TabletType) error {
	if !*enableSemiSync {
		// Semi-sync handling is not enabled.
		return nil
	}

	// Only enable if we're eligible for becoming master (REPLICA type).
	// Ineligible slaves (RDONLY) shouldn't ACK because we'll never promote them.
	if !isMasterEligible(tabletType) {
		return agent.MysqlDaemon.SetSemiSyncEnabled(false, false)
	}

	// Always enable slave-side since it doesn't hurt to keep it on for a master.
	// The master-side needs to be off for a slave, or else it will get stuck.
	return agent.MysqlDaemon.SetSemiSyncEnabled(tabletType == topodatapb.TabletType_MASTER, true)
}

func (agent *ActionAgent) fixSemiSyncAndReplication(tabletType topodatapb.TabletType) error {
	if !*enableSemiSync {
		// Semi-sync handling is not enabled.
		return nil
	}

	if tabletType == topodatapb.TabletType_MASTER {
		// Master is special. It is always handled at the
		// right time by the reparent operations, it doesn't
		// need to be fixed.
		return nil
	}

	if err := agent.fixSemiSync(tabletType); err != nil {
		return vterrors.Wrapf(err, "failed to fixSemiSync(%v)", tabletType)
	}

	// If replication is running, but the status is wrong,
	// we should restart replication. First, let's make sure
	// replication is running.
	status, err := agent.MysqlDaemon.SlaveStatus()
	if err != nil {
		// Replication is not configured, nothing to do.
		return nil
	}
	if !status.SlaveIORunning {
		// IO thread is not running, nothing to do.
		return nil
	}

	shouldAck := isMasterEligible(tabletType)
	acking, err := agent.MysqlDaemon.SemiSyncSlaveStatus()
	if err != nil {
		return vterrors.Wrap(err, "failed to get SemiSyncSlaveStatus")
	}
	if shouldAck == acking {
		return nil
	}

	// We need to restart replication
	log.Infof("Restarting replication for semi-sync flag change to take effect from %v to %v", acking, shouldAck)
	if err := agent.MysqlDaemon.StopSlave(agent.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "failed to StopSlave")
	}
	if err := agent.MysqlDaemon.StartSlave(agent.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "failed to StartSlave")
	}
	return nil
}
