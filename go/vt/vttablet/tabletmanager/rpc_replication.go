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

package tabletmanager

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var setSuperReadOnly bool
var disableReplicationManager bool

func registerReplicationFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&setSuperReadOnly, "use_super_read_only", setSuperReadOnly, "Set super_read_only flag when performing planned failover.")
	fs.BoolVar(&disableReplicationManager, "disable-replication-manager", disableReplicationManager, "Disable replication manager to prevent replication repairs.")
	fs.MarkDeprecated("disable-replication-manager", "Replication manager is deleted")
}

func init() {
	servenv.OnParseFor("vtcombo", registerReplicationFlags)
	servenv.OnParseFor("vttablet", registerReplicationFlags)
}

// ReplicationStatus returns the replication status
func (tm *TabletManager) ReplicationStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	status, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		return nil, err
	}
	return mysql.ReplicationStatusToProto(status), nil
}

// FullStatus returns the full status of MySQL including the replication information, semi-sync information, GTID information among others
func (tm *TabletManager) FullStatus(ctx context.Context) (*replicationdatapb.FullStatus, error) {
	// Server ID - "select @@global.server_id"
	serverID, err := tm.MysqlDaemon.GetServerID(ctx)
	if err != nil {
		return nil, err
	}

	// Server UUID - "select @@global.server_uuid"
	serverUUID, err := tm.MysqlDaemon.GetServerUUID(ctx)
	if err != nil {
		return nil, err
	}

	// Replication status - "SHOW REPLICA STATUS"
	replicationStatus, err := tm.MysqlDaemon.ReplicationStatus()
	var replicationStatusProto *replicationdatapb.Status
	if err != nil && err != mysql.ErrNotReplica {
		return nil, err
	}
	if err == nil {
		replicationStatusProto = mysql.ReplicationStatusToProto(replicationStatus)
	}

	// Primary status - "SHOW MASTER STATUS"
	primaryStatus, err := tm.MysqlDaemon.PrimaryStatus(ctx)
	var primaryStatusProto *replicationdatapb.PrimaryStatus
	if err != nil && err != mysql.ErrNoPrimaryStatus {
		return nil, err
	}
	if err == nil {
		primaryStatusProto = mysql.PrimaryStatusToProto(primaryStatus)
	}

	// Purged GTID set
	purgedGTIDs, err := tm.MysqlDaemon.GetGTIDPurged(ctx)
	if err != nil {
		return nil, err
	}

	// Version string "majorVersion.minorVersion.patchRelease"
	version := tm.MysqlDaemon.GetVersionString()

	// Version comment "select @@global.version_comment"
	versionComment := tm.MysqlDaemon.GetVersionComment(ctx)

	// Read only - "SHOW VARIABLES LIKE 'read_only'"
	readOnly, err := tm.MysqlDaemon.IsReadOnly()
	if err != nil {
		return nil, err
	}

	// Binlog Information - "select @@global.binlog_format, @@global.log_bin, @@global.log_slave_updates, @@global.binlog_row_image"
	binlogFormat, logBin, logReplicaUpdates, binlogRowImage, err := tm.MysqlDaemon.GetBinlogInformation(ctx)
	if err != nil {
		return nil, err
	}

	// GTID Mode - "select @@global.gtid_mode" - Only applicable for MySQL variants
	gtidMode, err := tm.MysqlDaemon.GetGTIDMode(ctx)
	if err != nil {
		return nil, err
	}

	// Semi sync settings - "show global variables like 'rpl_semi_sync_%_enabled'"
	primarySemiSync, replicaSemiSync := tm.MysqlDaemon.SemiSyncEnabled()

	// Semi sync status - "show status like 'Rpl_semi_sync_%_status'"
	primarySemiSyncStatus, replicaSemiSyncStatus := tm.MysqlDaemon.SemiSyncStatus()

	//  Semi sync clients count - "show status like 'semi_sync_primary_clients'"
	semiSyncClients := tm.MysqlDaemon.SemiSyncClients()

	// Semi sync settings - "show status like 'rpl_semi_sync_%'
	semiSyncTimeout, semiSyncNumReplicas := tm.MysqlDaemon.SemiSyncSettings()

	return &replicationdatapb.FullStatus{
		ServerId:                    serverID,
		ServerUuid:                  serverUUID,
		ReplicationStatus:           replicationStatusProto,
		PrimaryStatus:               primaryStatusProto,
		GtidPurged:                  mysql.EncodePosition(purgedGTIDs),
		Version:                     version,
		VersionComment:              versionComment,
		ReadOnly:                    readOnly,
		GtidMode:                    gtidMode,
		BinlogFormat:                binlogFormat,
		BinlogRowImage:              binlogRowImage,
		LogBinEnabled:               logBin,
		LogReplicaUpdates:           logReplicaUpdates,
		SemiSyncPrimaryEnabled:      primarySemiSync,
		SemiSyncReplicaEnabled:      replicaSemiSync,
		SemiSyncPrimaryStatus:       primarySemiSyncStatus,
		SemiSyncReplicaStatus:       replicaSemiSyncStatus,
		SemiSyncPrimaryClients:      semiSyncClients,
		SemiSyncPrimaryTimeout:      semiSyncTimeout,
		SemiSyncWaitForReplicaCount: semiSyncNumReplicas,
	}, nil
}

// PrimaryStatus returns the replication status for a primary tablet.
func (tm *TabletManager) PrimaryStatus(ctx context.Context) (*replicationdatapb.PrimaryStatus, error) {
	status, err := tm.MysqlDaemon.PrimaryStatus(ctx)
	if err != nil {
		return nil, err
	}
	return mysql.PrimaryStatusToProto(status), nil
}

// PrimaryPosition returns the position of a primary database
func (tm *TabletManager) PrimaryPosition(ctx context.Context) (string, error) {
	pos, err := tm.MysqlDaemon.PrimaryPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// WaitForPosition waits until replication reaches the desired position
func (tm *TabletManager) WaitForPosition(ctx context.Context, pos string) error {
	log.Infof("WaitForPosition: %v", pos)
	mpos, err := mysql.DecodePosition(pos)
	if err != nil {
		return err
	}
	return tm.MysqlDaemon.WaitSourcePos(ctx, mpos)
}

// StopReplication will stop the mysql. Works both when Vitess manages
// replication or not (using hook if not).
func (tm *TabletManager) StopReplication(ctx context.Context) error {
	log.Infof("StopReplication")
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	return tm.stopReplicationLocked(ctx)
}

func (tm *TabletManager) stopReplicationLocked(ctx context.Context) error {
	return tm.MysqlDaemon.StopReplication(tm.hookExtraEnv())
}

func (tm *TabletManager) stopIOThreadLocked(ctx context.Context) error {
	return tm.MysqlDaemon.StopIOThread(ctx)
}

// StopReplicationMinimum will stop the replication after it reaches at least the
// provided position. Works both when Vitess manages
// replication or not (using hook if not).
func (tm *TabletManager) StopReplicationMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error) {
	log.Infof("StopReplicationMinimum: position: %v waitTime: %v", position, waitTime)
	if err := tm.lock(ctx); err != nil {
		return "", err
	}
	defer tm.unlock()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return "", err
	}
	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()
	if err := tm.MysqlDaemon.WaitSourcePos(waitCtx, pos); err != nil {
		return "", err
	}
	if err := tm.stopReplicationLocked(ctx); err != nil {
		return "", err
	}
	pos, err = tm.MysqlDaemon.PrimaryPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// StartReplication will start the mysql. Works both when Vitess manages
// replication or not (using hook if not).
func (tm *TabletManager) StartReplication(ctx context.Context, semiSync bool) error {
	log.Infof("StartReplication")
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	if err := tm.fixSemiSync(tm.Tablet().Type, convertBoolToSemiSyncAction(semiSync)); err != nil {
		return err
	}
	return tm.MysqlDaemon.StartReplication(tm.hookExtraEnv())
}

// StartReplicationUntilAfter will start the replication and let it catch up
// until and including the transactions in `position`
func (tm *TabletManager) StartReplicationUntilAfter(ctx context.Context, position string, waitTime time.Duration) error {
	log.Infof("StartReplicationUntilAfter: position: %v waitTime: %v", position, waitTime)
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}

	return tm.MysqlDaemon.StartReplicationUntilAfter(waitCtx, pos)
}

// GetReplicas returns the address of all the replicas
func (tm *TabletManager) GetReplicas(ctx context.Context) ([]string, error) {
	return mysqlctl.FindReplicas(tm.MysqlDaemon)
}

// ResetReplication completely resets the replication on the host.
// All binary and relay logs are flushed. All replication positions are reset.
func (tm *TabletManager) ResetReplication(ctx context.Context) error {
	log.Infof("ResetReplication")
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	return tm.MysqlDaemon.ResetReplication(ctx)
}

// InitPrimary enables writes and returns the replication position.
func (tm *TabletManager) InitPrimary(ctx context.Context, semiSync bool) (string, error) {
	log.Infof("InitPrimary with semiSync as %t", semiSync)
	if err := tm.lock(ctx); err != nil {
		return "", err
	}
	defer tm.unlock()

	if setSuperReadOnly {
		// Setting super_read_only off so that we can run the DDL commands
		if err := tm.MysqlDaemon.SetSuperReadOnly(false); err != nil {
			if strings.Contains(err.Error(), strconv.Itoa(mysql.ERUnknownSystemVariable)) {
				log.Warningf("server does not know about super_read_only, continuing anyway...")
			} else {
				return "", err
			}
		}
	}

	// we need to generate a binlog entry so that we can get the current position.
	cmd := mysqlctl.GenerateInitialBinlogEntry()
	if err := tm.MysqlDaemon.ExecuteSuperQueryList(ctx, []string{cmd}); err != nil {
		return "", err
	}

	// get the current replication position
	pos, err := tm.MysqlDaemon.PrimaryPosition()
	if err != nil {
		return "", err
	}

	// Set the server read-write, from now on we can accept real
	// client writes. Note that if semi-sync replication is enabled,
	// we'll still need some replicas to be able to commit transactions.
	if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_PRIMARY, DBActionSetReadWrite, convertBoolToSemiSyncAction(semiSync)); err != nil {
		return "", err
	}

	// Enforce semi-sync after changing the tablet type to PRIMARY. Otherwise, the
	// primary will hang while trying to create the database.
	if err := tm.fixSemiSync(topodatapb.TabletType_PRIMARY, convertBoolToSemiSyncAction(semiSync)); err != nil {
		return "", err
	}

	return mysql.EncodePosition(pos), nil
}

// PopulateReparentJournal adds an entry into the reparent_journal table.
func (tm *TabletManager) PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, primaryAlias *topodatapb.TabletAlias, position string) error {
	log.Infof("PopulateReparentJournal: action: %v parent: %v  position: %v timeCreatedNS: %d actionName: %s primaryAlias: %s",
		actionName, primaryAlias, position, timeCreatedNS, actionName, primaryAlias)
	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}

	cmds := []string{mysqlctl.PopulateReparentJournal(timeCreatedNS, actionName, topoproto.TabletAliasString(primaryAlias), pos)}

	return tm.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds)
}

// InitReplica sets replication primary and position, and waits for the
// reparent_journal table entry up to context timeout
func (tm *TabletManager) InitReplica(ctx context.Context, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64, semiSync bool) error {
	log.Infof("InitReplica: parent: %v  position: %v  timeCreatedNS: %d  semisync: %t", parent, position, timeCreatedNS, semiSync)
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	// If we were a primary type, switch our type to replica.  This
	// is used on the old primary when using InitShardPrimary with
	// -force, and the new primary is different from the old primary.
	if tm.Tablet().Type == topodatapb.TabletType_PRIMARY {
		if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_REPLICA, DBActionNone, convertBoolToSemiSyncAction(semiSync)); err != nil {
			return err
		}
	}

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}
	ti, err := tm.TopoServer.GetTablet(ctx, parent)
	if err != nil {
		return err
	}

	// If using semi-sync, we need to enable it before connecting to primary.
	// If we were a primary type, we need to switch back to replica settings.
	// Otherwise we won't be able to commit anything.
	tt := tm.Tablet().Type
	if tt == topodatapb.TabletType_PRIMARY {
		tt = topodatapb.TabletType_REPLICA
	}
	if err := tm.fixSemiSync(tt, convertBoolToSemiSyncAction(semiSync)); err != nil {
		return err
	}

	if err := tm.MysqlDaemon.SetReplicationPosition(ctx, pos); err != nil {
		return err
	}
	if err := tm.MysqlDaemon.SetReplicationSource(ctx, ti.Tablet.MysqlHostname, int(ti.Tablet.MysqlPort), false /* stopReplicationBefore */, true /* startReplicationAfter */); err != nil {
		return err
	}

	// wait until we get the replicated row, or our context times out
	return tm.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS)
}

// DemotePrimary prepares a PRIMARY tablet to give up leadership to another tablet.
//
// It attemps to idempotently ensure the following guarantees upon returning
// successfully:
//   - No future writes will be accepted.
//   - No writes are in-flight.
//   - MySQL is in read-only mode.
//   - Semi-sync settings are consistent with a REPLICA tablet.
//
// If necessary, it waits for all in-flight writes to complete or time out.
//
// It should be safe to call this on a PRIMARY tablet that was already demoted,
// or on a tablet that already transitioned to REPLICA.
//
// If a step fails in the middle, it will try to undo any changes it made.
func (tm *TabletManager) DemotePrimary(ctx context.Context) (*replicationdatapb.PrimaryStatus, error) {
	log.Infof("DemotePrimary")
	// The public version always reverts on partial failure.
	return tm.demotePrimary(ctx, true /* revertPartialFailure */)
}

// demotePrimary implements DemotePrimary with an additional, private option.
//
// If revertPartialFailure is true, and a step fails in the middle, it will try
// to undo any changes it made.
func (tm *TabletManager) demotePrimary(ctx context.Context, revertPartialFailure bool) (primaryStatus *replicationdatapb.PrimaryStatus, finalErr error) {
	if err := tm.lock(ctx); err != nil {
		return nil, err
	}
	defer tm.unlock()

	tablet := tm.Tablet()
	wasPrimary := tablet.Type == topodatapb.TabletType_PRIMARY
	wasServing := tm.QueryServiceControl.IsServing()
	wasReadOnly, err := tm.MysqlDaemon.IsReadOnly()
	if err != nil {
		return nil, err
	}

	// If we are a primary tablet and not yet read-only, stop accepting new
	// queries and wait for in-flight queries to complete. If we are not primary,
	// or if we are already read-only, there's no need to stop the queryservice
	// in order to ensure the guarantee we are being asked to provide, which is
	// that no writes are occurring.
	if wasPrimary && !wasReadOnly {
		// Note that this may block until the transaction timeout if clients
		// don't finish their transactions in time. Even if some transactions
		// have to be killed at the end of their timeout, this will be
		// considered successful. If we are already not serving, this will be
		// idempotent.
		log.Infof("DemotePrimary disabling query service")
		if err := tm.QueryServiceControl.SetServingType(tablet.Type, logutil.ProtoToTime(tablet.PrimaryTermStartTime), false, "demotion in progress"); err != nil {
			return nil, vterrors.Wrap(err, "SetServingType(serving=false) failed")
		}
		defer func() {
			if finalErr != nil && revertPartialFailure && wasServing {
				if err := tm.QueryServiceControl.SetServingType(tablet.Type, logutil.ProtoToTime(tablet.PrimaryTermStartTime), true, ""); err != nil {
					log.Warningf("SetServingType(serving=true) failed during revert: %v", err)
				}
			}
		}()
	}

	// Now that we know no writes are in-flight and no new writes can occur,
	// set MySQL to read-only mode. If we are already read-only because of a
	// previous demotion, or because we are not primary anyway, this should be
	// idempotent.
	if setSuperReadOnly {
		// Setting super_read_only also sets read_only
		if err := tm.MysqlDaemon.SetSuperReadOnly(true); err != nil {
			if strings.Contains(err.Error(), strconv.Itoa(mysql.ERUnknownSystemVariable)) {
				log.Warningf("server does not know about super_read_only, continuing anyway...")
			} else {
				return nil, err
			}
		}
	} else {
		if err := tm.MysqlDaemon.SetReadOnly(true); err != nil {
			return nil, err
		}
	}
	defer func() {
		if finalErr != nil && revertPartialFailure && !wasReadOnly {
			// setting read_only OFF will also set super_read_only OFF if it was set
			if err := tm.MysqlDaemon.SetReadOnly(false); err != nil {
				log.Warningf("SetReadOnly(false) failed during revert: %v", err)
			}
		}
	}()

	// Here, we check if the primary side semi sync is enabled or not. If it isn't enabled then we do not need to take any action.
	// If it is enabled then we should turn it off and revert in case of failure.
	if tm.isPrimarySideSemiSyncEnabled() {
		// If using semi-sync, we need to disable primary-side.
		if err := tm.fixSemiSync(topodatapb.TabletType_REPLICA, SemiSyncActionSet); err != nil {
			return nil, err
		}
		defer func() {
			if finalErr != nil && revertPartialFailure && wasPrimary {
				// enable primary-side semi-sync again
				if err := tm.fixSemiSync(topodatapb.TabletType_PRIMARY, SemiSyncActionSet); err != nil {
					log.Warningf("fixSemiSync(PRIMARY) failed during revert: %v", err)
				}
			}
		}()
	}

	// Return the current replication position.
	status, err := tm.MysqlDaemon.PrimaryStatus(ctx)
	if err != nil {
		return nil, err
	}
	return mysql.PrimaryStatusToProto(status), nil
}

// UndoDemotePrimary reverts a previous call to DemotePrimary
// it sets read-only to false, fixes semi-sync
// and returns its primary position.
func (tm *TabletManager) UndoDemotePrimary(ctx context.Context, semiSync bool) error {
	log.Infof("UndoDemotePrimary")
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	// If using semi-sync, we need to enable source-side.
	if err := tm.fixSemiSync(topodatapb.TabletType_PRIMARY, convertBoolToSemiSyncAction(semiSync)); err != nil {
		return err
	}

	// Now, set the server read-only false.
	if err := tm.MysqlDaemon.SetReadOnly(false); err != nil {
		return err
	}

	// Update serving graph
	tablet := tm.Tablet()
	log.Infof("UndoDemotePrimary re-enabling query service")
	if err := tm.QueryServiceControl.SetServingType(tablet.Type, logutil.ProtoToTime(tablet.PrimaryTermStartTime), true, ""); err != nil {
		return vterrors.Wrap(err, "SetServingType(serving=true) failed")
	}
	return nil
}

// ReplicaWasPromoted promotes a replica to primary, no questions asked.
func (tm *TabletManager) ReplicaWasPromoted(ctx context.Context) error {
	log.Infof("ReplicaWasPromoted")
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()
	return tm.changeTypeLocked(ctx, topodatapb.TabletType_PRIMARY, DBActionNone, SemiSyncActionNone)
}

// ResetReplicationParameters resets the replica replication parameters
func (tm *TabletManager) ResetReplicationParameters(ctx context.Context) error {
	log.Infof("ResetReplicationParameters")
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	err := tm.MysqlDaemon.StopReplication(tm.hookExtraEnv())
	if err != nil {
		return err
	}

	err = tm.MysqlDaemon.ResetReplicationParameters(ctx)
	if err != nil {
		return err
	}
	return nil
}

// SetReplicationSource sets replication primary, and waits for the
// reparent_journal table entry up to context timeout
func (tm *TabletManager) SetReplicationSource(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool, semiSync bool) error {
	log.Infof("SetReplicationSource: parent: %v  position: %s force: %v semiSync: %v timeCreatedNS: %d", parentAlias, waitPosition, forceStartReplication, semiSync, timeCreatedNS)
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	// setReplicationSourceLocked also fixes the semi-sync. In case the tablet type is primary it assumes that it will become a replica if SetReplicationSource
	// is called, so we always call fixSemiSync with a non-primary tablet type. This will always set the source side replication to false.
	return tm.setReplicationSourceLocked(ctx, parentAlias, timeCreatedNS, waitPosition, forceStartReplication, convertBoolToSemiSyncAction(semiSync))
}

func (tm *TabletManager) setReplicationSourceRepairReplication(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) (err error) {
	parent, err := tm.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}

	ctx, unlock, lockErr := tm.TopoServer.LockShard(ctx, parent.Tablet.GetKeyspace(), parent.Tablet.GetShard(), fmt.Sprintf("repairReplication to %v as parent)", topoproto.TabletAliasString(parentAlias)))
	if lockErr != nil {
		return lockErr
	}

	defer unlock(&err)

	return tm.setReplicationSourceLocked(ctx, parentAlias, timeCreatedNS, waitPosition, forceStartReplication, SemiSyncActionNone)
}

func (tm *TabletManager) setReplicationSourceSemiSyncNoAction(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) error {
	log.Infof("SetReplicationSource: parent: %v  position: %v force: %v", parentAlias, waitPosition, forceStartReplication)
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	return tm.setReplicationSourceLocked(ctx, parentAlias, timeCreatedNS, waitPosition, forceStartReplication, SemiSyncActionNone)
}

func (tm *TabletManager) setReplicationSourceLocked(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool, semiSync SemiSyncAction) (err error) {
	// Change our type to REPLICA if we used to be PRIMARY.
	// Being sent SetReplicationSource means another PRIMARY has been successfully promoted,
	// so we convert to REPLICA first, since we want to do it even if other
	// steps fail below.
	// Note it is important to check for PRIMARY here so that we don't
	// unintentionally change the type of RDONLY tablets
	tablet := tm.Tablet()
	if tablet.Type == topodatapb.TabletType_PRIMARY {
		if err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_REPLICA, DBActionNone); err != nil {
			return err
		}
	}

	// See if we were replicating at all, and should be replicating.
	wasReplicating := false
	shouldbeReplicating := false
	status, err := tm.MysqlDaemon.ReplicationStatus()
	if err == mysql.ErrNotReplica {
		// This is a special error that means we actually succeeded in reading
		// the status, but the status is empty because replication is not
		// configured. We assume this means we used to be a primary, so we always
		// try to start replicating once we are told who the new primary is.
		shouldbeReplicating = true
		// Since we continue in the case of this error, make sure 'status' is
		// in a known, empty state.
		status = mysql.ReplicationStatus{}
	} else if err != nil {
		// Abort on any other non-nil error.
		return err
	}
	if status.IOHealthy() || status.SQLHealthy() {
		wasReplicating = true
		shouldbeReplicating = true
	}
	if forceStartReplication {
		shouldbeReplicating = true
	}

	// If using semi-sync, we need to enable it before connecting to primary.
	// If we are currently PRIMARY, assume we are about to become REPLICA.
	tabletType := tm.Tablet().Type
	if tabletType == topodatapb.TabletType_PRIMARY {
		tabletType = topodatapb.TabletType_REPLICA
	}
	if err := tm.fixSemiSync(tabletType, semiSync); err != nil {
		return err
	}
	// Update the primary/source address only if needed.
	// We don't want to interrupt replication for no reason.
	if parentAlias == nil {
		// if there is no primary in the shard, return an error so that we can retry
		return vterrors.New(vtrpc.Code_FAILED_PRECONDITION, "Shard primaryAlias is nil")
	}
	parent, err := tm.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}
	host := parent.Tablet.MysqlHostname
	port := int(parent.Tablet.MysqlPort)
	// We want to reset the replication parameters and set replication source again when forceStartReplication is provided
	// because sometimes MySQL gets stuck due to improper initialization of master info structure or related failures and throws errors like
	// ERROR 1201 (HY000): Could not initialize master info structure; more error messages can be found in the MySQL error log
	// These errors can only be resolved by resetting the replication parameters, otherwise START SLAVE fails. So when this RPC
	// gets called from VTOrc or replication manager to fix the replication in these cases with forceStartReplication, we should also
	// reset the replication parameters and set the source port information again.
	if status.SourceHost != host || status.SourcePort != port || forceStartReplication {
		// This handles reseting the replication parameters, changing the address and then starting the replication.
		if err := tm.MysqlDaemon.SetReplicationSource(ctx, host, port, wasReplicating, shouldbeReplicating); err != nil {
			if err := tm.handleRelayLogError(err); err != nil {
				return err
			}
		}
	} else if shouldbeReplicating {
		// The address is correct. We need to restart replication so that any semi-sync changes if any
		// are taken into account
		if err := tm.MysqlDaemon.StopReplication(tm.hookExtraEnv()); err != nil {
			if err := tm.handleRelayLogError(err); err != nil {
				return err
			}
		}
		if err := tm.MysqlDaemon.StartReplication(tm.hookExtraEnv()); err != nil {
			if err := tm.handleRelayLogError(err); err != nil {
				return err
			}
		}
	}

	// If needed, wait until we replicate to the specified point, or our context
	// times out. Callers can specify the point to wait for as either a
	// GTID-based replication position or a Vitess reparent journal entry,
	// or both.
	if shouldbeReplicating {
		log.Infof("Set up MySQL replication; should now be replicating from %s at %s", parentAlias, waitPosition)
		if waitPosition != "" {
			pos, err := mysql.DecodePosition(waitPosition)
			if err != nil {
				return err
			}
			if err := tm.MysqlDaemon.WaitSourcePos(ctx, pos); err != nil {
				return err
			}
		}
		if timeCreatedNS != 0 {
			if err := tm.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS); err != nil {
				return err
			}
		}
	}

	return nil
}

// ReplicaWasRestarted updates the parent record for a tablet.
func (tm *TabletManager) ReplicaWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error {
	log.Infof("ReplicaWasRestarted: parent: %v", parent)
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	// Only change type of former PRIMARY tablets.
	// Don't change type of RDONLY
	tablet := tm.Tablet()
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		return nil
	}
	return tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_REPLICA, DBActionNone)
}

// StopReplicationAndGetStatus stops MySQL replication, and returns the
// current status.
func (tm *TabletManager) StopReplicationAndGetStatus(ctx context.Context, stopReplicationMode replicationdatapb.StopReplicationMode) (StopReplicationAndGetStatusResponse, error) {
	log.Infof("StopReplicationAndGetStatus: mode: %v", stopReplicationMode)
	if err := tm.lock(ctx); err != nil {
		return StopReplicationAndGetStatusResponse{}, err
	}
	defer tm.unlock()

	// Get the status before we stop replication.
	// Doing this first allows us to return the status in the case that stopping replication
	// returns an error, so a user can optionally inspect the status before a stop was called.
	rs, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		return StopReplicationAndGetStatusResponse{}, vterrors.Wrap(err, "before status failed")
	}
	before := mysql.ReplicationStatusToProto(rs)

	if stopReplicationMode == replicationdatapb.StopReplicationMode_IOTHREADONLY {
		if !rs.IOHealthy() {
			return StopReplicationAndGetStatusResponse{
				Status: &replicationdatapb.StopReplicationStatus{
					Before: before,
					After:  before,
				},
			}, nil
		}
		if err := tm.stopIOThreadLocked(ctx); err != nil {
			return StopReplicationAndGetStatusResponse{
				Status: &replicationdatapb.StopReplicationStatus{
					Before: before,
				},
			}, vterrors.Wrap(err, "stop io thread failed")
		}
	} else {
		if !rs.Healthy() {
			// no replication is running, just return what we got
			return StopReplicationAndGetStatusResponse{
				Status: &replicationdatapb.StopReplicationStatus{
					Before: before,
					After:  before,
				},
			}, nil
		}
		if err := tm.stopReplicationLocked(ctx); err != nil {
			return StopReplicationAndGetStatusResponse{
				Status: &replicationdatapb.StopReplicationStatus{
					Before: before,
				},
			}, vterrors.Wrap(err, "stop replication failed")
		}
	}

	// Get the status after we stop replication so we have up to date position and relay log positions.
	rsAfter, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		return StopReplicationAndGetStatusResponse{
			Status: &replicationdatapb.StopReplicationStatus{
				Before: before,
			},
		}, vterrors.Wrap(err, "acquiring replication status failed")
	}
	after := mysql.ReplicationStatusToProto(rsAfter)

	rs.Position = rsAfter.Position
	rs.RelayLogPosition = rsAfter.RelayLogPosition
	rs.FilePosition = rsAfter.FilePosition
	rs.RelayLogSourceBinlogEquivalentPosition = rsAfter.RelayLogSourceBinlogEquivalentPosition

	return StopReplicationAndGetStatusResponse{
		Status: &replicationdatapb.StopReplicationStatus{
			Before: before,
			After:  after,
		},
	}, nil
}

// StopReplicationAndGetStatusResponse holds the original hybrid Status struct, as well as a new Status field, which
// hold the result of show replica status called before stopping replication, and after stopping replication.
type StopReplicationAndGetStatusResponse struct {
	// Status represents the replication status call right before, and right after telling the replica to stop.
	Status *replicationdatapb.StopReplicationStatus
}

// PromoteReplica makes the current tablet the primary
func (tm *TabletManager) PromoteReplica(ctx context.Context, semiSync bool) (string, error) {
	log.Infof("PromoteReplica")
	if err := tm.lock(ctx); err != nil {
		return "", err
	}
	defer tm.unlock()

	pos, err := tm.MysqlDaemon.Promote(tm.hookExtraEnv())
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := tm.fixSemiSync(topodatapb.TabletType_PRIMARY, convertBoolToSemiSyncAction(semiSync)); err != nil {
		return "", err
	}

	if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_PRIMARY, DBActionSetReadWrite, SemiSyncActionNone); err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

func isPrimaryEligible(tabletType topodatapb.TabletType) bool {
	switch tabletType {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return true
	}

	return false
}

func (tm *TabletManager) fixSemiSync(tabletType topodatapb.TabletType, semiSync SemiSyncAction) error {
	switch semiSync {
	case SemiSyncActionNone:
		return nil
	case SemiSyncActionSet:
		// Always enable replica-side since it doesn't hurt to keep it on for a primary.
		// The primary-side needs to be off for a replica, or else it will get stuck.
		return tm.MysqlDaemon.SetSemiSyncEnabled(tabletType == topodatapb.TabletType_PRIMARY, true)
	case SemiSyncActionUnset:
		return tm.MysqlDaemon.SetSemiSyncEnabled(false, false)
	default:
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "Unknown SemiSyncAction - %v", semiSync)
	}
}

func (tm *TabletManager) isPrimarySideSemiSyncEnabled() bool {
	semiSyncEnabled, _ := tm.MysqlDaemon.SemiSyncEnabled()
	return semiSyncEnabled
}

func (tm *TabletManager) fixSemiSyncAndReplication(tabletType topodatapb.TabletType, semiSync SemiSyncAction) error {
	if semiSync == SemiSyncActionNone {
		// Semi-sync handling is not required.
		return nil
	}

	if tabletType == topodatapb.TabletType_PRIMARY {
		// Primary is special. It is always handled at the
		// right time by the reparent operations, it doesn't
		// need to be fixed.
		return nil
	}

	if err := tm.fixSemiSync(tabletType, semiSync); err != nil {
		return vterrors.Wrapf(err, "failed to fixSemiSync(%v)", tabletType)
	}

	// If replication is running, but the status is wrong,
	// we should restart replication. First, let's make sure
	// replication is running.
	status, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		// Replication is not configured, nothing to do.
		return nil
	}
	if !status.IOHealthy() {
		// IO thread is not running, nothing to do.
		return nil
	}

	//shouldAck := semiSync == SemiSyncActionSet
	shouldAck := isPrimaryEligible(tabletType)
	acking, err := tm.MysqlDaemon.SemiSyncReplicationStatus()
	if err != nil {
		return vterrors.Wrap(err, "failed to get SemiSyncReplicationStatus")
	}
	if shouldAck == acking {
		return nil
	}

	// We need to restart replication
	log.Infof("Restarting replication for semi-sync flag change to take effect from %v to %v", acking, shouldAck)
	if err := tm.MysqlDaemon.StopReplication(tm.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "failed to StopReplication")
	}
	if err := tm.MysqlDaemon.StartReplication(tm.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "failed to StartReplication")
	}
	return nil
}

func (tm *TabletManager) handleRelayLogError(err error) error {
	// attempt to fix this error:
	// Slave failed to initialize relay log info structure from the repository (errno 1872) (sqlstate HY000) during query: START SLAVE
	// see https://bugs.mysql.com/bug.php?id=83713 or https://github.com/vitessio/vitess/issues/5067
	if strings.Contains(err.Error(), "Slave failed to initialize relay log info structure from the repository") {
		// Stop, reset and start replication again to resolve this error
		if err := tm.MysqlDaemon.RestartReplication(tm.hookExtraEnv()); err != nil {
			return err
		}
		return nil
	}
	return err
}

// repairReplication tries to connect this server to whoever is
// the current primary of the shard, and start replicating.
func (tm *TabletManager) repairReplication(ctx context.Context) error {
	tablet := tm.Tablet()

	si, err := tm.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return err
	}
	if !si.HasPrimary() {
		return fmt.Errorf("no primary tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
	}

	if topoproto.TabletAliasEqual(si.PrimaryAlias, tablet.Alias) {
		// The shard record says we are primary, but we disagree; we wouldn't
		// reach this point unless we were told to check replication.
		// Hopefully someone is working on fixing that, but in any case,
		// we should not try to reparent to ourselves.
		return fmt.Errorf("shard %v/%v record claims tablet %v is primary, but its type is %v", tablet.Keyspace, tablet.Shard, topoproto.TabletAliasString(tablet.Alias), tablet.Type)
	}
	return tm.setReplicationSourceRepairReplication(ctx, si.PrimaryAlias, 0, "", true)
}
