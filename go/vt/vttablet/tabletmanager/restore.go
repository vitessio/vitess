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

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file handles the initial backup restore upon startup.
// It is only enabled if restore_from_backup is set.

var (
	restoreFromBackup  = flag.Bool("restore_from_backup", false, "(init restore parameter) will check BackupStorage for a recent backup at startup and start there")
	restoreConcurrency = flag.Int("restore_concurrency", 4, "(init restore parameter) how many concurrent files to restore at once")
)

// RestoreData is the main entry point for backup restore.
// It will either work, fail gracefully, or return
// an error in case of a non-recoverable error.
// It takes the action lock so no RPC interferes.
func (agent *ActionAgent) RestoreData(ctx context.Context, logger logutil.Logger, deleteBeforeRestore bool) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()
	return agent.restoreDataLocked(ctx, logger, deleteBeforeRestore)
}

func (agent *ActionAgent) restoreDataLocked(ctx context.Context, logger logutil.Logger, deleteBeforeRestore bool) error {
	// change type to RESTORE (using UpdateTabletFields so it's
	// always authorized)
	var originalType topodatapb.TabletType
	if _, err := agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias, func(tablet *topodatapb.Tablet) error {
		originalType = tablet.Type
		tablet.Type = topodatapb.TabletType_RESTORE
		return nil
	}); err != nil {
		return fmt.Errorf("Cannot change type to RESTORE: %v", err)
	}

	// let's update our internal state (stop query service and other things)
	if err := agent.refreshTablet(ctx, "restore from backup"); err != nil {
		return fmt.Errorf("failed to update state before restore: %v", err)
	}

	// Try to restore. Depending on the reason for failure, we may be ok.
	// If we're not ok, return an error and the agent will log.Fatalf,
	// causing the process to be restarted and the restore retried.
	// Record local metadata values based on the original type.
	localMetadata := agent.getLocalMetadataValues(originalType)
	tablet := agent.Tablet()
	dir := fmt.Sprintf("%v/%v", tablet.Keyspace, tablet.Shard)
	pos, err := mysqlctl.Restore(ctx, agent.MysqlDaemon, dir, *restoreConcurrency, agent.hookExtraEnv(), localMetadata, logger, deleteBeforeRestore, topoproto.TabletDbName(tablet))
	switch err {
	case nil:
		// Starting from here we won't be able to recover if we get stopped by a cancelled
		// context. Thus we use the background context to get through to the finish.

		// Reconnect to master.
		if err := agent.startReplication(context.Background(), pos, originalType); err != nil {
			return err
		}
	case mysqlctl.ErrNoBackup:
		// No-op, starting with empty database.
	case mysqlctl.ErrExistingDB:
		// No-op, assuming we've just restarted.  Note the
		// replication reporter may restart replication at the
		// next health check if it thinks it should. We do not
		// alter replication here.
	default:
		return fmt.Errorf("Can't restore backup: %v", err)
	}

	// If we had type BACKUP or RESTORE it's better to set our type to the init_tablet_type to make result of the restore
	// similar to completely clean start from scratch.
	if (originalType == topodatapb.TabletType_BACKUP || originalType == topodatapb.TabletType_RESTORE) && *initTabletType != "" {
		initType, err := topoproto.ParseTabletType(*initTabletType)
		if err == nil {
			originalType = initType
		}
	}

	// Change type back to original type if we're ok to serve.
	if _, err := agent.TopoServer.UpdateTabletFields(context.Background(), tablet.Alias, func(tablet *topodatapb.Tablet) error {
		tablet.Type = originalType
		return nil
	}); err != nil {
		return fmt.Errorf("Cannot change type back to %v: %v", originalType, err)
	}

	// let's update our internal state (start query service and other things)
	if err := agent.refreshTablet(context.Background(), "after restore from backup"); err != nil {
		return fmt.Errorf("failed to update state after backup: %v", err)
	}

	return nil
}

func (agent *ActionAgent) startReplication(ctx context.Context, pos mysql.Position, tabletType topodatapb.TabletType) error {
	cmds := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget master host:port.
	}
	if err := agent.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return fmt.Errorf("failed to reset slave: %v", err)
	}

	// Set the position at which to resume from the master.
	if err := agent.MysqlDaemon.SetSlavePosition(ctx, pos); err != nil {
		return fmt.Errorf("failed to set slave position: %v", err)
	}

	// Read the shard to find the current master, and its location.
	tablet := agent.Tablet()
	si, err := agent.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return fmt.Errorf("can't read shard: %v", err)
	}
	if si.MasterAlias == nil {
		// We've restored, but there's no master. This is fine, since we've
		// already set the position at which to resume when we're later reparented.
		// If we had instead considered this fatal, all tablets would crash-loop
		// until a master appears, which would make it impossible to elect a master.
		log.Warningf("Can't start replication after restore: shard %v/%v has no master.", tablet.Keyspace, tablet.Shard)
		return nil
	}
	if topoproto.TabletAliasEqual(si.MasterAlias, tablet.Alias) {
		// We used to be the master before we got restarted in an empty data dir,
		// and no other master has been elected in the meantime.
		// This shouldn't happen, so we'll let the operator decide which tablet
		// should actually be promoted to master.
		log.Warningf("Can't start replication after restore: master record still points to this tablet.")
		return nil
	}
	ti, err := agent.TopoServer.GetTablet(ctx, si.MasterAlias)
	if err != nil {
		return fmt.Errorf("Cannot read master tablet %v: %v", si.MasterAlias, err)
	}

	// If using semi-sync, we need to enable it before connecting to master.
	if err := agent.fixSemiSync(tabletType); err != nil {
		return err
	}

	// Set master and start slave.
	if err := agent.MysqlDaemon.SetMaster(ctx, topoproto.MysqlHostname(ti.Tablet), int(topoproto.MysqlPort(ti.Tablet)), false /* slaveStopBefore */, true /* slaveStartAfter */); err != nil {
		return fmt.Errorf("MysqlDaemon.SetMaster failed: %v", err)
	}
	return nil
}

func (agent *ActionAgent) getLocalMetadataValues(tabletType topodatapb.TabletType) map[string]string {
	tablet := agent.Tablet()
	values := map[string]string{
		"Alias":         topoproto.TabletAliasString(tablet.Alias),
		"ClusterAlias":  fmt.Sprintf("%s.%s", tablet.Keyspace, tablet.Shard),
		"DataCenter":    tablet.Alias.Cell,
		"PromotionRule": "must_not",
	}
	if isMasterEligible(tabletType) {
		values["PromotionRule"] = "neutral"
	}
	return values
}
