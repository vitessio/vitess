// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"flag"
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"

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
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()
	return agent.restoreDataLocked(ctx, logger, deleteBeforeRestore)
}

func (agent *ActionAgent) restoreDataLocked(ctx context.Context, logger logutil.Logger, deleteBeforeRestore bool) error {
	// change type to RESTORE (using UpdateTabletFields so it's
	// always authorized)
	tablet := agent.Tablet()
	originalType := tablet.Type
	if _, err := agent.TopoServer.UpdateTabletFields(ctx, tablet.Alias, func(tablet *topodatapb.Tablet) error {
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
	dir := fmt.Sprintf("%v/%v", tablet.Keyspace, tablet.Shard)
	localMetadata, err := agent.getLocalMetadataValues()
	if err != nil {
		return err
	}
	pos, err := mysqlctl.Restore(ctx, agent.MysqlDaemon, dir, *restoreConcurrency, agent.hookExtraEnv(), localMetadata, logger, deleteBeforeRestore)
	switch err {
	case nil:
		// Reconnect to master.
		if err := agent.startReplication(ctx, pos); err != nil {
			return err
		}
	case mysqlctl.ErrNoBackup:
		// No-op, starting with empty database.
	case mysqlctl.ErrExistingDB:
		// No-op, assuming we've just restarted.
	default:
		return fmt.Errorf("Can't restore backup: %v", err)
	}

	// Change type back to original type if we're ok to serve.
	if _, err := agent.TopoServer.UpdateTabletFields(ctx, tablet.Alias, func(tablet *topodatapb.Tablet) error {
		tablet.Type = originalType
		return nil
	}); err != nil {
		return fmt.Errorf("Cannot change type back to %v: %v", originalType, err)
	}

	// let's update our internal state (start query service and other things)
	if err := agent.refreshTablet(ctx, "after restore from backup"); err != nil {
		return fmt.Errorf("failed to update state after backup: %v", err)
	}

	return nil
}

func (agent *ActionAgent) startReplication(ctx context.Context, pos replication.Position) error {
	// Set the position at which to resume from the master.
	cmds, err := agent.MysqlDaemon.SetSlavePositionCommands(pos)
	if err != nil {
		return err
	}
	if err := agent.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
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
	if *enableSemiSync {
		if err := agent.enableSemiSync(false); err != nil {
			return err
		}
	}

	// Set master and start slave.
	cmds, err = agent.MysqlDaemon.SetMasterCommands(ti.Hostname, int(ti.PortMap["mysql"]))
	if err != nil {
		return fmt.Errorf("MysqlDaemon.SetMasterCommands failed: %v", err)
	}
	cmds = append(cmds, "START SLAVE")
	if err := agent.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return fmt.Errorf("failed to start replication: %v", err)
	}

	return nil
}

func (agent *ActionAgent) getLocalMetadataValues() (map[string]string, error) {
	tablet := agent.Tablet()
	values := map[string]string{
		"Alias":         topoproto.TabletAliasString(tablet.Alias),
		"ClusterAlias":  fmt.Sprintf("%s.%s", tablet.Keyspace, tablet.Shard),
		"DataCenter":    tablet.Alias.Cell,
		"PromotionRule": "neutral",
	}
	masterEligible, err := agent.isMasterEligible()
	if err != nil {
		return nil, fmt.Errorf("can't determine PromotionRule while populating local_metadata: %v", err)
	}
	if !masterEligible {
		values["PromotionRule"] = "must_not"
	}
	return values, nil
}
