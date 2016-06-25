// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"flag"
	"fmt"

	log "github.com/golang/glog"
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

// RestoreFromBackup is the main entry point for backup restore.
// It will either work, fail gracefully, or return
// an error in case of a non-recoverable error.
// It takes the action lock so no RPC interferes.
func (agent *ActionAgent) RestoreFromBackup(ctx context.Context) error {
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()

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

	// Try to restore. Depending on the reason for failure, we may be ok.
	// If we're not ok, return an error and the agent will log.Fatalf,
	// causing the process to be restarted and the restore retried.
	dir := fmt.Sprintf("%v/%v", tablet.Keyspace, tablet.Shard)
	pos, err := mysqlctl.Restore(ctx, agent.MysqlDaemon, dir, *restoreConcurrency, agent.hookExtraEnv())
	switch err {
	case nil:
		// Populate local_metadata before starting replication,
		// so it's there before we start announcing ourself.
		if err := agent.populateLocalMetadata(ctx); err != nil {
			return err
		}

		// Reconnect to master.
		if err := agent.startReplication(ctx, pos); err != nil {
			return err
		}
	case mysqlctl.ErrNoBackup:
		log.Infof("Auto-restore is enabled, but no backups were found. Starting up empty.")
		if err := agent.populateLocalMetadata(ctx); err != nil {
			return err
		}
	case mysqlctl.ErrExistingDB:
		log.Infof("Auto-restore is enabled, but mysqld already contains data. Assuming vttablet was just restarted.")
		if err := agent.populateLocalMetadata(ctx); err != nil {
			return err
		}
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
