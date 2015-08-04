// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"flag"
	"fmt"

	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
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
	if err := agent.TopoServer.UpdateTabletFields(ctx, tablet.Alias, func(tablet *topo.Tablet) error {
		tablet.Type = topo.TYPE_RESTORE
		return nil
	}); err != nil {
		return fmt.Errorf("Cannot change type to RESTORE: %v", err)
	}

	// do the optional restore, if that fails we are in a bad state,
	// just log.Fatalf out.
	bucket := fmt.Sprintf("%v/%v", tablet.Keyspace, tablet.Shard)
	pos, err := mysqlctl.Restore(ctx, agent.MysqlDaemon, bucket, *restoreConcurrency, agent.hookExtraEnv())
	if err != nil && err != mysqlctl.ErrNoBackup {
		return fmt.Errorf("Cannot restore original backup: %v", err)
	}

	if err == nil {
		// now read the shard to find the current master, and its location
		si, err := agent.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
		if err != nil {
			return fmt.Errorf("Cannot read shard: %v", err)
		}
		if si.MasterAlias == nil {
			return fmt.Errorf("Shard %v/%v has no master", tablet.Keyspace, tablet.Shard)
		}
		ti, err := agent.TopoServer.GetTablet(ctx, topo.ProtoToTabletAlias(si.MasterAlias))
		if err != nil {
			return fmt.Errorf("Cannot read master tablet %v: %v", si.MasterAlias, err)
		}

		// set replication straight
		status := &myproto.ReplicationStatus{
			Position:   pos,
			MasterHost: ti.Hostname,
			MasterPort: ti.Portmap["mysql"],
		}
		cmds, err := agent.MysqlDaemon.StartReplicationCommands(status)
		if err != nil {
			return fmt.Errorf("MysqlDaemon.StartReplicationCommands failed: %v", err)
		}
		if err := agent.MysqlDaemon.ExecuteSuperQueryList(cmds); err != nil {
			return fmt.Errorf("MysqlDaemon.ExecuteSuperQueryList failed: %v", err)
		}
	}

	// change type back to original type
	if err := agent.TopoServer.UpdateTabletFields(ctx, tablet.Alias, func(tablet *topo.Tablet) error {
		tablet.Type = originalType
		return nil
	}); err != nil {
		return fmt.Errorf("Cannot change type back to %v: %v", originalType, err)
	}
	return nil
}
