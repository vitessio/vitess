// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// Backup takes a db backup and sends it to the BackupStorage
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) Backup(ctx context.Context, concurrency int, logger logutil.Logger) error {
	// update our type to BACKUP
	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type == topodatapb.TabletType_MASTER {
		return fmt.Errorf("type MASTER cannot take backup, if you really need to do this, restart vttablet in replica mode")
	}
	originalType := tablet.Type
	if _, err := topotools.ChangeType(ctx, agent.TopoServer, tablet.Alias, topodatapb.TabletType_BACKUP, make(map[string]string)); err != nil {
		return err
	}

	// let's update our internal state (stop query service and other things)
	if err := agent.refreshTablet(ctx, "backup"); err != nil {
		return fmt.Errorf("failed to update state before backup: %v", err)
	}

	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// now we can run the backup
	dir := fmt.Sprintf("%v/%v", tablet.Keyspace, tablet.Shard)
	name := fmt.Sprintf("%v.%v", time.Now().UTC().Format("2006-01-02.150405"), topoproto.TabletAliasString(tablet.Alias))
	returnErr := mysqlctl.Backup(ctx, agent.MysqlDaemon, l, dir, name, concurrency, agent.hookExtraEnv())

	// and change our type back to the appropriate value:
	// - if healthcheck is enabled, go to spare
	// - if not, go back to original type
	if agent.IsRunningHealthCheck() {
		originalType = topodatapb.TabletType_SPARE
	}
	_, err = topotools.ChangeType(ctx, agent.TopoServer, tablet.Alias, originalType, nil)
	if err != nil {
		// failure in changing the topology type is probably worse,
		// so returning that (we logged the snapshot error anyway)
		if returnErr != nil {
			l.Errorf("mysql backup command returned error: %v", returnErr)
		}
		returnErr = err
	}

	return returnErr
}
