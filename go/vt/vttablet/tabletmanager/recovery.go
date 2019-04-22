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

	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file handles the recovering from an older backup upon startup.
// It is only enabled if restore_for_recovery is set.

var (
	restoreForRecovery = flag.Bool("restore_for_recovery", false, "(init restore parameter) will check BackupStorage for a named backup and start there")
)

// RecoverData is the main entry point for Point-in-time recovery.
// It will either work, fail gracefully, or return
// an error in case of a non-recoverable error.
// It takes the action lock so no RPC interferes.
func (agent *ActionAgent) RecoverData(ctx context.Context, logger logutil.Logger) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()
	if agent.Cnf == nil {
		return fmt.Errorf("cannot perform restore without my.cnf, please restart vttablet with a my.cnf file specified")
	}
	return agent.recoverDataLocked(ctx, logger)
}

func (agent *ActionAgent) recoverDataLocked(ctx context.Context, logger logutil.Logger) error {

	// let's update our internal state (stop query service and other things)
	if err := agent.refreshTablet(ctx, "recover from backup"); err != nil {
		return vterrors.Wrap(err, "failed to update state before recovery")
	}
	// we expect init_tablet_type to be RECOVERY
	// but if somehow get here with a different tablet_type,
	// throw an error
	tablet := agent.Tablet()
	if tablet.Type != topodatapb.TabletType_RECOVERY {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "tablet type is incorrect, expected: RECOVERY, got: %v", tablet.Type)
	}

	// Try to restore. Depending on the reason for failure, we may be ok.
	// If we're not ok, return an error and the agent will log.Fatalf,
	// causing the process to be restarted and the restore retried.
	localMetadata := agent.getLocalMetadataValues(tablet.Type)
	dir := fmt.Sprintf("%v/%v", tablet.Keyspace, tablet.Shard)
	_, err := mysqlctl.RecoverSelectedBackup(ctx, agent.Cnf, agent.MysqlDaemon, dir, *restoreConcurrency, agent.hookExtraEnv(), localMetadata, logger, false /*TODO(deepthi) really? */, topoproto.TabletDbName(tablet))
	switch err {
	case nil:
		// successful, now start query service etc.
	case mysqlctl.ErrNoBackup:
		// No-op, starting with empty database.
	case mysqlctl.ErrExistingDB:
		// No-op, assuming we've just restarted.  Note the
		// replication reporter may restart replication at the
		// next health check if it thinks it should. We do not
		// alter replication here.
	default:
		agent.refreshTablet(ctx, "failed to recover from backup")
		return vterrors.Wrap(err, "Can't recover from backup")
	}

	// let's update our internal state (start query service and other things)
	if err := agent.refreshTablet(context.Background(), "after recovery from backup"); err != nil {
		return vterrors.Wrap(err, "failed to update state after recovery")
	}

	return nil
}
