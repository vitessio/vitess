/*
Copyright 2019 The Vitess Authors

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

/*
vtbackup is a batch command to perform a single pass of backup maintenance for a shard.

When run periodically for each shard, vtbackup can ensure these configurable policies:
* There is always a recent backup for the shard.
* Old backups for the shard are removed.

Whatever system launches vtbackup is responsible for the following:
* Running vtbackup with similar flags that would be used for a vttablet and
  mysqlctld in the target shard to be backed up.
* Provisioning as much disk space for vtbackup as would be given to vttablet.
  The data directory MUST be empty at startup. Do NOT reuse a persistent disk.
* Running vtbackup periodically for each shard, for each backup storage location.
* Ensuring that at most one instance runs at a time for a given pair of shard
  and backup storage location.
* Retrying vtbackup if it fails.
* Alerting human operators if the failure is persistent.

The process vtbackup follows to take a new backup is as follows:
1. Restore from the most recent backup.
2. Start a mysqld instance (but no vttablet) from the restored data.
3. Instruct mysqld to connect to the current shard master and replicate any
   transactions that are new since the last backup.
4. Wait until replication is caught up to the master.
5. Stop mysqld and take a new backup.

Aside from additional replication load while vtbackup's mysqld catches up on
new transactions, the shard should be otherwise unaffected. Existing tablets
will continue to serve, and no new tablets will appear in topology, meaning no
query traffic will ever be routed to vtbackup's mysqld. This silent operation
mode helps make backups minimally disruptive to serving capacity and orthogonal
to the handling of the query path.

The command-line parameters to vtbackup specify a policy for when a new backup
is needed, and when old backups should be removed. If the existing backups
already satisfy the policy, then vtbackup will do nothing and return success
immediately.
*/
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"math"
	"math/big"
	"os"
	"time"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// vtbackup-specific flags
	acceptableReplicationLag = flag.Duration("acceptable_replication_lag", 1*time.Second, "Wait until replication lag is less than or equal to this value before taking a new backup")
	timeout                  = flag.Duration("timeout", 1*time.Hour, "Overall timeout for this vtbackup run")

	// vttablet-like flags
	initDbNameOverride = flag.String("init_db_name_override", "", "(init parameter) override the name of the db used by vttablet")
	initKeyspace       = flag.String("init_keyspace", "", "(init parameter) keyspace to use for this tablet")
	initShard          = flag.String("init_shard", "", "(init parameter) shard to use for this tablet")
	concurrency        = flag.Int("concurrency", 4, "(init restore parameter) how many concurrent files to restore at once")

	// mysqlctld-like flags
	mysqlPort     = flag.Int("mysql_port", 3306, "mysql port")
	mysqlSocket   = flag.String("mysql_socket", "", "path to the mysql socket")
	mysqlTimeout  = flag.Duration("mysql_timeout", 5*time.Minute, "how long to wait for mysqld startup")
	initDBSQLFile = flag.String("init_db_sql_file", "", "path to .sql file to run after mysql_install_db")
)

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()

	servenv.ParseFlags("vtbackup")

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// This is an imaginary tablet alias. The value doesn't matter for anything,
	// except that we generate a random UID to ensure the target backup
	// directory is unique if multiple vtbackup instances are launched for the
	// same shard, at exactly the same second, pointed at the same backup
	// storage location.
	bigN, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32))
	if err != nil {
		log.Errorf("can't generate random tablet UID: %v", err)
		exit.Return(1)
	}
	tabletAlias := &topodatapb.TabletAlias{
		Cell: "vtbackup",
		Uid:  uint32(bigN.Uint64()),
	}

	// Clean up our temporary data dir if we exit for any reason, to make sure
	// every invocation of vtbackup starts with a clean slate, and it does not
	// accumulate garbage (and run out of disk space) if it's restarted.
	tabletDir := mysqlctl.TabletDir(tabletAlias.Uid)
	defer func() {
		log.Infof("Removing temporary tablet directory: %v", tabletDir)
		if err := os.RemoveAll(tabletDir); err != nil {
			log.Errorf("Failed to remove temporary tablet directory: %v", err)
		}
	}()

	// Start up mysqld as if we are mysqlctld provisioning a fresh tablet.
	mysqld, mycnf, err := mysqlctl.CreateMysqldAndMycnf(tabletAlias.Uid, *mysqlSocket, int32(*mysqlPort))
	if err != nil {
		log.Errorf("failed to initialize mysql config: %v", err)
		exit.Return(1)
	}
	initCtx, initCancel := context.WithTimeout(ctx, *mysqlTimeout)
	defer initCancel()
	if err := mysqld.Init(initCtx, mycnf, *initDBSQLFile); err != nil {
		log.Errorf("failed to initialize mysql data dir and start mysqld: %v", err)
		exit.Return(1)
	}
	// Shut down mysqld when we're done.
	defer func() {
		// Be careful not to use the original context, because we don't want to
		// skip shutdown just because we timed out waiting for other things.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		mysqld.Shutdown(ctx, mycnf, false)
	}()

	// Restore from backup.
	dbName := *initDbNameOverride
	if dbName == "" {
		dbName = fmt.Sprintf("vt_%s", *initKeyspace)
	}
	extraEnv := map[string]string{
		"TABLET_ALIAS": topoproto.TabletAliasString(tabletAlias),
	}
	dir := fmt.Sprintf("%v/%v", *initKeyspace, *initShard)

	log.Infof("Restoring latest backup from directory %v", dir)
	pos, err := mysqlctl.Restore(ctx, mycnf, mysqld, dir, *concurrency, extraEnv, map[string]string{}, logutil.NewConsoleLogger(), true, dbName)
	switch err {
	case nil:
		log.Info("Successfully restored from backup at replication position %v", pos)
	case mysqlctl.ErrNoBackup:
		log.Error("No backup found. Not starting up empty since -initial_backup flag was not enabled.")
		exit.Return(1)
	case mysqlctl.ErrExistingDB:
		log.Error("Can't run vtbackup because data directory is not empty.")
		exit.Return(1)
	default:
		log.Errorf("Error restoring from backup: %v", err)
		exit.Return(1)
	}

	// We have restored a backup. Now start replication.
	if err := resetReplication(ctx, pos, mysqld); err != nil {
		log.Errorf("Error resetting replication %v", err)
		exit.Return(1)
	}
	topoServer := topo.Open()
	defer topoServer.Close()
	if err := startReplication(ctx, pos, mysqld, topoServer); err != nil {
		log.Errorf("Error starting replication %v", err)
		exit.Return(1)
	}

	// Wait for replication to catch up.
	waitStartTime := time.Now()
	for {
		time.Sleep(time.Second)

		// Check if the context is still good.
		if err := ctx.Err(); err != nil {
			log.Errorf("Timed out waiting for replication to catch up to within %v.", *acceptableReplicationLag)
			exit.Return(1)
		}

		status, statusErr := mysqld.SlaveStatus()
		if statusErr != nil {
			log.Warningf("Error getting replication status: %v", statusErr)
			continue
		}
		if time.Duration(status.SecondsBehindMaster)*time.Second <= *acceptableReplicationLag {
			// We're caught up on replication.
			log.Infof("Replication caught up to within %v after %v", *acceptableReplicationLag, time.Since(waitStartTime))
			break
		}
		if !status.SlaveRunning() {
			log.Warning("Replication has stopped before backup could be taken. Trying to restart replication.")
			if err := startReplication(ctx, pos, mysqld, topoServer); err != nil {
				log.Warningf("Failed to restart replication: %v", err)
			}
		}
	}

	// Now we can take a new backup.
	name := fmt.Sprintf("%v.%v", time.Now().UTC().Format("2006-01-02.150405"), topoproto.TabletAliasString(tabletAlias))
	if err := mysqlctl.Backup(ctx, mycnf, mysqld, logutil.NewConsoleLogger(), dir, name, *concurrency, extraEnv); err != nil {
		log.Errorf("Error taking backup: %v", err)
		exit.Return(1)
	}
}

func resetReplication(ctx context.Context, pos mysql.Position, mysqld mysqlctl.MysqlDaemon) error {
	cmds := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget master host:port.
	}
	if err := mysqld.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return vterrors.Wrap(err, "failed to reset slave")
	}

	// Set the position at which to resume from the master.
	if err := mysqld.SetSlavePosition(ctx, pos); err != nil {
		return vterrors.Wrap(err, "failed to set slave position")
	}
	return nil
}

func startReplication(ctx context.Context, pos mysql.Position, mysqld mysqlctl.MysqlDaemon, topoServer *topo.Server) error {
	si, err := topoServer.GetShard(ctx, *initKeyspace, *initShard)
	if err != nil {
		return vterrors.Wrap(err, "can't read shard")
	}
	if si.MasterAlias == nil {
		// We've restored, but there's no master. This is fine, since we've
		// already set the position at which to resume when we're later reparented.
		// If we had instead considered this fatal, all tablets would crash-loop
		// until a master appears, which would make it impossible to elect a master.
		log.Warning("Can't start replication after restore: shard has no master.")
		return nil
	}
	ti, err := topoServer.GetTablet(ctx, si.MasterAlias)
	if err != nil {
		return vterrors.Wrapf(err, "Cannot read master tablet %v", si.MasterAlias)
	}

	// Set master and start slave.
	if err := mysqld.SetMaster(ctx, topoproto.MysqlHostname(ti.Tablet), int(topoproto.MysqlPort(ti.Tablet)), false /* slaveStopBefore */, true /* slaveStartAfter */); err != nil {
		return vterrors.Wrap(err, "MysqlDaemon.SetMaster failed")
	}
	return nil
}
