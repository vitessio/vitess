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

// vt backup job: Restore a Backup and Takes a new backup
package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	initDbNameOverride       = flag.String("init_db_name_override", "", "(init parameter) override the name of the db used by vttablet")
	initKeyspace             = flag.String("init_keyspace", "", "(init parameter) keyspace to use for this tablet")
	initShard                = flag.String("init_shard", "", "(init parameter) shard to use for this tablet")
	tabletPath               = flag.String("tablet-path", "", "tablet alias")
	concurrency              = flag.Int("concurrency", 4, "(init restore parameter) how many concurrent files to restore at once")
	acceptableReplicationLag = flag.Duration("acceptable_replication_lag", 0, "set what the is the acceptable replication lag to wait for before a backup is taken")
)

func main() {
	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()

	servenv.ParseFlags("vtbackup")

	tabletAlias, err := topoproto.ParseTabletAlias(*tabletPath)
	if err != nil {
		log.Exitf("failed to parse -tablet-path: %v", err)
	}

	dbName := *initDbNameOverride
	if dbName == "" {
		dbName = fmt.Sprintf("vt_%s", *initKeyspace)
	}

	var mycnf *mysqlctl.Mycnf
	var socketFile string
	extraEnv := map[string]string{"TABLET_ALIAS": strconv.FormatUint(uint64(tabletAlias.Uid), 10)}

	if !dbconfigs.HasConnectionParams() {
		var err error
		if mycnf, err = mysqlctl.NewMycnfFromFlags(tabletAlias.Uid); err != nil {
			log.Exitf("mycnf read failed: %v", err)
		}
		socketFile = mycnf.SocketFile
	} else {
		log.Info("connection parameters were specified. Not loading my.cnf.")
	}

	dbcfgs, err := dbconfigs.Init(socketFile)
	if err != nil {
		log.Warning(err)
	}

	topoServer := topo.Open()
	mysqld := mysqlctl.NewMysqld(dbcfgs)
	dir := fmt.Sprintf("%v/%v", *initKeyspace, *initShard)

	pos, err := mysqlctl.Restore(context.Background(), mycnf, mysqld, dir, *concurrency, extraEnv, map[string]string{}, logutil.NewConsoleLogger(), true, dbName)
	switch err {
	case nil:
		// Horray Evenything worked
		// We have restored a backup ( If one existed, now make sure replicatoin is started
		if err := resetReplication(context.Background(), pos, mysqld); err != nil {
			log.Fatalf("Error Starting Replication %v", err)
		}

	case mysqlctl.ErrNoBackup:
		// No-op, starting with empty database.
	case mysqlctl.ErrExistingDB:
		// No-op, assuming we've just restarted.  Note the
		// replication reporter may restart replication at the
		// next health check if it thinks it should. We do not
		// alter replication here.
	default:
		log.Fatalf("Error restoring backup: %v", err)
	}

	// We have restored a backup ( If one existed, now make sure replicatoin is started
	if err := startReplication(context.Background(), pos, mysqld, topoServer); err != nil {
		log.Fatalf("Error Starting Replication %v", err)
	}

	for {
		status, statusErr := mysqld.SlaveStatus()
		if statusErr != nil {
			log.Warning("Error getting Slave Status (%v)", statusErr)
		} else if time.Duration(status.SecondsBehindMaster)*time.Second <= *acceptableReplicationLag {
			break
		}
		if !status.SlaveRunning() {
			log.Warning("Slave has stopped before backup could be taken")
			startReplication(context.Background(), pos, mysqld, topoServer)
		}
		time.Sleep(time.Second)
	}

	// now we can run the backup
	name := fmt.Sprintf("%v.%v", time.Now().UTC().Format("2006-01-02.150405"), topoproto.TabletAliasString(tabletAlias))
	returnErr := mysqlctl.Backup(context.Background(), mycnf, mysqld, logutil.NewConsoleLogger(), dir, name, *concurrency, extraEnv)
	if returnErr != nil {
		log.Fatalf("Error taking backup: %v", returnErr)
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
