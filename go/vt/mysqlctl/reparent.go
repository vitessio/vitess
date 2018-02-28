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

package mysqlctl

/*
This file contains the reparenting methods for mysqlctl.

TODO(alainjobart) Once refactoring is done, remove unused code paths.
*/

import (
	"fmt"
	"time"

	"vitess.io/vitess/go/mysql"

	"golang.org/x/net/context"
)

// CreateReparentJournal returns the commands to execute to create
// the _vt.reparent_journal table. It is safe to run these commands
// even if the table already exists.
//
// If the table was created by Vitess version 2.0, the following command
// may need to be run:
// ALTER TABLE _vt.reparent_journal MODIFY COLUMN replication_position VARBINARY(64000);
func CreateReparentJournal() []string {
	return []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS _vt.reparent_journal (
  time_created_ns BIGINT UNSIGNED NOT NULL,
  action_name VARBINARY(250) NOT NULL,
  master_alias VARBINARY(32) NOT NULL,
  replication_position VARBINARY(%v) DEFAULT NULL,
  PRIMARY KEY (time_created_ns))
ENGINE=InnoDB`, mysql.MaximumPositionSize)}
}

// PopulateReparentJournal returns the SQL command to use to populate
// the _vt.reparent_journal table, as well as the time_created_ns
// value used.
func PopulateReparentJournal(timeCreatedNS int64, actionName, masterAlias string, pos mysql.Position) string {
	posStr := mysql.EncodePosition(pos)
	if len(posStr) > mysql.MaximumPositionSize {
		posStr = posStr[:mysql.MaximumPositionSize]
	}
	return fmt.Sprintf("INSERT INTO _vt.reparent_journal "+
		"(time_created_ns, action_name, master_alias, replication_position) "+
		"VALUES (%v, '%v', '%v', '%v')",
		timeCreatedNS, actionName, masterAlias, posStr)
}

// queryReparentJournal returns the SQL query to use to query the database
// for a reparent_journal row.
func queryReparentJournal(timeCreatedNS int64) string {
	return fmt.Sprintf("SELECT action_name, master_alias, replication_position FROM _vt.reparent_journal WHERE time_created_ns=%v", timeCreatedNS)
}

// WaitForReparentJournal will wait until the context is done for
// the row in the reparent_journal table.
func (mysqld *Mysqld) WaitForReparentJournal(ctx context.Context, timeCreatedNS int64) error {
	for {
		qr, err := mysqld.FetchSuperQuery(ctx, queryReparentJournal(timeCreatedNS))
		if err == nil && len(qr.Rows) == 1 {
			// we have the row, we're done
			return nil
		}

		// wait a little bit, interrupt if context is done
		t := time.After(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t:
		}
	}
}

// DemoteMaster will gracefully demote a master mysql instance to read only.
// If the master is still alive, then we need to demote it gracefully
// make it read-only, flush the writes and get the position
func (mysqld *Mysqld) DemoteMaster() (rp mysql.Position, err error) {
	cmds := []string{
		"FLUSH TABLES WITH READ LOCK",
		"UNLOCK TABLES",
	}
	if err = mysqld.ExecuteSuperQueryList(context.TODO(), cmds); err != nil {
		return rp, err
	}
	return mysqld.MasterPosition()
}

// PromoteSlave will promote a slave to be the new master.
func (mysqld *Mysqld) PromoteSlave(hookExtraEnv map[string]string) (mysql.Position, error) {
	// Since we handle replication, just stop it.
	cmds := []string{
		SQLStopSlave,
		"RESET SLAVE ALL", // "ALL" makes it forget master host:port.
	}

	// Promote to master.
	if err := mysqld.ExecuteSuperQueryList(context.TODO(), cmds); err != nil {
		return mysql.Position{}, err
	}

	return mysqld.MasterPosition()
}
