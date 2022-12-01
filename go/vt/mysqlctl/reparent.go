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

package mysqlctl

/*
This file contains the reparenting methods for mysqlctl.
*/

import (
	"fmt"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"

	"context"
)

// CreateReparentJournal returns the commands to execute to create
// the _vt.reparent_journal table. It is safe to run these commands
// even if the table already exists.
func CreateReparentJournal() []string {
	return []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS _vt.reparent_journal (
  time_created_ns BIGINT UNSIGNED NOT NULL,
  action_name VARBINARY(250) NOT NULL,
  primary_alias VARBINARY(32) NOT NULL,
  replication_position VARBINARY(%v) DEFAULT NULL,
  PRIMARY KEY (time_created_ns))
ENGINE=InnoDB`, mysql.MaximumPositionSize)}
}

// AlterReparentJournal returns the commands to execute to change
// column master_alias -> primary_alias or the other way
// In 13.0.0 we introduced renaming of primary_alias -> master_alias.
// This was to support in-place downgrade from a later version.
// In 14.0.0 we replace that with renaming of master_alias -> primary_alias.
// This is to support in-place upgrades from 13.0.x to 14.0.x
func AlterReparentJournal() []string {
	return []string{
		"ALTER TABLE _vt.reparent_journal CHANGE COLUMN master_alias primary_alias VARBINARY(32) NOT NULL",
	}
}

// PopulateReparentJournal returns the SQL command to use to populate
// the _vt.reparent_journal table, as well as the time_created_ns
// value used.
func PopulateReparentJournal(timeCreatedNS int64, actionName, primaryAlias string, pos mysql.Position) string {
	posStr := mysql.EncodePosition(pos)
	if len(posStr) > mysql.MaximumPositionSize {
		posStr = posStr[:mysql.MaximumPositionSize]
	}
	return fmt.Sprintf("INSERT INTO _vt.reparent_journal "+
		"(time_created_ns, action_name, primary_alias, replication_position) "+
		"VALUES (%v, '%v', '%v', '%v')",
		timeCreatedNS, actionName, primaryAlias, posStr)
}

// queryReparentJournal returns the SQL query to use to query the database
// for a reparent_journal row.
func queryReparentJournal(timeCreatedNS int64) string {
	return fmt.Sprintf("SELECT action_name, primary_alias, replication_position FROM _vt.reparent_journal WHERE time_created_ns=%v", timeCreatedNS)
}

// WaitForReparentJournal will wait until the context is done for
// the row in the reparent_journal table.
func (mysqld *Mysqld) WaitForReparentJournal(ctx context.Context, timeCreatedNS int64) error {
	for {
		qr, err := mysqld.FetchSuperQuery(ctx, queryReparentJournal(timeCreatedNS))
		if err != nil {
			log.Infof("error query reparent journal %v", err)
		}
		if err == nil && len(qr.Rows) == 1 {
			// we have the row, we're done
			return nil
		}

		// wait a little bit, interrupt if context is done
		t := time.After(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			log.Warning("WaitForReparentJournal failed to see row before timeout.")
			return ctx.Err()
		case <-t:
		}
	}
}

// Promote will promote this server to be the new primary.
func (mysqld *Mysqld) Promote(hookExtraEnv map[string]string) (mysql.Position, error) {
	ctx := context.TODO()
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return mysql.Position{}, err
	}
	defer conn.Recycle()

	// Since we handle replication, just stop it.
	cmds := []string{
		conn.StopReplicationCommand(),
		"RESET SLAVE ALL", // "ALL" makes it forget primary host:port.
		// When using semi-sync and GTID, a replica first connects to the new primary with a given GTID set,
		// it can take a long time to scan the current binlog file to find the corresponding position.
		// This can cause commits that occur soon after the primary is promoted to take a long time waiting
		// for a semi-sync ACK, since replication is not fully set up.
		// More details in: https://github.com/vitessio/vitess/issues/4161
		"FLUSH BINARY LOGS",
	}

	if err := mysqld.executeSuperQueryListConn(ctx, conn, cmds); err != nil {
		return mysql.Position{}, err
	}
	return conn.PrimaryPosition()
}
