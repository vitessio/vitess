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

// AlterReparentJournal returns the commands to execute to change
// column master_alias -> primary_alias or the other way
// In 13.0.0 we introduce renaming of primary_alias -> master_alias.
// This is to support in-place downgrade from a later version.
// In 14.0.0 we will replace this with renaming of master_alias -> primary_alias.
// This is to support in-place upgrades from 13.0.x to 14.0.x
func AlterReparentJournal() []string {
	return []string{
		"ALTER TABLE _vt.reparent_journal CHANGE COLUMN primary_alias master_alias VARBINARY(32) NOT NULL",
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
		"(time_created_ns, action_name, master_alias, replication_position) "+
		"VALUES (%v, '%v', '%v', '%v')",
		timeCreatedNS, actionName, primaryAlias, posStr)
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
