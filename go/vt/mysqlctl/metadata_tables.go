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

import (
	"bytes"
	"fmt"

	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
)

// Note that definitions of local_metadata and shard_metadata should be the same
// as in testing which is defined in config/init_db.sql.
const (
	sqlCreateLocalMetadataTable = `CREATE TABLE IF NOT EXISTS _vt.local_metadata (
  name VARCHAR(255) NOT NULL,
  value MEDIUMBLOB NOT NULL,
  PRIMARY KEY (name)
  ) ENGINE=InnoDB`
	sqlCreateShardMetadataTable = `CREATE TABLE IF NOT EXISTS _vt.shard_metadata (
  name VARCHAR(255) NOT NULL,
  value MEDIUMBLOB NOT NULL,
  PRIMARY KEY (name)
  ) ENGINE=InnoDB`
	sqlUpdateLocalMetadataTable = "UPDATE _vt.local_metadata SET db_name='%s' WHERE db_name=''"
	sqlUpdateShardMetadataTable = "UPDATE _vt.shard_metadata SET db_name='%s' WHERE db_name=''"
)

var (
	sqlAlterLocalMetadataTable = []string{
		`ALTER TABLE _vt.local_metadata ADD COLUMN db_name VARBINARY(255) NOT NULL DEFAULT ''`,
		`ALTER TABLE _vt.local_metadata DROP PRIMARY KEY, ADD PRIMARY KEY(name, db_name)`,
		// VARCHAR(255) is not long enough to hold replication positions, hence changing to
		// MEDIUMBLOB.
		`ALTER TABLE _vt.local_metadata CHANGE value value MEDIUMBLOB NOT NULL`,
	}
	sqlAlterShardMetadataTable = []string{
		`ALTER TABLE _vt.shard_metadata ADD COLUMN db_name VARBINARY(255) NOT NULL DEFAULT ''`,
		`ALTER TABLE _vt.shard_metadata DROP PRIMARY KEY, ADD PRIMARY KEY(name, db_name)`,
	}
)

// MetadataManager manages the creation and filling of the _vt.local_metadata
// and _vt.shard_metadata tables.
type MetadataManager struct{}

// PopulateMetadataTables creates and fills the _vt.local_metadata table and
// creates the _vt.shard_metadata table.
//
// _vt.local_metadata table is a per-tablet table that is never replicated.
// This allows queries against local_metadata to return different values on
// different tablets, which is used for communicating between Vitess and
// MySQL-level tools like Orchestrator (https://github.com/openark/orchestrator).
//
// _vt.shard_metadata is a replicated table with per-shard information, but it's
// created here to make it easier to create it on databases that were running
// old version of Vitess, or databases that are getting converted to run under
// Vitess.
//
// This function is semantically equivalent to calling updateMetadataTables
// followed immediately by upsertLocalMetadata.
func (m *MetadataManager) PopulateMetadataTables(mysqld MysqlDaemon, localMetadata map[string]string, dbName string) error {
	log.Infof("Populating _vt.local_metadata table... %s", dbName)
	// Get a non-pooled DBA connection.
	conn, err := mysqld.GetDbaConnection(context.TODO())
	if err != nil {
		return err
	}
	defer conn.Close()

	// Disable replication on this session. We close the connection after using
	// it, so there's no need to re-enable replication when we're done.
	if _, err := conn.ExecuteFetch("SET @@session.sql_log_bin = 0", 0, false); err != nil {
		return err
	}

	// Create the database and table if necessary.
	if err := updateMetadataTables(conn, dbName); err != nil {
		return err
	}
	log.Infof("upserting metadata...%d", len(localMetadata))
	// Populate local_metadata from the passed list of values.
	return upsertLocalMetadata(conn, localMetadata, dbName)
}

// UpsertLocalMetadata adds the given metadata map to the _vt.local_metadata
// table, updating any rows that exist for a given `_vt.local_metadata.name`
// with the map value. The session that performs these upserts sets
// sql_log_bin=0, as the _vt.local_metadata table is meant to never be
// replicated.
//
// Callers are responsible for ensuring the _vt.local_metadata table exists
// before calling this function, usually by calling CreateMetadataTables at
// least once prior.
func (m *MetadataManager) UpsertLocalMetadata(mysqld MysqlDaemon, localMetadata map[string]string, dbName string) error {
	log.Infof("Upserting _vt.local_metadata ...")

	conn, err := mysqld.GetDbaConnection(context.TODO())
	if err != nil {
		return err
	}
	defer conn.Close()

	// Disable replication on this session. We close the connection after using
	// it, so there's no need to re-enable replication when we're done.
	if _, err := conn.ExecuteFetch("SET @@session.sql_log_bin = 0", 0, false); err != nil {
		return err
	}

	return upsertLocalMetadata(conn, localMetadata, dbName)
}

func updateMetadataTables(conn *dbconnpool.DBConnection, dbName string) error {
	if err := updateLocalMetadataTable(conn, dbName); err != nil {
		return err
	}

	if err := updateShardMetadataTable(conn, dbName); err != nil {
		return err
	}

	return nil
}

func updateLocalMetadataTable(conn *dbconnpool.DBConnection, dbName string) error {
	sql := fmt.Sprintf(sqlUpdateLocalMetadataTable, dbName)
	if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
		log.Errorf("Error executing %v: %v, continuing. Please check the data in _vt.local_metadata and take corrective action.", sql, err)
	}

	return nil
}

func updateShardMetadataTable(conn *dbconnpool.DBConnection, dbName string) error {
	sql := fmt.Sprintf(sqlUpdateShardMetadataTable, dbName)
	if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
		log.Errorf("Error executing %v: %v, continuing. Please check the data in _vt.shard_metadata and take corrective action.", sql, err)
	}

	return nil
}

// upsertLocalMetadata adds the given metadata map to the _vt.local_metadata
// table, updating any rows that exist for a given `_vt.local_metadata.name`
// with the map value. The session that performs these upserts sets
// sql_log_bin=0, as the _vt.local_metadata table is meant to never be
// replicated.
//
// Callers are responsible for ensuring the _vt.local_metadata table exists
// before calling this function, usually by calling CreateMetadataTables at
// least once prior.
func upsertLocalMetadata(conn *dbconnpool.DBConnection, localMetadata map[string]string, dbName string) error {
	// Populate local_metadata from the passed list of values.
	if _, err := conn.ExecuteFetch("BEGIN", 0, false); err != nil {
		return err
	}
	for name, val := range localMetadata {
		nameValue := sqltypes.NewVarChar(name)
		valValue := sqltypes.NewVarChar(val)
		dbNameValue := sqltypes.NewVarBinary(dbName)

		queryBuf := bytes.Buffer{}
		queryBuf.WriteString("INSERT INTO _vt.local_metadata (name,value, db_name) VALUES (")
		nameValue.EncodeSQL(&queryBuf)
		queryBuf.WriteByte(',')
		valValue.EncodeSQL(&queryBuf)
		queryBuf.WriteByte(',')
		dbNameValue.EncodeSQL(&queryBuf)
		queryBuf.WriteString(") ON DUPLICATE KEY UPDATE value = ")
		valValue.EncodeSQL(&queryBuf)

		if _, err := conn.ExecuteFetch(queryBuf.String(), 0, false); err != nil {
			return err
		}
	}

	if _, err := conn.ExecuteFetch("COMMIT", 0, false); err != nil {
		return err
	}

	return nil
}

// InitTabletMetadata creates and fills the _vt.local_metadata table and
// creates the _vt.shard_metadata table.
//
// _vt.local_metadata table is a per-tablet table that is never replicated.
// This allows queries against local_metadata to return different values on
// different tablets, which is used for communicating between Vitess and
// MySQL-level tools like Orchestrator (https://github.com/openark/orchestrator).
//
// _vt.shard_metadata is a replicated table with per-shard information, but it's
// created here to make it easier to create it on databases that were running
// old version of Vitess, or databases that are getting converted to run under
// Vitess.
//
// This function is semantically equivalent to calling updateMetadataTables
// followed immediately by upsertLocalMetadata.
func InitTabletMetadata() error {
	f1 := func(conn *mysql.Conn) error {
		if _, err := conn.ExecuteFetch("CREATE DATABASE IF NOT EXISTS _vt", 0, false); err != nil {
			log.Errorf("Error executing %v: %v", "CREATE DATABASE IF NOT EXISTS _vt", err)
			return err
		}

		if _, err := conn.ExecuteFetch(sqlCreateLocalMetadataTable, 0, false); err != nil {
			log.Errorf("Error executing %v: %v", sqlCreateLocalMetadataTable, err)
			return err
		}

		for _, sql := range sqlAlterLocalMetadataTable {
			if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
				// Ignore "Duplicate column name 'db_name'" errors which can happen on every restart.
				if merr, ok := err.(*mysql.SQLError); !ok || merr.Num != mysql.ERDupFieldName {
					log.Errorf("Error executing %v: %v", sql, err)
					return err
				}
			}
		}

		if _, err := conn.ExecuteFetch(sqlCreateShardMetadataTable, 0, false); err != nil {
			log.Errorf("Error executing %v: %v", sqlCreateShardMetadataTable, err)
			return err
		}

		for _, sql := range sqlAlterShardMetadataTable {
			if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
				// Ignore "Duplicate column name 'db_name'" errors which can happen on every restart.
				if merr, ok := err.(*mysql.SQLError); !ok || merr.Num != mysql.ERDupFieldName {
					log.Errorf("Error executing %v: %v", sql, err)
					return err
				}
			}
		}

		return nil
	}
	mysql.SchemaInitializer.RegisterSchemaInitializer("Initial TM Schema", f1, false, true)
	return nil
}

func init() {
	InitTabletMetadata()
}
