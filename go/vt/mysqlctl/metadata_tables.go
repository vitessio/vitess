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

import (
	"bytes"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sqltypes"
)

// Note that definitions of local_metadata and shard_metadata should be the same
// as in testing which is defined in config/init_db.sql.
const sqlCreateLocalMetadataTable = `CREATE TABLE IF NOT EXISTS _vt.local_metadata (
  name VARCHAR(255) NOT NULL,
  value VARCHAR(255) NOT NULL,
  PRIMARY KEY (name)
  ) ENGINE=InnoDB`
const sqlCreateShardMetadataTable = `CREATE TABLE IF NOT EXISTS _vt.shard_metadata (
  name VARCHAR(255) NOT NULL,
  value MEDIUMBLOB NOT NULL,
  PRIMARY KEY (name)
  ) ENGINE=InnoDB`

// populateMetadataTables creates and fills the _vt.local_metadata table and
// creates _vt.shard_metadata table. _vt.local_metadata table is
// a per-tablet table that is never replicated. This allows queries
// against local_metadata to return different values on different tablets,
// which is used for communicating between Vitess and MySQL-level tools like
// Orchestrator (http://github.com/outbrain/orchestrator).
// _vt.shard_metadata is a replicated table with per-shard information, but it's
// created here to make it easier to create it on databases that were running
// old version of Vitess, or databases that are getting converted to run under
// Vitess.
func populateMetadataTables(mysqld MysqlDaemon, localMetadata map[string]string) error {
	log.Infof("Populating _vt.local_metadata table...")

	// Get a non-pooled DBA connection.
	conn, err := mysqld.GetDbaConnection()
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
	if _, err := conn.ExecuteFetch("CREATE DATABASE IF NOT EXISTS _vt", 0, false); err != nil {
		return err
	}
	if _, err := conn.ExecuteFetch(sqlCreateLocalMetadataTable, 0, false); err != nil {
		return err
	}
	if _, err := conn.ExecuteFetch(sqlCreateShardMetadataTable, 0, false); err != nil {
		return err
	}

	// Populate local_metadata from the passed list of values.
	if _, err := conn.ExecuteFetch("BEGIN", 0, false); err != nil {
		return err
	}
	for name, val := range localMetadata {
		nameValue, err := sqltypes.BuildValue(name)
		if err != nil {
			return err
		}
		valValue, err := sqltypes.BuildValue(val)
		if err != nil {
			return err
		}

		queryBuf := bytes.Buffer{}
		queryBuf.WriteString("INSERT INTO _vt.local_metadata (name,value) VALUES (")
		nameValue.EncodeSQL(&queryBuf)
		queryBuf.WriteByte(',')
		valValue.EncodeSQL(&queryBuf)
		queryBuf.WriteString(") ON DUPLICATE KEY UPDATE value = ")
		valValue.EncodeSQL(&queryBuf)

		if _, err := conn.ExecuteFetch(queryBuf.String(), 0, false); err != nil {
			return err
		}
	}
	_, err = conn.ExecuteFetch("COMMIT", 0, false)
	return err
}
