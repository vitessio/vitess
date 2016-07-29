// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bytes"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sqltypes"
)

const sqlCreateLocalMetadataTable = `CREATE TABLE IF NOT EXISTS _vt.local_metadata (
  name VARCHAR(255) NOT NULL,
  value VARCHAR(255) NOT NULL,
  PRIMARY KEY (name)
  ) ENGINE=InnoDB`

// populateLocalMetadata creates and fills the _vt.local_metadata table,
// which is a per-tablet table that is never replicated. This allows queries
// against local_metadata to return different values on different tablets,
// which is used for communicating between Vitess and MySQL-level tools like
// Orchestrator (http://github.com/outbrain/orchestrator).
func populateLocalMetadata(mysqld MysqlDaemon, localMetadata map[string]string) error {
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
