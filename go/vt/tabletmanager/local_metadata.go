// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"strings"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

const sqlCreateLocalMetadataTable = `CREATE TABLE _vt.local_metadata (
  name VARCHAR(255) NOT NULL,
  value VARCHAR(255) NOT NULL,
  PRIMARY KEY (name)
  ) ENGINE=InnoDB`

// populateLocalMetadata creates and fills the _vt.local_metadata table,
// which is a per-tablet table that is never replicated. This allows queries
// against local_metadata to return different values on different tablets,
// which is used for communicating between Vitess and MySQL-level tools like
// Orchestrator (http://github.com/outbrain/orchestrator).
func (agent *ActionAgent) populateLocalMetadata(ctx context.Context) error {
	log.Infof("Populating _vt.local_metadata table...")

	// Wait for mysqld to be ready, in case it was launched in parallel with us.
	if err := agent.MysqlDaemon.Wait(ctx); err != nil {
		return err
	}

	// Get a non-pooled DBA connection.
	conn, err := agent.MysqlDaemon.GetDbaConnection()
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
	if _, err := conn.ExecuteFetch("DROP TABLE IF EXISTS _vt.local_metadata", 0, false); err != nil {
		return err
	}
	if _, err := conn.ExecuteFetch(sqlCreateLocalMetadataTable, 0, false); err != nil {
		return err
	}

	// Insert values.
	tablet := agent.Tablet()
	values := map[string]string{
		"Alias":         topoproto.TabletAliasString(tablet.Alias),
		"ClusterAlias":  fmt.Sprintf("%s.%s", tablet.Keyspace, tablet.Shard),
		"DataCenter":    tablet.Alias.Cell,
		"PromotionRule": "neutral",
	}
	masterEligible, err := agent.isMasterEligible()
	if err != nil {
		return fmt.Errorf("can't determine PromotionRule while populating local_metadata: %v", err)
	}
	if !masterEligible {
		values["PromotionRule"] = "must_not"
	}
	var rows []string
	for k, v := range values {
		rows = append(rows, fmt.Sprintf("('%s','%s')", k, v))
	}
	query := fmt.Sprintf(
		"INSERT INTO _vt.local_metadata (name,value) VALUES %s",
		strings.Join(rows, ","))
	_, err = conn.ExecuteFetch(query, 0, false)
	return err
}
