/*
Copyright 2022 The Vitess Authors.

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

package inst

import (
	"errors"
	"time"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// ErrShardNotFound is a fixed error message used when a shard is not found in the database.
var ErrShardNotFound = errors.New("shard not found")

// ReadShardPrimaryInformation reads the vitess shard record and gets the shard primary alias and timestamp.
func ReadShardPrimaryInformation(keyspaceName, shardName string) (primaryAlias string, primaryTimestamp string, err error) {
	if err = topo.ValidateKeyspaceName(keyspaceName); err != nil {
		return
	}
	if _, _, err = topo.ValidateShardName(shardName); err != nil {
		return
	}

	query := `SELECT
			primary_alias, primary_timestamp
		FROM
			vitess_shard
		WHERE
			keyspace = ? AND shard = ?`
	args := sqlutils.Args(keyspaceName, shardName)
	shardFound := false
	err = db.QueryVTOrc(query, args, func(row sqlutils.RowMap) error {
		shardFound = true
		primaryAlias = row.GetString("primary_alias")
		primaryTimestamp = row.GetString("primary_timestamp")
		return nil
	})
	if err != nil {
		return
	}
	if !shardFound {
		return "", "", ErrShardNotFound
	}
	return primaryAlias, primaryTimestamp, nil
}

// GetAllShardNames returns the names of all keyspace/shards.
func GetAllShardNames() (map[string][]string, error) {
	shards := make(map[string][]string, 0)
	query := `SELECT keyspace, shard FROM vitess_shard`
	err := db.QueryVTOrc(query, nil, func(row sqlutils.RowMap) error {
		keyspace := row.GetString("keyspace")
		shards[keyspace] = append(shards[keyspace], row.GetString("shard"))
		return nil
	})
	return shards, err
}

// GetKeyspaceShardNames returns the names of all shards in a keyspace.
func GetKeyspaceShardNames(keyspaceName string) ([]string, error) {
	shards := make([]string, 0)
	query := `SELECT shard FROM vitess_shard WHERE keyspace = ?`
	args := sqlutils.Args(keyspaceName)
	err := db.QueryVTOrc(query, args, func(row sqlutils.RowMap) error {
		shards = append(shards, row.GetString("shard"))
		return nil
	})
	return shards, err
}

// SaveShard saves the shard record against the shard name.
func SaveShard(shard *topo.ShardInfo) error {
	_, err := db.ExecVTOrc(`
		REPLACE
			INTO vitess_shard (
				keyspace, shard, primary_alias, primary_timestamp, updated_timestamp
			) VALUES (
				?, ?, ?, ?, DATETIME('now')
			)
		`,
		shard.Keyspace(),
		shard.ShardName(),
		getShardPrimaryAliasString(shard),
		getShardPrimaryTermStartTimeString(shard),
	)
	return err
}

// DeleteStaleKeyspaceShards deletes shard records that have not been updated since a provided time.
func DeleteStaleKeyspaceShards(keyspace string, staleTime time.Time) error {
	_, err := db.ExecVTOrc(`DELETE FROM vitess_shard
		WHERE
			keyspace = ?
			AND
			updated_timestamp < DATETIME(?, 'unixepoch')
		`,
		keyspace,
		staleTime.Unix(),
	)
	return err
}

// getShardPrimaryAliasString gets the shard primary alias to be stored as a string in the database.
func getShardPrimaryAliasString(shard *topo.ShardInfo) string {
	if shard.PrimaryAlias == nil {
		return ""
	}
	return topoproto.TabletAliasString(shard.PrimaryAlias)
}

// getShardPrimaryAliasString gets the shard primary term start time to be stored as a string in the database.
func getShardPrimaryTermStartTimeString(shard *topo.ShardInfo) string {
	if shard.PrimaryTermStartTime == nil {
		return ""
	}
	return protoutil.TimeFromProto(shard.PrimaryTermStartTime).UTC().String()
}
