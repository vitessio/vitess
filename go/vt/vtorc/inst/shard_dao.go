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

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// ErrShardNotFound is a fixed error message used when a shard is not found in the database.
var ErrShardNotFound = errors.New("shard not found")

// ReadShardNames reads the names of vitess shards for a single keyspace.
func ReadShardNames(keyspaceName string) (shardNames []string, err error) {
	shardNames = make([]string, 0)
	query := `select shard from vitess_shard where keyspace = ?`
	args := sqlutils.Args(keyspaceName)
	err = db.QueryVTOrc(query, args, func(row sqlutils.RowMap) error {
		shardNames = append(shardNames, row.GetString("shard"))
		return nil
	})
	return shardNames, err
}

// ReadShardPrimaryInformation reads the vitess shard record and gets the shard primary alias and timestamp.
func ReadShardPrimaryInformation(keyspaceName, shardName string) (primaryAlias string, primaryTimestamp string, err error) {
	if err = topo.ValidateKeyspaceName(keyspaceName); err != nil {
		return
	}
	if _, _, err = topo.ValidateShardName(shardName); err != nil {
		return
	}

	query := `
		select
			primary_alias, primary_timestamp
		from
			vitess_shard
		where keyspace=? and shard=?
		`
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

// ShardStats represents stats for a single shard watched by VTOrc.
type ShardStats struct {
	Keyspace                 string
	Shard                    string
	DisableEmergencyReparent bool
	TabletCount              int64
}

// ReadKeyspaceShardStats returns stats such as # of tablets watched by keyspace/shard and ERS-disabled state.
// The backend query uses an index by "keyspace, shard": ks_idx_vitess_tablet.
func ReadKeyspaceShardStats() ([]ShardStats, error) {
	ksShardStats := make([]ShardStats, 0)
	query := `SELECT
                vt.keyspace AS keyspace,
                vt.shard AS shard,
                COUNT() AS tablet_count,
                vk.disable_emergency_reparent AS ksERSDisabled,
                vs.disable_emergency_reparent AS shardERSDisabled
        FROM
                vitess_tablet vt
        LEFT JOIN
                vitess_keyspace vk
                ON
                vk.keyspace = vt.keyspace
        LEFT JOIN
                vitess_shard vs
                ON
                (vs.keyspace = vt.keyspace AND vs.shard = vt.shard)
        GROUP BY
                vt.keyspace,
                vt.shard`
	err := db.QueryVTOrc(query, nil, func(row sqlutils.RowMap) error {
		ksShardStats = append(ksShardStats, ShardStats{
			Keyspace:                 row.GetString("keyspace"),
			Shard:                    row.GetString("shard"),
			TabletCount:              row.GetInt64("tablet_count"),
			DisableEmergencyReparent: row.GetBool("ksERSDisabled") || row.GetBool("shardERSDisabled"),
		})
		return nil
	})
	return ksShardStats, err
}

// SaveShard saves the shard record against the shard name.
func SaveShard(shard *topo.ShardInfo) error {
	var disableEmergencyReparent int
	if shard.VtorcConfig != nil && shard.VtorcConfig.DisableEmergencyReparent {
		disableEmergencyReparent = 1
	}
	_, err := db.ExecVTOrc(`
		replace	into vitess_shard (
			keyspace, shard, primary_alias, primary_timestamp, disable_emergency_reparent
		) values (
			?, ?, ?, ?, ?
		)`,
		shard.Keyspace(),
		shard.ShardName(),
		getShardPrimaryAliasString(shard),
		getShardPrimaryTermStartTimeString(shard),
		disableEmergencyReparent,
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

// DeleteShard deletes a shard using a keyspace and shard name.
func DeleteShard(keyspace, shard string) error {
	_, err := db.ExecVTOrc(`DELETE FROM
			vitess_shard
		WHERE
			keyspace = ?
			AND shard = ?`,
		keyspace,
		shard,
	)
	return err
}
