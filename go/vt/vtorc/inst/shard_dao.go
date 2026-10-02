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
	"strings"
	"time"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
func ReadShardPrimaryInformation(keyspaceName, shardName string) (
	primaryAlias *topodatapb.TabletAlias,
	primaryTimestamp time.Time,
	err error,
) {
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
			keyspace = ?
			AND shard = ?`
	args := sqlutils.Args(keyspaceName, shardName)
	shardFound := false
	err = db.QueryVTOrc(query, args, func(row sqlutils.RowMap) (err error) {
		shardFound = true
		if primaryAliasStr := row.GetString("primary_alias"); primaryAliasStr != "" {
			primaryAlias, err = topoproto.ParseTabletAlias(primaryAliasStr)
			if err != nil {
				return err
			}
		}
		primaryTimestamp = row.GetTime("primary_timestamp")
		return nil
	})
	if err != nil {
		return
	}
	if !shardFound {
		err = ErrShardNotFound
	}
	return primaryAlias, primaryTimestamp, err
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
                MIN(vk.disable_emergency_reparent) AS ks_ers_disabled,
                MIN(vs.disable_emergency_reparent) AS shard_ers_disabled
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
			DisableEmergencyReparent: row.GetBool("ks_ers_disabled") || row.GetBool("shard_ers_disabled"),
		})
		return nil
	})
	return ksShardStats, err
}

// SaveShard saves the shard record against the shard name.
func SaveShard(shard *topo.ShardInfo) error {
	var disableEmergencyReparent int
	if shard.VtorcState != nil && shard.VtorcState.DisableEmergencyReparent {
		disableEmergencyReparent = 1
	}
	_, err := db.ExecVTOrc(`
		replace	into vitess_shard (
			keyspace, shard, primary_alias, primary_timestamp, disable_emergency_reparent, group_replication_voters,
			group_replication_incarnation, group_replication_bootstrap_target
		) values (
			?, ?, ?, ?, ?, ?, ?, ?
		)`,
		shard.Keyspace(),
		shard.ShardName(),
		getShardPrimaryAliasString(shard),
		getShardPrimaryTermStartTime(shard),
		disableEmergencyReparent,
		formatGroupReplicationVoters(shard.GroupReplicationVoters),
		shard.GroupReplicationIncarnation,
		groupReplicationBootstrapTarget(shard.Shard),
	)
	return err
}

// groupReplicationBootstrapTarget returns the alias of the target of the shard's bootstrap intent,
// if the intent still applies to the incarnation the shard record lists, else "".
func groupReplicationBootstrapTarget(shard *topodatapb.Shard) string {
	intent := shard.GetGroupReplicationBootstrapIntent()
	if intent == nil || intent.GetTarget() == nil || intent.GetPreviousIncarnation() != shard.GetGroupReplicationIncarnation() {
		return ""
	}
	return topoproto.TabletAliasString(intent.GetTarget())
}

// ReadShardGroupReplicationVoters reads the voting members of the shard's replication group, as
// recorded in the shard record.
func ReadShardGroupReplicationVoters(keyspaceName, shardName string) ([]*topodatapb.TabletAlias, error) {
	query := `SELECT
			group_replication_voters
		FROM
			vitess_shard
		WHERE
			keyspace = ?
			AND shard = ?`
	var voters []*topodatapb.TabletAlias
	shardFound := false
	err := db.QueryVTOrc(query, sqlutils.Args(keyspaceName, shardName), func(row sqlutils.RowMap) (err error) {
		shardFound = true
		voters, err = parseGroupReplicationVoters(row.GetString("group_replication_voters"))
		return err
	})
	if err != nil {
		return nil, err
	}
	if !shardFound {
		return nil, ErrShardNotFound
	}
	return voters, nil
}

// ReadShardGroupReplicationIncarnation reads the incarnation of the shard's replication group, as
// recorded in the shard record.
func ReadShardGroupReplicationIncarnation(keyspaceName, shardName string) (string, error) {
	query := `SELECT
			group_replication_incarnation
		FROM
			vitess_shard
		WHERE
			keyspace = ?
			AND shard = ?`
	incarnation := ""
	shardFound := false
	err := db.QueryVTOrc(query, sqlutils.Args(keyspaceName, shardName), func(row sqlutils.RowMap) error {
		shardFound = true
		incarnation = row.GetString("group_replication_incarnation")
		return nil
	})
	if err != nil {
		return "", err
	}
	if !shardFound {
		return "", ErrShardNotFound
	}
	return incarnation, nil
}

// formatGroupReplicationVoters formats the voting members of a shard's replication group to be
// stored in the database.
func formatGroupReplicationVoters(voters []*topodatapb.TabletAlias) string {
	aliases := make([]string, 0, len(voters))
	for _, voter := range voters {
		aliases = append(aliases, topoproto.TabletAliasString(voter))
	}
	return strings.Join(aliases, ",")
}

// parseGroupReplicationVoters parses the voting members of a shard's replication group as they are
// stored in the database.
func parseGroupReplicationVoters(value string) ([]*topodatapb.TabletAlias, error) {
	if value == "" {
		return nil, nil
	}
	var voters []*topodatapb.TabletAlias
	for aliasString := range strings.SplitSeq(value, ",") {
		alias, err := topoproto.ParseTabletAlias(aliasString)
		if err != nil {
			return nil, err
		}
		voters = append(voters, alias)
	}
	return voters, nil
}

// getShardPrimaryAliasString gets the shard primary alias to be stored as a string in the database.
func getShardPrimaryAliasString(shard *topo.ShardInfo) string {
	if shard.PrimaryAlias == nil {
		return ""
	}
	return topoproto.TabletAliasString(shard.PrimaryAlias)
}

// getShardPrimaryTermStartTime gets the shard primary term start time to be stored as a string in the database.
func getShardPrimaryTermStartTime(shard *topo.ShardInfo) time.Time {
	if shard.PrimaryTermStartTime == nil {
		return time.Time{}
	}
	return protoutil.TimeFromProto(shard.PrimaryTermStartTime).UTC()
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
