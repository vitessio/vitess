/*
Copyright 2020 The Vitess Authors.

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
	"context"
	"errors"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

// ErrTabletAliasNil is a fixed error message.
var (
	ErrTabletAliasNil = errors.New("tablet alias is nil")
	tmc               tmclient.TabletManagerClient
)

// InitializeTMC initializes the tablet manager client to use for all VTOrc RPC calls.
func InitializeTMC() tmclient.TabletManagerClient {
	tmc = tmclient.NewTabletManagerClient()
	return tmc
}

// fullStatus gets the full status of the MySQL running in vttablet.
func fullStatus(tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	tmcCtx, tmcCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tmc.FullStatus(tmcCtx, tablet)
}

// ReadTablet reads the vitess tablet record.
func ReadTablet(tabletAlias *topodatapb.TabletAlias) (*topodatapb.Tablet, error) {
	query := `SELECT
		info
	FROM
		vitess_tablet
	WHERE
		alias = ?`
	args := sqlutils.Args(topoproto.TabletAliasString(tabletAlias))
	tablet := &topodatapb.Tablet{}
	opts := prototext.UnmarshalOptions{DiscardUnknown: true}
	err := db.QueryVTOrc(query, args, func(row sqlutils.RowMap) error {
		return opts.Unmarshal([]byte(row.GetString("info")), tablet)
	})
	if err != nil {
		return nil, err
	}
	if tablet.Alias == nil {
		return nil, ErrTabletAliasNil
	}
	return tablet, nil
}

// ReadTabletCountsByCell returns the count of tablets watched by cell.
// The backend query uses an index by "cell": cell_idx_vitess_tablet.
func ReadTabletCountsByCell() (map[string]int64, error) {
	tabletCounts := make(map[string]int64)
	query := `SELECT
		cell,
		COUNT() AS count
	FROM
		vitess_tablet
	GROUP BY
		cell`
	err := db.QueryVTOrc(query, nil, func(row sqlutils.RowMap) error {
		cell := row.GetString("cell")
		tabletCounts[cell] = row.GetInt64("count")
		return nil
	})
	return tabletCounts, err
}

// ShardEligibleObserverCount returns the number of REPLICA/RDONLY tablets in the shard from VTOrc's
// topo view (vitess_tablet) — the expected observer population the quorum majority gate uses as its
// base. It mirrors the shard_eligible_observers count the analysis query feeds the ERS matcher, so a
// caller without an analysis row (the read-only /api/shard-tablet-health-quorum endpoint) can present the same
// actionable verdict instead of falling back to the observers it happens to have seen. The
// tablet-type filter is built from shardObserverTabletTypeList so it cannot drift from
// IsShardHealthObserverType.
func ShardEligibleObserverCount(keyspace, shard string) (int, error) {
	query := `SELECT
		COUNT(*) AS observer_count
	FROM
		vitess_tablet
	WHERE
		keyspace = ?
		AND shard = ?
		AND tablet_type IN (` + shardObserverTabletTypeList() + `)`
	var count int
	err := db.QueryVTOrc(query, sqlutils.Args(keyspace, shard), func(row sqlutils.RowMap) error {
		count = row.GetInt("observer_count")
		return nil
	})
	return count, err
}

// SaveTablet saves the tablet record against the instanceKey.
func SaveTablet(tablet *topodatapb.Tablet) error {
	tabletp, err := prototext.Marshal(tablet)
	if err != nil {
		return err
	}
	_, err = db.ExecVTOrc(`REPLACE
		INTO vitess_tablet (
			alias,
			hostname,
			port,
			cell,
			keyspace,
			shard,
			tablet_type,
			primary_timestamp,
			info
		) VALUES (
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?
		)`,
		topoproto.TabletAliasString(tablet.Alias),
		tablet.MysqlHostname,
		int(tablet.MysqlPort),
		tablet.Alias.Cell,
		tablet.Keyspace,
		tablet.Shard,
		int(tablet.Type),
		protoutil.TimeFromProto(tablet.PrimaryTermStartTime).UTC(),
		tabletp,
	)
	return err
}
