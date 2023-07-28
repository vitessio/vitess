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

	"vitess.io/vitess/go/vt/external/golib/sqlutils"

	"vitess.io/vitess/go/vt/logutil"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

// ErrTabletAliasNil is a fixed error message.
var ErrTabletAliasNil = errors.New("tablet alias is nil")

// ResetReplicationParameters resets the replication parameters on the given tablet.
func ResetReplicationParameters(tabletAlias string) error {
	tablet, err := ReadTablet(tabletAlias)
	if err != nil {
		return err
	}
	tmc := tmclient.NewTabletManagerClient()
	tmcCtx, tmcCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer tmcCancel()
	if err := tmc.ResetReplicationParameters(tmcCtx, tablet); err != nil {
		return err
	}
	return nil
}

// FullStatus gets the full status of the MySQL running in vttablet.
func FullStatus(tabletAlias string) (*replicationdatapb.FullStatus, error) {
	tablet, err := ReadTablet(tabletAlias)
	if err != nil {
		return nil, err
	}
	tmc := tmclient.NewTabletManagerClient()
	tmcCtx, tmcCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tmc.FullStatus(tmcCtx, tablet)
}

// ReadTablet reads the vitess tablet record.
func ReadTablet(tabletAlias string) (*topodatapb.Tablet, error) {
	query := `
		select
			info
		from
			vitess_tablet
		where alias = ?
		`
	args := sqlutils.Args(tabletAlias)
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

// SaveTablet saves the tablet record against the instanceKey.
func SaveTablet(tablet *topodatapb.Tablet) error {
	tabletp, err := prototext.Marshal(tablet)
	if err != nil {
		return err
	}
	_, err = db.ExecVTOrc(`
		replace
			into vitess_tablet (
				alias, hostname, port, cell, keyspace, shard, tablet_type, primary_timestamp, info
			) values (
				?, ?, ?, ?, ?, ?, ?, ?, ?
			)
		`,
		topoproto.TabletAliasString(tablet.Alias),
		tablet.MysqlHostname,
		int(tablet.MysqlPort),
		tablet.Alias.Cell,
		tablet.Keyspace,
		tablet.Shard,
		int(tablet.Type),
		logutil.ProtoToTime(tablet.PrimaryTermStartTime),
		tabletp,
	)
	return err
}
