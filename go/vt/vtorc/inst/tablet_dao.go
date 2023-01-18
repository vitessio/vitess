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

	"vitess.io/vitess/go/vt/log"

	"google.golang.org/protobuf/encoding/prototext"

	"google.golang.org/protobuf/proto"

	"github.com/openark/golib/sqlutils"

	"vitess.io/vitess/go/vt/logutil"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

// TopoServ is the connection to the topo server.
var TopoServ *topo.Server

// ErrTabletAliasNil is a fixed error message.
var ErrTabletAliasNil = errors.New("tablet alias is nil")

// SwitchPrimary makes the new tablet the primary and proactively performs
// the necessary propagation to the old primary. The propagation is best
// effort. If it fails, the tablet's shard sync will eventually converge.
// The proactive propagation allows a competing VTOrc from discovering
// the successful action of a previous one, which reduces churn.
func SwitchPrimary(newPrimaryKey, oldPrimaryKey InstanceKey) error {
	durability, err := GetDurabilityPolicy(newPrimaryKey)
	if err != nil {
		return err
	}
	newPrimaryTablet, err := ChangeTabletType(newPrimaryKey, topodatapb.TabletType_PRIMARY, SemiSyncAckers(durability, newPrimaryKey) > 0)
	if err != nil {
		return err
	}
	// The following operations are best effort.
	if newPrimaryTablet.Type != topodatapb.TabletType_PRIMARY {
		log.Errorf("Unexpected: tablet type did not change to primary: %v", newPrimaryTablet.Type)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()
	_, err = TopoServ.UpdateShardFields(ctx, newPrimaryTablet.Keyspace, newPrimaryTablet.Shard, func(si *topo.ShardInfo) error {
		if proto.Equal(si.PrimaryAlias, newPrimaryTablet.Alias) && proto.Equal(si.PrimaryTermStartTime, newPrimaryTablet.PrimaryTermStartTime) {
			return topo.NewError(topo.NoUpdateNeeded, "")
		}

		// We just successfully reparented. We should check timestamps, but always overwrite.
		lastTerm := si.GetPrimaryTermStartTime()
		newTerm := logutil.ProtoToTime(newPrimaryTablet.PrimaryTermStartTime)
		if !newTerm.After(lastTerm) {
			log.Errorf("Possible clock skew. New primary start time is before previous one: %v vs %v", newTerm, lastTerm)
		}

		aliasStr := topoproto.TabletAliasString(newPrimaryTablet.Alias)
		log.Infof("Updating shard record: primary_alias=%v, primary_term_start_time=%v", aliasStr, newTerm)
		si.PrimaryAlias = newPrimaryTablet.Alias
		si.PrimaryTermStartTime = newPrimaryTablet.PrimaryTermStartTime
		return nil
	})
	// Don't proceed if shard record could not be updated.
	if err != nil {
		log.Error(err)
		return nil
	}
	if _, err := ChangeTabletType(oldPrimaryKey, topodatapb.TabletType_REPLICA, IsReplicaSemiSync(durability, newPrimaryKey, oldPrimaryKey)); err != nil {
		// This is best effort.
		log.Error(err)
	}
	return nil
}

// ChangeTabletType designates the tablet that owns an instance as the primary.
func ChangeTabletType(instanceKey InstanceKey, tabletType topodatapb.TabletType, semiSync bool) (*topodatapb.Tablet, error) {
	if instanceKey.Hostname == "" {
		return nil, errors.New("can't set tablet to primary: instance is unspecified")
	}
	tablet, err := ReadTablet(instanceKey)
	if err != nil {
		return nil, err
	}
	tmc := tmclient.NewTabletManagerClient()
	tmcCtx, tmcCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer tmcCancel()
	if err := tmc.ChangeType(tmcCtx, tablet, tabletType, semiSync); err != nil {
		return nil, err
	}
	tsCtx, tsCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer tsCancel()
	ti, err := TopoServ.GetTablet(tsCtx, tablet.Alias)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if err := SaveTablet(ti.Tablet); err != nil {
		log.Error(err)
	}
	return ti.Tablet, nil
}

// ResetReplicationParameters resets the replication parameters on the given tablet.
func ResetReplicationParameters(instanceKey InstanceKey) error {
	tablet, err := ReadTablet(instanceKey)
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
func FullStatus(instanceKey InstanceKey) (*replicationdatapb.FullStatus, error) {
	tablet, err := ReadTablet(instanceKey)
	if err != nil {
		return nil, err
	}
	tmc := tmclient.NewTabletManagerClient()
	tmcCtx, tmcCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tmc.FullStatus(tmcCtx, tablet)
}

// ReadTablet reads the vitess tablet record.
func ReadTablet(instanceKey InstanceKey) (*topodatapb.Tablet, error) {
	query := `
		select
			info
		from
			vitess_tablet
		where hostname=? and port=?
		`
	args := sqlutils.Args(instanceKey.Hostname, instanceKey.Port)
	tablet := &topodatapb.Tablet{}
	err := db.QueryVTOrc(query, args, func(row sqlutils.RowMap) error {
		return prototext.Unmarshal([]byte(row.GetString("info")), tablet)
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
