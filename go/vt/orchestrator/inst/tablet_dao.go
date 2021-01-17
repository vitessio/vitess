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

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

// TopoServ is the connection to the topo server.
var TopoServ *topo.Server

// ErrTabletAliasNil is a fixed error message.
var ErrTabletAliasNil = errors.New("tablet alias is nil")

// SwitchMaster makes the new tablet the master and proactively performs
// the necessary propagation to the old master. The propagation is best
// effort. If it fails, the tablet's shard sync will eventually converge.
// The proactive propagation allows a competing Orchestrator from discovering
// the successful action of a previous one, which reduces churn.
func SwitchMaster(newMasterKey, oldMasterKey InstanceKey) error {
	newMasterTablet, err := ChangeTabletType(newMasterKey, topodatapb.TabletType_MASTER)
	if err != nil {
		return err
	}
	// The following operations are best effort.
	if newMasterTablet.Type != topodatapb.TabletType_MASTER {
		log.Errorf("Unexpected: tablet type did not change to master: %v", newMasterTablet.Type)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
	defer cancel()
	_, err = TopoServ.UpdateShardFields(ctx, newMasterTablet.Keyspace, newMasterTablet.Shard, func(si *topo.ShardInfo) error {
		if proto.Equal(si.MasterAlias, newMasterTablet.Alias) && proto.Equal(si.MasterTermStartTime, newMasterTablet.MasterTermStartTime) {
			return topo.NewError(topo.NoUpdateNeeded, "")
		}

		// We just successfully reparented. We should check timestamps, but always overwrite.
		lastTerm := si.GetMasterTermStartTime()
		newTerm := logutil.ProtoToTime(newMasterTablet.MasterTermStartTime)
		if !newTerm.After(lastTerm) {
			log.Errorf("Possible clock skew. New master start time is before previous one: %v vs %v", newTerm, lastTerm)
		}

		aliasStr := topoproto.TabletAliasString(newMasterTablet.Alias)
		log.Infof("Updating shard record: master_alias=%v, master_term_start_time=%v", aliasStr, newTerm)
		si.MasterAlias = newMasterTablet.Alias
		si.MasterTermStartTime = newMasterTablet.MasterTermStartTime
		return nil
	})
	// Don't proceed if shard record could not be updated.
	if err != nil {
		log.Errore(err)
		return nil
	}
	if _, err := ChangeTabletType(oldMasterKey, topodatapb.TabletType_REPLICA); err != nil {
		// This is best effort.
		log.Errore(err)
	}
	return nil
}

// ChangeTabletType designates the tablet that owns an instance as the master.
func ChangeTabletType(instanceKey InstanceKey, tabletType topodatapb.TabletType) (*topodatapb.Tablet, error) {
	if instanceKey.Hostname == "" {
		return nil, errors.New("can't set tablet to master: instance is unspecified")
	}
	tablet, err := ReadTablet(instanceKey)
	if err != nil {
		return nil, err
	}
	tmc := tmclient.NewTabletManagerClient()
	tmcCtx, tmcCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
	defer tmcCancel()
	if err := tmc.ChangeType(tmcCtx, tablet, tabletType); err != nil {
		return nil, err
	}
	tsCtx, tsCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
	defer tsCancel()
	ti, err := TopoServ.GetTablet(tsCtx, tablet.Alias)
	if err != nil {
		return nil, log.Errore(err)
	}
	if err := SaveTablet(ti.Tablet); err != nil {
		log.Errore(err)
	}
	return ti.Tablet, nil
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
	err := db.QueryOrchestrator(query, args, func(row sqlutils.RowMap) error {
		return proto.UnmarshalText(row.GetString("info"), tablet)
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
	_, err := db.ExecOrchestrator(`
		replace
			into vitess_tablet (
				hostname, port, cell, keyspace, shard, tablet_type, master_timestamp, info
			) values (
				?, ?, ?, ?, ?, ?, ?, ?
			)
		`,
		tablet.MysqlHostname,
		int(tablet.MysqlPort),
		tablet.Alias.Cell,
		tablet.Keyspace,
		tablet.Shard,
		int(tablet.Type),
		logutil.ProtoToTime(tablet.MasterTermStartTime),
		proto.CompactTextString(tablet),
	)
	return err
}
