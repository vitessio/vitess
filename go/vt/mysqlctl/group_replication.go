/*
Copyright 2026 The Vitess Authors.

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
	"context"
	"fmt"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// GroupReplicationStatus returns the MySQL Group Replication state of the server.
func (mysqld *Mysqld) GroupReplicationStatus(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return conn.Conn.GroupReplicationStatus()
}

// ConfigureGroupReplication installs the Group Replication plugin if it is not loaded yet
// and applies cfg. The member must not be part of a group: MySQL rejects changes to most of
// these variables while Group Replication runs.
func (mysqld *Mysqld) ConfigureGroupReplication(ctx context.Context, cfg mysql.GroupReplicationConfig) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	status, err := conn.Conn.GroupReplicationStatus()
	if err != nil {
		return err
	}
	if mysql.IsGroupMemberActive(status) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot configure group replication while the member is %s", status.MemberState)
	}

	var cmds []string
	if !status.PluginActive {
		// INSTALL PLUGIN writes to mysql.plugin, which super_read_only rejects. The statement
		// is not written to the binary log, so lifting super_read_only for it does not create
		// errant transactions.
		superReadOnly, err := mysqld.IsSuperReadOnly(ctx)
		if err != nil {
			return err
		}
		if superReadOnly {
			cmds = append(cmds, "SET GLOBAL super_read_only = OFF")
		}
		cmds = append(cmds, mysql.InstallGroupReplicationPluginCommand())
		if superReadOnly {
			cmds = append(cmds, "SET GLOBAL super_read_only = ON")
		}
	}
	cmds = append(cmds, mysql.ConfigureGroupReplicationCommands(cfg)...)
	log.Info(fmt.Sprintf("Configuring group replication: group %s, local address %s, seeds %v", cfg.GroupName, cfg.LocalAddress, cfg.Seeds))
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// StartGroupReplication makes the member join its group. With bootstrap set, the member
// creates a new group instead. Distributed recovery authenticates as the replication user.
func (mysqld *Mysqld) StartGroupReplication(ctx context.Context, bootstrap bool) error {
	params, err := mysqld.dbcfgs.ReplConnector().MysqlParams()
	if err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := mysql.StartGroupReplicationCommands(bootstrap, params.Uname, params.Pass)
	log.Info(fmt.Sprintf("Starting group replication (bootstrap: %v)", bootstrap))
	err = mysqld.executeSuperQueryListConn(ctx, conn, cmds)
	if err != nil && bootstrap {
		// Never leave the bootstrap flag on: a later START GROUP_REPLICATION would create a
		// second group.
		if resetErr := mysqld.executeSuperQueryListConn(ctx, conn, []string{"SET GLOBAL group_replication_bootstrap_group = OFF"}); resetErr != nil {
			log.Warn(fmt.Sprintf("Failed to reset group_replication_bootstrap_group: %v", resetErr))
		}
	}
	return err
}

// StopGroupReplication makes the member leave its group. MySQL leaves the member
// super_read_only.
func (mysqld *Mysqld) StopGroupReplication(ctx context.Context) error {
	return mysqld.ExecuteSuperQueryList(ctx, []string{mysql.StopGroupReplicationCommand()})
}

// SetGroupReplicationPrimary makes the member with the given server_uuid the group's
// primary. It returns once the group has switched primaries.
func (mysqld *Mysqld) SetGroupReplicationPrimary(ctx context.Context, memberUUID string) error {
	cmd, err := mysql.SetGroupPrimaryCommand(memberUUID)
	if err != nil {
		return err
	}
	return mysqld.ExecuteSuperQueryList(ctx, []string{cmd})
}
