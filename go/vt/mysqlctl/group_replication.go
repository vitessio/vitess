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
	"slices"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// bootstrapFlagResetTimeout bounds the reset of group_replication_bootstrap_group after a failed
// bootstrap.
const bootstrapFlagResetTimeout = 10 * time.Second

// GroupReplicationStatus returns the MySQL Group Replication state of the server. The queries
// are bounded by ctx: when it expires, the connection is killed and closed, so that a query that
// the server does not answer cannot hold the caller, for example a tablet RPC holding the action
// lock, forever.
func (mysqld *Mysqld) GroupReplicationStatus(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	var status *replicationdatapb.GroupReplicationStatus
	err = mysqld.executeWithContext(ctx, conn, "group replication status", func() error {
		var queryErr error
		status, queryErr = conn.Conn.GroupReplicationStatus()
		return queryErr
	})
	if err != nil {
		return nil, err
	}
	return status, nil
}

// GroupReplicationApplierStatus returns the member's own state and how far its applier is behind
// its group, read with a single query. Like GroupReplicationStatus, it is bounded by ctx.
func (mysqld *Mysqld) GroupReplicationApplierStatus(ctx context.Context) (*mysql.GroupReplicationApplierStatus, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	var status *mysql.GroupReplicationApplierStatus
	err = mysqld.executeWithContext(ctx, conn, "group replication applier status", func() error {
		var queryErr error
		status, queryErr = conn.Conn.GroupReplicationApplierStatus()
		return queryErr
	})
	if err != nil {
		return nil, err
	}
	return status, nil
}

// GroupReplicationFenceStatus returns the member's state, view and super_read_only, read with a
// single query, and its view id if withView is set. Like GroupReplicationStatus, it is bounded by
// ctx.
func (mysqld *Mysqld) GroupReplicationFenceStatus(ctx context.Context, withView bool) (*mysql.GroupReplicationFenceStatus, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	var status *mysql.GroupReplicationFenceStatus
	err = mysqld.executeWithContext(ctx, conn, "group replication fence status", func() error {
		var queryErr error
		status, queryErr = conn.Conn.GroupReplicationFenceStatus(withView)
		return queryErr
	})
	if err != nil {
		return nil, err
	}
	return status, nil
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

	var status *replicationdatapb.GroupReplicationStatus
	err = mysqld.executeWithContext(ctx, conn, "group replication status", func() error {
		var queryErr error
		status, queryErr = conn.Conn.GroupReplicationStatus()
		return queryErr
	})
	if err != nil {
		return err
	}
	if mysql.IsGroupMemberActive(status) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot configure group replication while the member is %s", status.MemberState)
	}

	params, err := mysqld.dbcfgs.ReplConnector().MysqlParams()
	if err != nil {
		return err
	}
	if err := mysqld.checkGroupReplicationPrivileges(ctx, conn, params.Uname); err != nil {
		return err
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
	cmds = append(cmds, mysql.GroupReplicationCredentialsCommand(params.Uname, params.Pass))
	log.Info(fmt.Sprintf("Configuring group replication: group %s, local address %s, seeds %v", cfg.GroupName, cfg.LocalAddress, cfg.Seeds))
	if err := mysqld.executeSuperQueryListConn(ctx, conn, cmds); err != nil {
		return err
	}
	return mysqld.disableSuperReadOnlyActionLocally(ctx, conn)
}

// disableSuperReadOnlyActionLocally disables the member action
// mysql_disable_super_read_only_if_primary in the configuration of a member that is not in a group,
// if it is enabled there.
//
// With the action enabled, Group Replication makes the member it elects primary writable after the
// election, whatever Vitess decided: the primary of a group that the shard's tablets do not follow
// too, such as a group of one that a join formed on its own. Such a group uses the configuration of
// the member that forms it: in the lab, a joiner whose configuration was still enabled formed a
// writable group of one and took 133 commits, and with it disabled every election left the new
// primary super_read_only (sro-eval case B, MySQL 8.4.11). A member that joins a group takes the
// group's configuration, so this matters for every member that may form a group, before every
// START: fresh members, members that were out of the group when it was disabled online, and members
// whose configuration was reset.
//
// MySQL changes the configuration only with super_read_only OFF, like INSTALL PLUGIN, and does not
// write the change to the binary log; read_only stays ON meanwhile, as it does for INSTALL PLUGIN.
func (mysqld *Mysqld) disableSuperReadOnlyActionLocally(ctx context.Context, conn *dbconnpool.PooledDBConnection) error {
	var actions *mysql.GroupReplicationMemberActions
	err := mysqld.executeWithContext(ctx, conn, "group replication member actions", func() error {
		var queryErr error
		actions, queryErr = conn.Conn.GroupReplicationMemberActions()
		return queryErr
	})
	if err != nil {
		return err
	}
	if !actions.SuperReadOnlyActionEnabled {
		return nil
	}
	superReadOnly, err := mysqld.IsSuperReadOnly(ctx)
	if err != nil {
		return err
	}
	var cmds []string
	if superReadOnly {
		cmds = append(cmds, "SET GLOBAL super_read_only = OFF")
	}
	cmds = append(cmds, mysql.DisableGroupReplicationSuperReadOnlyActionCommand())
	if superReadOnly {
		cmds = append(cmds, "SET GLOBAL super_read_only = ON")
	}
	log.Info(fmt.Sprintf("Disabling the group replication member action %s in the member's own configuration (version %d)",
		mysql.GroupReplicationSuperReadOnlyAction, actions.ConfigurationVersion))
	err = mysqld.executeSuperQueryListConn(ctx, conn, cmds)
	if err != nil && superReadOnly {
		// Never leave super_read_only off on a member that had it on.
		resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bootstrapFlagResetTimeout)
		defer cancel()
		if _, resetErr := mysqld.SetSuperReadOnly(resetCtx, true); resetErr != nil {
			log.Warn(fmt.Sprintf("Failed to set super_read_only again after failing to disable %s: %v", mysql.GroupReplicationSuperReadOnlyAction, resetErr))
		}
	}
	if err != nil {
		return vterrors.Wrapf(err, "failed to disable the group replication member action %s", mysql.GroupReplicationSuperReadOnlyAction)
	}
	return nil
}

// GroupReplicationMemberActions returns the member's member actions configuration: its group's while
// it is in a group, its own otherwise.
func (mysqld *Mysqld) GroupReplicationMemberActions(ctx context.Context) (*mysql.GroupReplicationMemberActions, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	var actions *mysql.GroupReplicationMemberActions
	err = mysqld.executeWithContext(ctx, conn, "group replication member actions", func() error {
		var queryErr error
		actions, queryErr = conn.Conn.GroupReplicationMemberActions()
		return queryErr
	})
	if err != nil {
		return nil, err
	}
	return actions, nil
}

// DisableGroupReplicationSuperReadOnlyAction disables the member action
// mysql_disable_super_read_only_if_primary in the configuration of the member's group. MySQL accepts it
// only on the primary of the group, and only while super_read_only is OFF; the group's other members
// take the new configuration within about a second, and the members that are out of the group when
// they join it.
func (mysqld *Mysqld) DisableGroupReplicationSuperReadOnlyAction(ctx context.Context) error {
	return mysqld.ExecuteSuperQueryList(ctx, []string{mysql.DisableGroupReplicationSuperReadOnlyActionCommand()})
}

// checkGroupReplicationPrivileges returns a FAILED_PRECONDITION error unless an account of the
// replication user has the privileges that the MySQL communication stack needs. Without them,
// MySQL refuses the connections between members, and a join fails only after its timeout.
func (mysqld *Mysqld) checkGroupReplicationPrivileges(ctx context.Context, conn *dbconnpool.PooledDBConnection, user string) error {
	query := "SELECT HOST, PRIV FROM mysql.global_grants WHERE USER = " + sqltypes.EncodeStringSQL(user)
	qr, err := mysqld.executeFetchContext(ctx, conn, query, 10000, false)
	if err != nil {
		return vterrors.Wrapf(err, "failed to read the privileges of the replication user %s", user)
	}
	privileges := make(map[string][]string)
	for _, row := range qr.Rows {
		host := row[0].ToString()
		privileges[host] = append(privileges[host], strings.ToUpper(row[1].ToString()))
	}
	for _, granted := range privileges {
		if !slices.ContainsFunc(mysql.GroupReplicationPrivileges, func(p string) bool { return !slices.Contains(granted, p) }) {
			return nil
		}
	}
	required := strings.Join(mysql.GroupReplicationPrivileges, ", ")
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
		"the replication user %s lacks %s, which Group Replication needs to connect the members of a group; run GRANT %s ON *.* TO %s@'%%' on the shard primary",
		user, required, required, sqltypes.EncodeStringSQL(user))
}

// StartGroupReplication makes the member join its group. With bootstrap set, the member
// creates a new group instead. The member authenticates to the others as the replication user
// that ConfigureGroupReplication stored on the recovery channel.
func (mysqld *Mysqld) StartGroupReplication(ctx context.Context, bootstrap bool) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	if bootstrap {
		// MySQL accepts group_replication_bootstrap_group=ON while a START GROUP_REPLICATION runs, for
		// example one whose client gave up, and such a START could then complete as a bootstrap: a
		// second group. The caller holds the tablet's action lock, under which every START of the
		// tablet runs, and MySQL never starts one on its own (start_on_boot and auto-rejoin are off),
		// so none can begin between this check and the bootstrap.
		var starting bool
		err = mysqld.executeWithContext(ctx, conn, "group replication start in progress", func() error {
			var queryErr error
			starting, queryErr = conn.Conn.GroupReplicationStartInProgress()
			return queryErr
		})
		if err != nil {
			return err
		}
		if starting {
			return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE,
				"refusing to set group_replication_bootstrap_group while a START GROUP_REPLICATION runs, which MySQL could complete as a bootstrap: %s",
				mysql.GroupReplicationCommandRunningMessage)
		}
	}
	cmds := mysql.StartGroupReplicationCommands(bootstrap)
	log.Info(fmt.Sprintf("Starting group replication (bootstrap: %v)", bootstrap))
	err = mysqld.executeSuperQueryListConn(ctx, conn, cmds)
	if err != nil && bootstrap {
		// Never leave the bootstrap flag on: a later START GROUP_REPLICATION would create a
		// second group. Reset it on a fresh connection and context: when ctx expired, the
		// connection running START GROUP_REPLICATION was killed and ctx cannot run anything,
		// but MySQL may still complete the killed statement, or a later one, as a bootstrap.
		resetCtx, cancel := context.WithTimeout(context.Background(), bootstrapFlagResetTimeout)
		defer cancel()
		if resetErr := mysqld.ExecuteSuperQueryList(resetCtx, []string{"SET GLOBAL group_replication_bootstrap_group = OFF"}); resetErr != nil {
			log.Warn(fmt.Sprintf("Failed to reset group_replication_bootstrap_group: %v", resetErr))
		}
	}
	return err
}

// groupReplicationApplierPollInterval is how often ApplyGroupReplicationRelayLog reads the executed
// GTID set while the applier runs.
const groupReplicationApplierPollInterval = 100 * time.Millisecond

// ApplyGroupReplicationRelayLog starts the applier thread of the group_replication_applier channel
// on a member that is not in a group, which applies what the member received from its last group
// and had not applied when it left (see mysql.StartGroupReplicationApplierCommand). It waits until
// the executed GTID set contains until, or ctx ends, and stops the thread again in either case.
func (mysqld *Mysqld) ApplyGroupReplicationRelayLog(ctx context.Context, until replication.GTIDSet) error {
	if err := mysqld.ExecuteSuperQueryList(ctx, []string{mysql.StartGroupReplicationApplierCommand()}); err != nil {
		return vterrors.Wrapf(err, "failed to start the applier of the %s channel", mysql.GroupReplicationApplierChannel)
	}
	defer func() {
		// Stop the thread even if ctx ended: a later START GROUP_REPLICATION restarts the channel
		// anyway, but nothing else should apply transactions while the member is out of its group.
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bootstrapFlagResetTimeout)
		defer cancel()
		if err := mysqld.ExecuteSuperQueryList(stopCtx, []string{mysql.StopGroupReplicationApplierCommand()}); err != nil {
			log.Warn(fmt.Sprintf("Failed to stop the applier of the %s channel: %v", mysql.GroupReplicationApplierChannel, err))
		}
	}()
	for {
		pos, err := mysqld.PrimaryPosition(ctx)
		if err != nil {
			return vterrors.Wrapf(err, "failed to read the executed GTID set")
		}
		if pos.GTIDSet != nil && pos.GTIDSet.Contains(until) {
			return nil
		}
		select {
		case <-ctx.Done():
			return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "the applier of the %s channel did not apply %s in time, the executed GTID set is %v: %v",
				mysql.GroupReplicationApplierChannel, until, pos.GTIDSet, ctx.Err())
		case <-time.After(groupReplicationApplierPollInterval):
		}
	}
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

// IsOfflineMode returns whether offline_mode is ON.
func (mysqld *Mysqld) IsOfflineMode(ctx context.Context) (bool, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SELECT @@global.offline_mode")
	if err != nil {
		return false, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result reading offline_mode: %v", qr.Rows)
	}
	v := qr.Rows[0][0].ToString()
	return v == "1" || v == "ON", nil
}

// SetOfflineMode sets offline_mode. The dba user, which has CONNECTION_ADMIN, can still connect
// while it is ON.
func (mysqld *Mysqld) SetOfflineMode(ctx context.Context, on bool) error {
	value := "OFF"
	if on {
		value = "ON"
	}
	return mysqld.ExecuteSuperQueryList(ctx, []string{"SET GLOBAL offline_mode = " + value})
}
