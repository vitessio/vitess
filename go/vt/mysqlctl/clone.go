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
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttls"
)

const (
	clonePluginStatusQuery = "SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'clone'"
	cloneStatusQuery       = "SELECT STATE, ERROR_NO, ERROR_MESSAGE FROM performance_schema.clone_status ORDER BY ID DESC LIMIT 1"

	// clonePrimaryPositionTimeout is how long we wait to read the GTID position
	// after the clone is done.
	clonePrimaryPositionTimeout = 30 * time.Second
)

var (
	cloneFromPrimary        = false
	cloneFromTablet         = ""
	cloneRestartWaitTimeout = 5 * time.Minute
)

func init() {
	for _, cmd := range []string{"vtcombo", "vttablet", "vtbackup", "vttestserver"} {
		servenv.OnParseFor(cmd, registerCloneFlags)
	}
}

func registerCloneFlags(fs *pflag.FlagSet) {
	utils.SetFlagBoolVar(fs, &cloneFromPrimary, "clone-from-primary", cloneFromPrimary, "Clone data from the primary tablet in the shard using MySQL CLONE REMOTE instead of restoring from backup. Requires MySQL 8.0.17+. Mutually exclusive with --clone-from-tablet.")
	utils.SetFlagStringVar(fs, &cloneFromTablet, "clone-from-tablet", cloneFromTablet, "Clone data from this tablet using MySQL CLONE REMOTE instead of restoring from backup (tablet alias, e.g., zone1-123). Requires MySQL 8.0.17+. Mutually exclusive with --clone-from-primary.")
	utils.SetFlagDurationVar(fs, &cloneRestartWaitTimeout, "clone-restart-wait-timeout", cloneRestartWaitTimeout, "Timeout for waiting for MySQL to restart after CLONE REMOTE.")
}

// CloneFromDonor clones data from the specified donor tablet using MySQL CLONE REMOTE.
// It returns the GTID position of the cloned data.
// If mycnf is non-nil, CloneFromDonor may use it to restart mysqld locally when
// CLONE completes but MySQL cannot restart itself.
func CloneFromDonor(ctx context.Context, topoServer *topo.Server, mysqld MysqlDaemon, mycnf *Mycnf, keyspace, shard string) (replication.Position, error) {
	var donorAlias *topodatapb.TabletAlias
	var err error

	switch {
	case cloneFromPrimary && cloneFromTablet != "":
		return replication.Position{}, errors.New("--clone-from-primary and --clone-from-tablet are mutually exclusive")
	case cloneFromPrimary:
		// Look up the primary tablet from topology.
		log.Info(fmt.Sprintf("Looking up primary tablet for shard %s/%s for use as CLONE REMOTE donor", keyspace, shard))
		si, err := topoServer.GetShard(ctx, keyspace, shard)
		if err != nil {
			return replication.Position{}, fmt.Errorf("failed to get shard %s/%s: %v", keyspace, shard, err)
		}
		if topoproto.TabletAliasIsZero(si.PrimaryAlias) {
			return replication.Position{}, fmt.Errorf("shard %s/%s has no primary", keyspace, shard)
		}
		donorAlias = si.PrimaryAlias
		log.Info(fmt.Sprintf("Found primary tablet %s for use as CLONE REMOTE donor", topoproto.TabletAliasString(donorAlias)))
	case cloneFromTablet != "":
		// Parse the explicit donor tablet alias.
		log.Info(fmt.Sprintf("Using tablet %s for use as CLONE REMOTE donor", cloneFromTablet))
		donorAlias, err = topoproto.ParseTabletAlias(cloneFromTablet)
		if err != nil {
			return replication.Position{}, fmt.Errorf("invalid tablet alias %q: %v", cloneFromTablet, err)
		}
	default:
		return replication.Position{}, errors.New("no donor specified")
	}

	// Get donor tablet info from topology.
	donorTablet, err := topoServer.GetTablet(ctx, donorAlias)
	if err != nil {
		return replication.Position{}, fmt.Errorf("failed to get tablet %s from topology: %v", topoproto.TabletAliasString(donorAlias), err)
	}

	// Get clone credentials.
	cloneConfig := dbconfigs.GlobalDBConfigs.CloneUser
	if cloneConfig.User == "" {
		return replication.Position{}, errors.New("clone user not configured; set --db-clone-user flag")
	}

	// Create the clone executor.
	executor := &CloneExecutor{
		DonorHost:     donorTablet.MysqlHostname,
		DonorPort:     int(donorTablet.MysqlPort),
		DonorUser:     cloneConfig.User,
		DonorPassword: cloneConfig.Password,
		UseSSL:        cloneConfig.UseSSL,
	}

	log.Info(fmt.Sprintf("Clone executor configured for donor %s:%d", executor.DonorHost, executor.DonorPort))

	// Execute the clone operation.
	// Note: ExecuteClone will wait for mysqld to restart and for the CLONE plugin to report successful completion
	// success via performance_schema before returning.
	if err := executor.ExecuteClone(ctx, mysqld, mycnf, cloneRestartWaitTimeout); err != nil {
		return replication.Position{}, fmt.Errorf("clone execution failed: %v", err)
	}

	// Use a detached context for post-clone operations: the parent context may
	// have been canceled by an OnTerm callback when MySQL exited during the
	// clone-induced restart.
	posCtx, posCancel := context.WithTimeout(context.WithoutCancel(ctx), clonePrimaryPositionTimeout)
	defer posCancel()

	pos, err := mysqld.PrimaryPosition(posCtx)
	if err != nil {
		return replication.Position{}, fmt.Errorf("failed to get position after clone: %v", err)
	}

	log.Info(fmt.Sprintf("Clone completed successfully at position %v", pos))
	return pos, nil
}

// CloneExecutor handles MySQL CLONE REMOTE operations for backup and replica provisioning.
// It executes CLONE INSTANCE FROM on the recipient to clone data from a donor.
type CloneExecutor struct {
	// DonorHost is the hostname or IP of the donor MySQL instance.
	DonorHost string
	// DonorPort is the MySQL port of the donor instance.
	DonorPort int
	// DonorUser is the MySQL user for clone operations (needs BACKUP_ADMIN on donor).
	DonorUser string
	// DonorPassword is the password for the clone user.
	DonorPassword string
	// UseSSL indicates whether to use SSL for the clone connection.
	UseSSL bool
}

// validateRecipient checks that the recipient MySQL instance meets all prerequisites for cloning.
// It verifies:
// - MySQL version >= 8.0.17
// - Clone plugin is installed
func (c *CloneExecutor) validateRecipient(ctx context.Context, mysqld MysqlDaemon) error {
	// Check MySQL version using capabilities system
	if err := c.checkCloneCapability(ctx, mysqld); err != nil {
		return err
	}

	// Check clone plugin is installed
	if err := c.checkClonePluginInstalled(ctx, mysqld); err != nil {
		return err
	}

	return nil
}

// validateDonorRemote connects to the donor MySQL instance and validates it meets
// all prerequisites for cloning. This is called from ExecuteClone to verify the
// donor before attempting the clone operation.
func (c *CloneExecutor) validateDonorRemote(ctx context.Context) error {
	conn, err := mysql.Connect(ctx, c.donorConnParams())
	if err != nil {
		return vterrors.Wrapf(err, "failed to connect to donor %s:%d", c.DonorHost, c.DonorPort)
	}
	defer conn.Close()

	// Check MySQL version
	qr, err := conn.ExecuteFetch("SELECT @@version", 1, false)
	if err != nil {
		return vterrors.Wrapf(err, "failed to query donor MySQL version")
	}
	if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "empty version result from donor")
	}
	versionStr := qr.Rows[0][0].ToString()
	capableOf := mysql.ServerVersionCapableOf(versionStr)
	if capableOf == nil {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "unable to determine MySQL capabilities for donor version %q", versionStr)
	}
	ok, err := capableOf(capabilities.MySQLClonePluginFlavorCapability)
	if err != nil {
		return vterrors.Wrapf(err, "failed to check donor clone capability")
	}
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "donor MySQL CLONE requires version 8.0.17 or higher, got %s", versionStr)
	}

	// Check clone plugin is installed
	qr, err = conn.ExecuteFetch(clonePluginStatusQuery, 1, false)
	if err != nil {
		return vterrors.Wrapf(err, "failed to check donor clone plugin status")
	}
	if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "clone plugin is not installed on donor (add 'plugin-load-add=mysql_clone.so' to my.cnf)")
	}
	status := qr.Rows[0][0].ToString()
	if !strings.EqualFold(status, "ACTIVE") {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "clone plugin is not active on donor (status: %s)", status)
	}

	log.Info(fmt.Sprintf("Donor %s:%d validated successfully (MySQL %s)", c.DonorHost, c.DonorPort, versionStr))
	return nil
}

// checkCloneCapability verifies that the MySQL version supports the CLONE plugin.
func (c *CloneExecutor) checkCloneCapability(ctx context.Context, mysqld MysqlDaemon) error {
	versionStr, err := mysqld.GetVersionString(ctx)
	if err != nil {
		return vterrors.Wrapf(err, "failed to query MySQL version")
	}

	// GetVersionString may return either SQL query result (e.g., "8.0.44") or CLI output
	// (e.g., "/usr/sbin/mysqld Ver 8.0.44..."). Try parsing as CLI output first to
	// extract a clean version string.
	_, version, parseErr := ParseVersionString(versionStr)
	if parseErr == nil {
		versionStr = fmt.Sprintf("%d.%d.%d", version.Major, version.Minor, version.Patch)
	}

	capableOf := mysql.ServerVersionCapableOf(versionStr)
	if capableOf == nil {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "unable to determine MySQL capabilities for version %q", versionStr)
	}

	ok, err := capableOf(capabilities.MySQLClonePluginFlavorCapability)
	if err != nil {
		return vterrors.Wrapf(err, "failed to check clone capability")
	}
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "MySQL CLONE requires version 8.0.17 or higher, got %s", versionStr)
	}

	return nil
}

func (c *CloneExecutor) donorConnParams() *mysql.ConnParams {
	params := &mysql.ConnParams{
		Host:  c.DonorHost,
		Port:  c.DonorPort,
		Uname: c.DonorUser,
		Pass:  c.DonorPassword,
	}
	if c.UseSSL {
		params.SslMode = vttls.Required
	} else {
		params.SslMode = vttls.Disabled
	}
	return params
}

// ExecuteClone performs CLONE REMOTE from the donor to the recipient.
// This will:
// 1. Set clone_valid_donor_list on the recipient
// 2. Execute CLONE INSTANCE FROM on the recipient
// 3. Wait for MySQL to restart and verify clone completed successfully
//
// If mycnf is non-nil, ExecuteClone may use it to restart mysqld when CLONE
// completes but MySQL cannot restart itself (ERRestartServerFailed).
//
// The waitTimeout specifies how long to wait for MySQL to restart and
// report clone completion after the CLONE command finishes.
//
// Note: This operation will DESTROY all existing data on the recipient.
func (c *CloneExecutor) ExecuteClone(ctx context.Context, mysqld MysqlDaemon, mycnf *Mycnf, waitTimeout time.Duration) error {
	if !MySQLCloneEnabled() {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "MySQL CLONE not enabled; set --mysql-clone-enabled=true on both donor and recipient")
	}

	// Validate recipient prerequisites
	if err := c.validateRecipient(ctx, mysqld); err != nil {
		return vterrors.Wrapf(err, "recipient validation failed")
	}

	// Validate donor prerequisites by connecting remotely
	if err := c.validateDonorRemote(ctx); err != nil {
		return vterrors.Wrapf(err, "donor validation failed")
	}

	log.Info(fmt.Sprintf("Starting CLONE REMOTE from %s:%d", c.DonorHost, c.DonorPort))

	// Set the valid donor list
	donorAddr := fmt.Sprintf("%s:%d", c.DonorHost, c.DonorPort)
	setDonorListQuery := fmt.Sprintf("SET GLOBAL clone_valid_donor_list = '%s'", donorAddr)

	if err := mysqld.ExecuteSuperQuery(ctx, setDonorListQuery); err != nil {
		return vterrors.Wrapf(err, "failed to set clone_valid_donor_list")
	}

	// Build the CLONE INSTANCE command
	cloneCmd := c.buildCloneCommand()

	log.Info(fmt.Sprintf("Executing CLONE INSTANCE FROM %s:%d (this may take a while)", c.DonorHost, c.DonorPort))

	// Execute the clone command. When clone completes, MySQL restarts automatically
	// which will cause the connection to drop. We ignore this error and verify
	// success by checking clone_status after MySQL comes back up.
	cloneErr := mysqld.ExecuteSuperQuery(ctx, cloneCmd)
	if cloneErr != nil && !isCloneConnError(cloneErr) {
		return vterrors.Wrapf(cloneErr, "clone command failed")
	}

	verifyCtx, verifyCancel, err := c.prepareCloneVerification(ctx, mysqld, mycnf, cloneErr, waitTimeout)
	if err != nil {
		return err
	}
	defer verifyCancel()

	// Wait for MySQL to restart and verify clone completed successfully.
	if err := c.waitForCloneComplete(verifyCtx, mysqld, waitTimeout); err != nil {
		return vterrors.Wrapf(err, "clone success verification failed")
	}

	log.Info(fmt.Sprintf("CLONE REMOTE completed successfully from %s:%d", c.DonorHost, c.DonorPort))
	return nil
}

// prepareCloneVerification returns the context used for the post-clone
// verification phase. If the CLONE command caused mysqld to exit, it detaches
// verification from the parent context so caller cancellation triggered by that
// exit does not abort the success check. It also performs a manual restart when
// CLONE completed but mysqld could not restart itself.
func (c *CloneExecutor) prepareCloneVerification(ctx context.Context, mysqld MysqlDaemon, mycnf *Mycnf, cloneErr error, waitTimeout time.Duration) (context.Context, context.CancelFunc, error) {
	if cloneErr == nil {
		return ctx, func() {}, nil
	}

	log.Info(
		"CLONE command returned, connection was likely lost during MySQL restart",
		slog.Any("error", cloneErr),
	)

	verifyCtx, verifyCancel := context.WithTimeout(context.WithoutCancel(ctx), waitTimeout)
	if isRestartServerFailed(cloneErr) {
		if mycnf == nil {
			verifyCancel()
			return nil, nil, vterrors.Wrapf(cloneErr, "mysqld could not restart itself after clone and no my.cnf available for manual restart")
		}
		log.Info("MySQL could not restart itself after CLONE; restarting manually")
		if err := mysqld.StartAfterExit(verifyCtx, mycnf); err != nil {
			verifyCancel()
			return nil, nil, vterrors.Wrapf(err, "failed to restart MySQL after clone")
		}
	}

	return verifyCtx, verifyCancel, nil
}

// buildCloneCommand constructs the CLONE INSTANCE SQL command.
func (c *CloneExecutor) buildCloneCommand() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CLONE INSTANCE FROM %s@%s:%d",
		sqltypes.EncodeStringSQL(c.DonorUser),
		sqltypes.EncodeStringSQL(c.DonorHost),
		c.DonorPort))
	sb.WriteString(" IDENTIFIED BY " + sqltypes.EncodeStringSQL(c.DonorPassword))

	if c.UseSSL {
		sb.WriteString(" REQUIRE SSL")
	} else {
		sb.WriteString(" REQUIRE NO SSL")
	}

	return sb.String()
}

func isCloneConnError(err error) bool {
	var sqlErr *sqlerror.SQLError
	if !errors.As(err, &sqlErr) {
		return false
	}
	switch sqlErr.Number() {
	case sqlerror.CRServerGone, sqlerror.CRServerLost, sqlerror.ERRestartServerFailed:
		return true
	default:
		return false
	}
}

// isRestartServerFailed reports whether err is an ERRestartServerFailed error,
// meaning MySQL completed the clone but could not restart itself because it is
// not managed by a process supervisor.
func isRestartServerFailed(err error) bool {
	var sqlErr *sqlerror.SQLError
	return errors.As(err, &sqlErr) && sqlErr.Number() == sqlerror.ERRestartServerFailed
}

// checkClonePluginInstalled verifies that the clone plugin is loaded.
func (c *CloneExecutor) checkClonePluginInstalled(ctx context.Context, mysqld MysqlDaemon) error {
	result, err := mysqld.FetchSuperQuery(ctx, clonePluginStatusQuery)
	if err != nil {
		return vterrors.Wrapf(err, "failed to check clone plugin status")
	}

	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "clone plugin is not installed (add 'plugin-load-add=mysql_clone.so' to my.cnf)")
	}

	status := result.Rows[0][0].ToString()
	if !strings.EqualFold(status, "ACTIVE") {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "clone plugin is not active (status: %s)", status)
	}

	return nil
}

// waitForCloneComplete waits for a clone operation to complete by polling
// performance_schema.clone_status. This handles the MySQL restart that occurs
// after clone - connections will fail during restart and this function will
// retry until MySQL is back and clone_status shows completion.
func (c *CloneExecutor) waitForCloneComplete(ctx context.Context, mysqld MysqlDaemon, timeout time.Duration) error {
	const pollInterval = time.Second

	log.Info(fmt.Sprintf("Waiting for clone to complete (timeout: %v)", timeout))

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for clone to complete after %v", timeout)
		case <-ticker.C:
			// Try to query clone status - connection may fail if MySQL is restarting
			result, err := mysqld.FetchSuperQuery(ctx, cloneStatusQuery)
			if err != nil {
				// Connection failures are expected during MySQL restart
				log.Info(fmt.Sprintf("Clone status query failed (MySQL may be restarting): %v", err))
				continue
			}

			if len(result.Rows) == 0 {
				// No clone status yet - MySQL may have just started
				log.Info("No clone status found, waiting...")
				continue
			}

			if len(result.Rows[0]) < 3 {
				// Unexpected row format
				log.Warn(fmt.Sprintf("Unexpected clone_status row format: got %d columns, expected 3", len(result.Rows[0])))
				continue
			}

			state := result.Rows[0][0].ToString()
			errorNo := result.Rows[0][1].ToString()
			errorMsg := result.Rows[0][2].ToString()

			log.Info(fmt.Sprintf("Clone status: STATE=%s, ERROR_NO=%s", state, errorNo))

			switch {
			case strings.EqualFold(state, "Completed"):
				if errorNo != "0" {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "clone completed with error %s: %s", errorNo, errorMsg)
				}
				log.Info(fmt.Sprintf("Clone completed successfully from %s:%d", c.DonorHost, c.DonorPort))
				return nil
			case strings.EqualFold(state, "Failed"):
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "clone failed with error %s: %s", errorNo, errorMsg)
			case strings.EqualFold(state, "In Progress"), strings.EqualFold(state, "Not Started"):
				// Still running, keep waiting
				continue
			default:
				// Unknown state, keep waiting but log it
				log.Warn("Unknown clone state: " + state)
				continue
			}
		}
	}
}
