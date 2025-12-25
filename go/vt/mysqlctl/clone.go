/*
Copyright 2025 The Vitess Authors.

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
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"
)

var (
	cloneFromPrimary = false
	cloneFromTablet  = ""
)

func init() {
	// TODO: enable these flags for vtbackup.
	for _, cmd := range []string{"vttablet" /*, "vtbackup"*/} {
		servenv.OnParseFor(cmd, registerCloneFlags)
	}
}

func registerCloneFlags(fs *pflag.FlagSet) {
	utils.SetFlagBoolVar(fs, &cloneFromPrimary, "clone-from-primary", cloneFromPrimary, "Clone data from the primary tablet in the shard using MySQL CLONE REMOTE instead of restoring from backup. Requires MySQL 8.0.17+. Mutually exclusive with --clone-from-tablet.")
	utils.SetFlagStringVar(fs, &cloneFromTablet, "clone-from-tablet", cloneFromTablet, "Clone data from this tablet using MySQL CLONE REMOTE instead of restoring from backup (tablet alias, e.g., zone1-123). Requires MySQL 8.0.17+. Mutually exclusive with --clone-from-primary.")
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

// ValidateRecipient checks that the recipient MySQL instance meets all prerequisites for cloning.
// It verifies:
// - MySQL version >= 8.0.17
// - Clone plugin is installed
func (c *CloneExecutor) ValidateRecipient(ctx context.Context, mysqld MysqlDaemon) error {
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
	params := &mysql.ConnParams{
		Host:  c.DonorHost,
		Port:  c.DonorPort,
		Uname: c.DonorUser,
		Pass:  c.DonorPassword,
	}

	conn, err := mysql.Connect(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to connect to donor %s:%d: %w", c.DonorHost, c.DonorPort, err)
	}
	defer conn.Close()

	// Check MySQL version
	qr, err := conn.ExecuteFetch("SELECT @@version", 1, false)
	if err != nil {
		return fmt.Errorf("failed to query donor MySQL version: %w", err)
	}
	if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
		return errors.New("empty version result from donor")
	}
	versionStr := qr.Rows[0][0].ToString()
	capableOf := mysql.ServerVersionCapableOf(versionStr)
	if capableOf == nil {
		return fmt.Errorf("unable to determine MySQL capabilities for donor version %q", versionStr)
	}
	ok, err := capableOf(capabilities.MySQLClonePluginFlavorCapability)
	if err != nil {
		return fmt.Errorf("failed to check donor clone capability: %w", err)
	}
	if !ok {
		return fmt.Errorf("donor MySQL CLONE requires version 8.0.17 or higher, got %s", versionStr)
	}

	// Check clone plugin is installed
	qr, err = conn.ExecuteFetch("SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'clone'", 1, false)
	if err != nil {
		return fmt.Errorf("failed to check donor clone plugin status: %w", err)
	}
	if len(qr.Rows) == 0 {
		return errors.New("clone plugin is not installed on donor (add 'plugin-load-add=mysql_clone.so' to my.cnf)")
	}
	status := qr.Rows[0][0].ToString()
	if status != "ACTIVE" {
		return fmt.Errorf("clone plugin is not active on donor (status: %s)", status)
	}

	// Check for non-InnoDB tables
	qr, err = conn.ExecuteFetch(`
		SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE
		FROM information_schema.TABLES
		WHERE ENGINE != 'InnoDB'
		AND ENGINE IS NOT NULL
		AND TABLE_TYPE = 'BASE TABLE'
		AND TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
	`, 1000, false)
	if err != nil {
		return fmt.Errorf("failed to check donor for non-InnoDB tables: %w", err)
	}
	if len(qr.Rows) > 0 {
		var tables []string
		for _, row := range qr.Rows {
			schema := row[0].ToString()
			table := row[1].ToString()
			engine := row[2].ToString()
			tables = append(tables, fmt.Sprintf("%s.%s (%s)", schema, table, engine))
		}
		return fmt.Errorf("non-InnoDB tables found on donor (CLONE only supports InnoDB): %s", strings.Join(tables, ", "))
	}

	log.Infof("Donor %s:%d validated successfully (MySQL %s)", c.DonorHost, c.DonorPort, versionStr)
	return nil
}

// checkCloneCapability verifies that the MySQL version supports the CLONE plugin.
func (c *CloneExecutor) checkCloneCapability(ctx context.Context, mysqld MysqlDaemon) error {
	versionStr, err := mysqld.GetVersionString(ctx)
	if err != nil {
		return fmt.Errorf("failed to query MySQL version: %w", err)
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
		return fmt.Errorf("unable to determine MySQL capabilities for version %q", versionStr)
	}

	ok, err := capableOf(capabilities.MySQLClonePluginFlavorCapability)
	if err != nil {
		return fmt.Errorf("failed to check clone capability: %w", err)
	}
	if !ok {
		return fmt.Errorf("MySQL CLONE requires version 8.0.17 or higher, got %s", versionStr)
	}

	return nil
}

// ExecuteClone performs CLONE REMOTE from the donor to the recipient.
// This will:
// 1. Set clone_valid_donor_list on the recipient
// 2. Execute CLONE INSTANCE FROM on the recipient
// 3. Wait for MySQL to restart and verify clone completed successfully
//
// The restartTimeout specifies how long to wait for MySQL to restart and
// report clone completion after the CLONE command finishes.
//
// Note: This operation will DESTROY all existing data on the recipient.
func (c *CloneExecutor) ExecuteClone(ctx context.Context, mysqld MysqlDaemon, restartTimeout time.Duration) error {
	if !MySQLCloneEnabled() {
		return errors.New("MySQL CLONE not enabled; set --mysql-clone-enabled=true on both donor and recipient")
	}

	// Validate recipient prerequisites
	if err := c.ValidateRecipient(ctx, mysqld); err != nil {
		return fmt.Errorf("recipient validation failed: %w", err)
	}

	// Validate donor prerequisites by connecting remotely
	if err := c.validateDonorRemote(ctx); err != nil {
		return fmt.Errorf("donor validation failed: %w", err)
	}

	log.Infof("Starting CLONE REMOTE from %s:%d", c.DonorHost, c.DonorPort)

	// Set the valid donor list
	donorAddr := fmt.Sprintf("%s:%d", c.DonorHost, c.DonorPort)
	setDonorListQuery := fmt.Sprintf("SET GLOBAL clone_valid_donor_list = '%s'", donorAddr)

	if err := mysqld.ExecuteSuperQuery(ctx, setDonorListQuery); err != nil {
		return fmt.Errorf("failed to set clone_valid_donor_list: %w", err)
	}

	// Build the CLONE INSTANCE command
	cloneCmd := c.buildCloneCommand()

	log.Infof("Executing CLONE INSTANCE FROM %s:%d (this may take a while)", c.DonorHost, c.DonorPort)

	// Execute the clone command. When clone completes, MySQL restarts automatically
	// which will cause the connection to drop. We ignore this error and verify
	// success by checking clone_status after MySQL comes back up.
	if err := mysqld.ExecuteSuperQuery(ctx, cloneCmd); err != nil {
		log.Infof("CLONE command returned (connection likely lost due to MySQL restart): %v", err)
	}

	// Wait for MySQL to restart and verify clone completed successfully
	if err := c.waitForCloneComplete(ctx, mysqld, restartTimeout); err != nil {
		return fmt.Errorf("clone verification failed: %w", err)
	}

	log.Infof("CLONE REMOTE completed successfully from %s:%d", c.DonorHost, c.DonorPort)
	return nil
}

// buildCloneCommand constructs the CLONE INSTANCE SQL command.
func (c *CloneExecutor) buildCloneCommand() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CLONE INSTANCE FROM '%s'@'%s':%d",
		c.DonorUser, c.DonorHost, c.DonorPort))
	sb.WriteString(fmt.Sprintf(" IDENTIFIED BY '%s'", c.DonorPassword))

	if c.UseSSL {
		sb.WriteString(" REQUIRE SSL")
	} else {
		sb.WriteString(" REQUIRE NO SSL")
	}

	return sb.String()
}

// checkClonePluginInstalled verifies that the clone plugin is loaded.
func (c *CloneExecutor) checkClonePluginInstalled(ctx context.Context, mysqld MysqlDaemon) error {
	query := "SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'clone'"

	result, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to check clone plugin status: %w", err)
	}

	if len(result.Rows) == 0 {
		return errors.New("clone plugin is not installed (add 'plugin-load-add=mysql_clone.so' to my.cnf)")
	}

	status := result.Rows[0][0].ToString()
	if status != "ACTIVE" {
		return fmt.Errorf("clone plugin is not active (status: %s)", status)
	}

	return nil
}

// CloneFromDonor clones data from the specified donor tablet using MySQL CLONE REMOTE.
// It returns the GTID position of the cloned data.
func CloneFromDonor(ctx context.Context, topoServer *topo.Server, mysqld MysqlDaemon, keyspace, shard string) (replication.Position, error) {
	var donorAlias *topodatapb.TabletAlias
	var err error

	switch {
	case cloneFromPrimary:
		// Look up the primary tablet from topology.
		log.Infof("Looking up primary tablet for shard %s/%s", keyspace, shard)
		si, err := topoServer.GetShard(ctx, keyspace, shard)
		if err != nil {
			return replication.Position{}, fmt.Errorf("failed to get shard %s/%s: %v", keyspace, shard, err)
		}
		if topoproto.TabletAliasIsZero(si.PrimaryAlias) {
			return replication.Position{}, fmt.Errorf("shard %s/%s has no primary", keyspace, shard)
		}
		donorAlias = si.PrimaryAlias
		log.Infof("Found primary tablet: %s", topoproto.TabletAliasString(donorAlias))
	case cloneFromTablet != "":
		// Parse the explicit donor tablet alias.
		log.Infof("Starting clone-based backup from tablet %s", cloneFromTablet)
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

	log.Infof("Clone executor configured for donor %s:%d", executor.DonorHost, executor.DonorPort)

	// Validate that the recipient (local) MySQL meets prerequisites.
	if err := executor.ValidateRecipient(ctx, mysqld); err != nil {
		return replication.Position{}, fmt.Errorf("recipient validation failed: %v", err)
	}

	// Execute the clone operation.
	// Note: ExecuteClone will wait for myqld to restart and for CLONE to report
	// success via performance_schema before returning.
	if err := executor.ExecuteClone(ctx, mysqld, 5*time.Minute); err != nil {
		return replication.Position{}, fmt.Errorf("clone execution failed: %v", err)
	}

	// Get the GTID position from the cloned data.
	pos, err := mysqld.PrimaryPosition(ctx)
	if err != nil {
		return replication.Position{}, fmt.Errorf("failed to get position after clone: %v", err)
	}

	log.Infof("Clone completed successfully at position %v", pos)
	return pos, nil
}

// waitForCloneComplete waits for a clone operation to complete by polling
// performance_schema.clone_status. This handles the MySQL restart that occurs
// after clone - connections will fail during restart and this function will
// retry until MySQL is back and clone_status shows completion.
func (c *CloneExecutor) waitForCloneComplete(ctx context.Context, mysqld MysqlDaemon, timeout time.Duration) error {
	const pollInterval = time.Second
	query := "SELECT STATE, ERROR_NO, ERROR_MESSAGE FROM performance_schema.clone_status ORDER BY ID DESC LIMIT 1"

	log.Infof("Waiting for clone to complete (timeout: %v)", timeout)

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timeout waiting for clone to complete after %v", timeout)
		case <-ticker.C:
			// Try to query clone status - connection may fail if MySQL is restarting
			result, err := mysqld.FetchSuperQuery(ctx, query)
			if err != nil {
				// Connection failures are expected during MySQL restart
				log.Infof("Clone status query failed (MySQL may be restarting): %v", err)
				continue
			}

			if len(result.Rows) == 0 {
				// No clone status yet - MySQL may have just started
				log.Infof("No clone status found, waiting...")
				continue
			}

			state := result.Rows[0][0].ToString()
			errorNo := result.Rows[0][1].ToString()
			errorMsg := result.Rows[0][2].ToString()

			log.Infof("Clone status: STATE=%s, ERROR_NO=%s", state, errorNo)

			switch state {
			case "Completed":
				if errorNo != "0" {
					return fmt.Errorf("clone completed with error %s: %s", errorNo, errorMsg)
				}
				log.Infof("Clone completed successfully")
				return nil
			case "Failed":
				return fmt.Errorf("clone failed with error %s: %s", errorNo, errorMsg)
			case "In Progress", "Not Started":
				// Still running, keep waiting
				continue
			default:
				// Unknown state, keep waiting but log it
				log.Warningf("Unknown clone state: %s", state)
				continue
			}
		}
	}
}
