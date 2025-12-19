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
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/vt/log"
)

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

// ValidateDonor checks that the donor MySQL instance meets all prerequisites for cloning.
// It verifies:
// - MySQL version >= 8.0.17
// - Clone plugin is installed
// - No non-InnoDB tables exist (clone only supports InnoDB)
func (c *CloneExecutor) ValidateDonor(ctx context.Context, mysqld MysqlDaemon) error {
	// Check MySQL version using capabilities system
	if err := c.checkCloneCapability(ctx, mysqld); err != nil {
		return err
	}

	// Check for non-InnoDB tables
	if err := c.checkNoNonInnoDBTables(ctx, mysqld); err != nil {
		return err
	}

	// Check clone plugin is installed
	if err := c.checkClonePluginInstalled(ctx, mysqld); err != nil {
		return err
	}

	return nil
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

// checkCloneCapability verifies that the MySQL version supports the CLONE plugin.
func (c *CloneExecutor) checkCloneCapability(ctx context.Context, mysqld MysqlDaemon) error {
	result, err := mysqld.FetchSuperQuery(ctx, "SELECT @@version")
	if err != nil {
		return fmt.Errorf("failed to query MySQL version: %w", err)
	}

	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return fmt.Errorf("empty version result")
	}

	versionStr := result.Rows[0][0].ToString()
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
// 3. The recipient MySQL will restart automatically after clone completes
//
// Note: This operation will DESTROY all existing data on the recipient.
func (c *CloneExecutor) ExecuteClone(ctx context.Context, mysqld MysqlDaemon) error {
	if !MySQLCloneEnabled() {
		return fmt.Errorf("MySQL CLONE not enabled; set --mysql-clone-enabled=true on both donor and recipient")
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

	// Execute the clone command
	// Note: After this command completes, MySQL will restart automatically
	if err := mysqld.ExecuteSuperQuery(ctx, cloneCmd); err != nil {
		return fmt.Errorf("CLONE INSTANCE failed: %w", err)
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

// checkNoNonInnoDBTables verifies that no user tables use non-InnoDB storage engines.
// MySQL CLONE only copies InnoDB data; other engines would result in empty tables.
func (c *CloneExecutor) checkNoNonInnoDBTables(ctx context.Context, mysqld MysqlDaemon) error {
	query := `
		SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE
		FROM information_schema.TABLES
		WHERE ENGINE != 'InnoDB'
		AND ENGINE IS NOT NULL
		AND TABLE_TYPE = 'BASE TABLE'
		AND TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
	`

	result, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to check for non-InnoDB tables: %w", err)
	}

	if len(result.Rows) > 0 {
		var tables []string
		for _, row := range result.Rows {
			schema := row[0].ToString()
			table := row[1].ToString()
			engine := row[2].ToString()
			tables = append(tables, fmt.Sprintf("%s.%s (%s)", schema, table, engine))
		}
		return fmt.Errorf("non-InnoDB tables found (CLONE only supports InnoDB): %s", strings.Join(tables, ", "))
	}

	return nil
}

// checkClonePluginInstalled verifies that the clone plugin is loaded.
func (c *CloneExecutor) checkClonePluginInstalled(ctx context.Context, mysqld MysqlDaemon) error {
	query := "SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'clone'"

	result, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to check clone plugin status: %w", err)
	}

	if len(result.Rows) == 0 {
		return fmt.Errorf("clone plugin is not installed (add 'plugin-load-add=mysql_clone.so' to my.cnf)")
	}

	status := result.Rows[0][0].ToString()
	if status != "ACTIVE" {
		return fmt.Errorf("clone plugin is not active (status: %s)", status)
	}

	return nil
}
