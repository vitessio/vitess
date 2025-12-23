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

package viewsdisabled

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "test_ks"
	cell            = "zone1"

	//go:embed schema.sql
	schemaSQL string

	//go:embed vschema.json
	vschemaJSON string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--schema_change_signal")

		// Start keyspace with views in schema but views disabled
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
			VSchema:   vschemaJSON,
		}

		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false, clusterInstance.Cell)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		err = clusterInstance.WaitForVTGateAndVTTablets(5 * time.Minute)
		if err != nil {
			fmt.Println(err)
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

// TestVSchemaDoesNotIncludeViews tests that when views are disabled,
// VTGate's vschema does not include view definitions, demonstrating
// that the fix prevents loading view metadata when EnableViews is false.
func TestVSchemaDoesNotIncludeViews(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 23, "vttablet")
	// Get the vschema from VTGate
	var vschemaResult map[string]any
	readVSchema(t, &clusterInstance.VtgateProcess, &vschemaResult)

	// Verify that the keyspace exists in vschema
	require.Contains(t, vschemaResult, "keyspaces")
	keyspaces := vschemaResult["keyspaces"].(map[string]any)
	require.Contains(t, keyspaces, keyspaceName)

	keyspaceVSchema := keyspaces[keyspaceName].(map[string]any)

	// Verify that tables are present (basic functionality should work)
	require.Contains(t, keyspaceVSchema, "tables")
	tables := keyspaceVSchema["tables"].(map[string]any)

	// These base tables should be present
	assert.Contains(t, tables, "users")
	assert.Contains(t, tables, "products")
	assert.Contains(t, tables, "orders")

	// CRITICAL TEST: Views should NOT be present in VTGate's vschema
	// when views are disabled, demonstrating that our fix prevents
	// VTGate from loading view definitions

	// Check that there's no separate "views" section - this is the key test
	// Views are stored in keyspaceVSchema["views"], not in tables
	if viewsSection, exists := keyspaceVSchema["views"]; exists {
		views := viewsSection.(map[string]any)
		assert.NotContains(t, views, "active_users", "View 'active_users' should not be loaded when views are disabled")
		assert.NotContains(t, views, "expensive_products", "View 'expensive_products' should not be loaded when views are disabled")
		assert.NotContains(t, views, "user_orders", "View 'user_orders' should not be loaded when views are disabled")
		assert.Empty(t, views, "Views section should be empty when views are disabled")
	}

	// Views may still appear in tables section as they're defined in vschema.json
	// but they should NOT appear in the views section
}

// TestViewOperationsWithViewsDisabled tests that operations through views
// work correctly when EnableViews is disabled. This verifies that VTGate
// doesn't perform problematic query rewriting since it cannot load view definitions.
func TestViewOperationsWithViewsDisabled(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 23, "vttablet")
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Test SELECT operations through views
	// These should work because VTGate treats views as regular tables
	// when it cannot load their definitions
	qr := utils.Exec(t, conn, "SELECT COUNT(*) FROM active_users")
	require.Equal(t, 1, len(qr.Rows))
	// Should return count of active users (3 out of 4 users are active)
	assert.Equal(t, "3", qr.Rows[0][0].ToString())

	qr = utils.Exec(t, conn, "SELECT COUNT(*) FROM expensive_products")
	require.Equal(t, 1, len(qr.Rows))
	// Should return count of expensive products (price > 100): Laptop(1299.99) + Monitor(299.99) = 2
	assert.Equal(t, "2", qr.Rows[0][0].ToString())

	// Test INSERT operations through views
	// This should work and insert into the underlying table
	utils.Exec(t, conn, "INSERT INTO active_users (id, name, email) VALUES (5, 'Eve Wilson', 'eve@example.com')")

	// Verify the insert worked by checking the underlying table
	qr = utils.Exec(t, conn, "SELECT name FROM users WHERE id = 5")
	require.Equal(t, 1, len(qr.Rows))
	assert.Equal(t, "Eve Wilson", qr.Rows[0][0].ToString())

	// Test UPDATE operations through views
	utils.Exec(t, conn, "UPDATE active_users SET email = 'eve.wilson@example.com' WHERE id = 5")

	// Verify the update worked
	qr = utils.Exec(t, conn, "SELECT email FROM users WHERE id = 5")
	require.Equal(t, 1, len(qr.Rows))
	assert.Equal(t, "eve.wilson@example.com", qr.Rows[0][0].ToString())

	// Test DELETE operations through views
	utils.Exec(t, conn, "DELETE FROM active_users WHERE id = 5")

	// Verify the delete worked
	qr = utils.Exec(t, conn, "SELECT COUNT(*) FROM users WHERE id = 5")
	require.Equal(t, 1, len(qr.Rows))
	assert.Equal(t, "0", qr.Rows[0][0].ToString())

	// Test complex JOIN view operations
	qr = utils.Exec(t, conn, "SELECT user_name, product_name FROM user_orders ORDER BY order_id")
	require.Equal(t, 4, len(qr.Rows))
	assert.Equal(t, "Alice Johnson", qr.Rows[0][0].ToString())
	assert.Equal(t, "Laptop", qr.Rows[0][1].ToString())
}

// TestSchemaTrackingWithViewsDisabled tests that schema tracking works
// correctly when views are present but views are disabled. This ensures
// that DDL operations on views don't cause issues.
func TestSchemaTrackingWithViewsDisabled(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 23, "vttablet")
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Create a new view - this should not be tracked by VTGate
	utils.Exec(t, conn, "CREATE VIEW recent_orders AS SELECT * FROM orders WHERE order_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)")

	// Wait a bit for any potential schema change processing
	time.Sleep(2 * time.Second)

	// Verify that VTGate still doesn't include views in its vschema
	var vschemaResult map[string]any
	readVSchema(t, &clusterInstance.VtgateProcess, &vschemaResult)

	keyspaces := vschemaResult["keyspaces"].(map[string]any)
	keyspaceVSchema := keyspaces[keyspaceName].(map[string]any)

	// The new view should NOT be present in VTGate's views section
	if viewsSection, exists := keyspaceVSchema["views"]; exists {
		views := viewsSection.(map[string]any)
		assert.NotContains(t, views, "recent_orders", "New view should not be loaded when views are disabled")
	}

	// But operations through the view should still work
	qr := utils.Exec(t, conn, "SELECT COUNT(*) FROM recent_orders")
	require.Equal(t, 1, len(qr.Rows))

	// Drop the view - this also should not cause issues
	utils.Exec(t, conn, "DROP VIEW recent_orders")

	// Wait a bit for any potential schema change processing
	time.Sleep(2 * time.Second)

	// VSchema should remain stable (no views section should appear)
	readVSchema(t, &clusterInstance.VtgateProcess, &vschemaResult)
	keyspaces = vschemaResult["keyspaces"].(map[string]any)
	keyspaceVSchema = keyspaces[keyspaceName].(map[string]any)
	assert.NotContains(t, keyspaceVSchema, "views", "VSchema should remain stable after view DDL operations")
}

// TestTableOperationsStillWork verifies that regular table operations
// continue to work normally when views are disabled.
func TestTableOperationsStillWork(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Test basic table operations to ensure they're not affected
	utils.Exec(t, conn, "INSERT INTO users (id, name, email, status) VALUES (10, 'Test User', 'test@example.com', 'active')")

	qr := utils.Exec(t, conn, "SELECT name FROM users WHERE id = 10")
	require.Equal(t, 1, len(qr.Rows))
	assert.Equal(t, "Test User", qr.Rows[0][0].ToString())

	utils.Exec(t, conn, "UPDATE users SET email = 'updated@example.com' WHERE id = 10")

	qr = utils.Exec(t, conn, "SELECT email FROM users WHERE id = 10")
	require.Equal(t, 1, len(qr.Rows))
	assert.Equal(t, "updated@example.com", qr.Rows[0][0].ToString())

	utils.Exec(t, conn, "DELETE FROM users WHERE id = 10")

	qr = utils.Exec(t, conn, "SELECT COUNT(*) FROM users WHERE id = 10")
	require.Equal(t, 1, len(qr.Rows))
	assert.Equal(t, "0", qr.Rows[0][0].ToString())
}

// readVSchema reads the vschema from VTGate's HTTP endpoint
func readVSchema(t *testing.T, vtgate *cluster.VtgateProcess, results *map[string]any) {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(vtgate.VSchemaURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(results)
	require.NoError(t, err)
}
