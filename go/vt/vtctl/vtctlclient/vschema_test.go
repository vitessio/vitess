package vtctlclient

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestVSchemaAtomicOperations(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("")
	client := newTestClient(ts)

	tests := []struct {
		name        string
		setup       func(t *testing.T)
		operation   func(t *testing.T) error
		concurrent  func(t *testing.T)
		wantErr     bool
		errContains string
	}{
		{
			name: "successful_add_table",
			setup: func(t *testing.T) {
				// Initialize empty vschema
				initEmptyVSchema(t, ts, "test_keyspace")
			},
			operation: func(t *testing.T) error {
				return client.VSchema().AddTable(ctx, "test_keyspace", "test_table", &vschemapb.Table{})
			},
			wantErr: false,
		},
		{
			name: "concurrent_modification",
			setup: func(t *testing.T) {
				initEmptyVSchema(t, ts, "test_keyspace")
			},
			operation: func(t *testing.T) error {
				return client.VSchema().AddTable(ctx, "test_keyspace", "test_table", &vschemapb.Table{})
			},
			concurrent: func(t *testing.T) {
				// Simulate concurrent modification
				client2 := newTestClient(ts)
				err := client2.VSchema().AddTable(ctx, "test_keyspace", "other_table", &vschemapb.Table{})
				require.NoError(t, err)
			},
			wantErr:     true,
			errContains: "version mismatch",
		},
		{
			name: "retry_success",
			setup: func(t *testing.T) {
				initEmptyVSchema(t, ts, "test_keyspace")
			},
			operation: func(t *testing.T) error {
				return client.VSchema().AddTable(ctx, "test_keyspace", "test_table", &vschemapb.Table{})
			},
			concurrent: func(t *testing.T) {
				// Simulate temporary interference
				time.Sleep(10 * time.Millisecond)
				client2 := newTestClient(ts)
				err := client2.VSchema().AddTable(ctx, "test_keyspace", "other_table", &vschemapb.Table{})
				require.NoError(t, err)
			},
			wantErr: false, // Should succeed after retry
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(t)

			if tt.concurrent != nil {
				go tt.concurrent(t)
			}

			err := tt.operation(t)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				// Verify final state
				vschema, err := ts.GetVSchema(ctx, "test_keyspace")
				require.NoError(t, err)
				assert.NotNil(t, vschema)
			}
		})
	}
}

func TestVSchemaVersionConflicts(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("")
	client := newTestClient(ts)

	// Test multiple concurrent modifications
	t.Run("multiple_concurrent_modifications", func(t *testing.T) {
		initEmptyVSchema(t, ts, "test_keyspace")

		// Launch multiple goroutines attempting modifications
		const numGoroutines = 5
		errChan := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(idx int) {
				err := client.VSchema().AddTable(ctx, "test_keyspace", 
					fmt.Sprintf("table_%d", idx), &vschemapb.Table{})
				errChan <- err
			}(i)
		}

		// Collect results
		successCount := 0
		for i := 0; i < numGoroutines; i++ {
			err := <-errChan
			if err == nil {
				successCount++
			}
		}

		// At least one should succeed, others should fail with version conflict
		assert.True(t, successCount >= 1)
		assert.True(t, successCount < numGoroutines)
	})
}

func TestVSchemaErrorConditions(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("")
	client := newTestClient(ts)

	tests := []struct {
		name        string
		operation   func() error
		errContains string
	}{
		{
			name: "invalid_keyspace",
			operation: func() error {
				return client.VSchema().AddTable(ctx, "", "table", &vschemapb.Table{})
			},
			errContains: "invalid keyspace",
		},
		{
			name: "invalid_table_name",
			operation: func() error {
				return client.VSchema().AddTable(ctx, "keyspace", "", &vschemapb.Table{})
			},
			errContains: "invalid table name",
		},
		{
			name: "nil_table_spec",
			operation: func() error {
				return client.VSchema().AddTable(ctx, "keyspace", "table", nil)
			},
			errContains: "nil table spec",
		},
		{
			name: "keyspace_not_found",
			operation: func() error {
				return client.VSchema().AddTable(ctx, "nonexistent", "table", &vschemapb.Table{})
			},
			errContains: "keyspace not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}

// Helper functions

func initEmptyVSchema(t *testing.T, ts *memorytopo.Server, keyspace string) {
	ctx := context.Background()
	err := ts.CreateKeyspace(ctx, keyspace, &vschemapb.Keyspace{})
	require.NoError(t, err)
}

func newTestClient(ts *memorytopo.Server) VtctlClient {
	// Create a test client implementation
	return &testVtctlClient{
		ts: ts,
	}
}

type testVtctlClient struct {
	ts *memorytopo.Server
}

// Implement VtctlClient interface methods... 