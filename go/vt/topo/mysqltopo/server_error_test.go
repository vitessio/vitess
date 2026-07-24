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

package mysqltopo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
)

func TestConvertError(t *testing.T) {
	dupEntry := &mysql.MySQLError{Number: 1062, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "Duplicate entry 'keyspaces/commerce' for key 'topo_files.PRIMARY'"}
	deadlock := &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "Deadlock found when trying to get lock; try restarting transaction"}
	lockWait := &mysql.MySQLError{Number: 1205, SQLState: [5]byte{'H', 'Y', '0', '0', '0'}, Message: "Lock wait timeout exceeded; try restarting transaction"}

	tests := []struct {
		name     string
		err      error
		wantCode topo.ErrorCode
	}{{
		name:     "driver dup entry",
		err:      dupEntry,
		wantCode: topo.NodeExists,
	}, {
		name:     "wrapped driver dup entry",
		err:      fmt.Errorf("insert failed: %w", dupEntry),
		wantCode: topo.NodeExists,
	}, {
		name:     "driver deadlock is retryable",
		err:      deadlock,
		wantCode: topo.Timeout,
	}, {
		name:     "driver lock wait timeout is retryable",
		err:      lockWait,
		wantCode: topo.Timeout,
	}, {
		name:     "vitess-wrapped errno message",
		err:      errors.New("Duplicate entry 'x' for key 'PRIMARY' (errno 1062) (sqlstate 23000) during query: insert"),
		wantCode: topo.NodeExists,
	}, {
		name:     "native ERROR message",
		err:      errors.New("ERROR 1062 (23000): Duplicate entry 'x' for key 'PRIMARY'"),
		wantCode: topo.NodeExists,
	}, {
		name:     "context canceled",
		err:      context.Canceled,
		wantCode: topo.Interrupted,
	}, {
		name:     "wrapped context canceled",
		err:      fmt.Errorf("query aborted: %w", context.Canceled),
		wantCode: topo.Interrupted,
	}, {
		name:     "context deadline exceeded",
		err:      context.DeadlineExceeded,
		wantCode: topo.Timeout,
	}, {
		name:     "no rows",
		err:      sql.ErrNoRows,
		wantCode: topo.NoNode,
	}, {
		name:     "wrapped no rows",
		err:      fmt.Errorf("get file: %w", sql.ErrNoRows),
		wantCode: topo.NoNode,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertError(tt.err, "some/path")
			require.True(t, topo.IsErrType(got, tt.wantCode), "convertError(%v) = %v, want topo error code %v", tt.err, got, tt.wantCode)
		})
	}

	t.Run("nil", func(t *testing.T) {
		require.NoError(t, convertError(nil, "some/path"))
	})

	t.Run("unrelated error passes through", func(t *testing.T) {
		err := errors.New("connection refused")
		require.Equal(t, err, convertError(err, "some/path"))
	})

	t.Run("unrelated driver error passes through", func(t *testing.T) {
		err := &mysql.MySQLError{Number: 1146, Message: "Table 'topo.topo_files' doesn't exist"}
		require.Equal(t, error(err), convertError(err, "some/path"))
	})
}

// TestCreateConcurrentNodeExists verifies that when concurrent Create calls
// race past the existence pre-check, the losers' duplicate-key errors from the
// go-sql-driver surface as topo.NodeExists rather than as raw driver errors.
func TestCreateConcurrentNodeExists(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	ctx := context.Background()
	const workers = 20
	errs := make(chan error, workers)
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < workers; i++ {
		go func() {
			start.Wait()
			_, err := server.Create(ctx, "concurrent/create/race", []byte("contents"))
			errs <- err
		}()
	}
	start.Done()

	var created, exists int
	for i := 0; i < workers; i++ {
		err := <-errs
		switch {
		case err == nil:
			created++
		case topo.IsErrType(err, topo.NodeExists):
			exists++
		default:
			t.Errorf("unexpected error from concurrent Create: %v", err)
		}
	}
	require.Equal(t, 1, created, "exactly one Create should succeed")
	require.Equal(t, workers-1, exists, "all losers should report NodeExists")
}
