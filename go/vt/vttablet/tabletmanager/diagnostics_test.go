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

package tabletmanager

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// deadlineContext is a manually expired context that reports deadline exceeded.
type deadlineContext struct {
	// Context provides parent values and cancellation.
	context.Context

	// done is closed when the test deadline expires.
	done chan struct{}

	// once ensures the deadline is expired at most once.
	once sync.Once
}

// newDeadlineContext returns a manually expired deadline context.
func newDeadlineContext(parent context.Context) *deadlineContext {
	return &deadlineContext{
		Context: parent,
		done:    make(chan struct{}),
	}
}

// Done returns a channel that closes when expire is called.
func (c *deadlineContext) Done() <-chan struct{} {
	return c.done
}

// Err reports deadline exceeded after expire is called.
func (c *deadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return c.Context.Err()
	}
}

// expire marks the context as past its deadline.
func (c *deadlineContext) expire() {
	c.once.Do(func() {
		close(c.done)
	})
}

// TestDemotePrimaryDumpsMysqlDiagnosticsWhenSetSuperReadOnlyTimesOut verifies
// that a timed out super_read_only step collects MySQL diagnostics.
func TestDemotePrimaryDumpsMysqlDiagnosticsWhenSetSuperReadOnlyTimesOut(t *testing.T) {
	baseCtx := t.Context()
	ctx := newDeadlineContext(baseCtx)
	ts := memorytopo.NewServer(baseCtx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)

	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	var gotQueries []string
	var gotQueriesMu sync.Mutex

	fakeMysqlDaemon.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
		gotQueriesMu.Lock()
		defer gotQueriesMu.Unlock()

		gotQueries = append(gotQueries, query)

		return sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("value", "varchar"),
			"ok",
		), nil
	}

	fakeMysqlDaemon.SetSuperReadOnlyFunc = func(ctx context.Context, on bool) (mysqlctl.ResetSuperReadOnlyFunc, error) {
		ctx.(*deadlineContext).expire()

		require.Eventually(t, func() bool {
			gotQueriesMu.Lock()
			defer gotQueriesMu.Unlock()

			return len(gotQueries) == len(diagnosticQueries)
		}, 30*time.Second, 10*time.Millisecond)

		return nil, ctx.Err()
	}

	_, err := tm.demotePrimary(ctx, false /* revertPartialFailure */, false /* force */)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	gotQueriesMu.Lock()
	defer gotQueriesMu.Unlock()

	assert.Len(t, gotQueries, len(diagnosticQueries))
}

// TestDemotePrimaryDoesNotDumpMysqlDiagnosticsWhenSetSuperReadOnlyFailsWithoutTimeout
// verifies that non-timeout failures do not trigger extra MySQL queries.
func TestDemotePrimaryDoesNotDumpMysqlDiagnosticsWhenSetSuperReadOnlyFailsWithoutTimeout(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)

	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeMysqlDaemon.SetSuperReadOnlyError = errors.New("access denied")

	var gotQueries []string
	fakeMysqlDaemon.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
		gotQueries = append(gotQueries, query)

		return sqltypes.MakeTestResult(sqltypes.MakeTestFields("value", "varchar"), "ok"), nil
	}

	_, err := tm.demotePrimary(ctx, false /* revertPartialFailure */, false /* force */)
	require.ErrorContains(t, err, "access denied")

	assert.Empty(t, gotQueries)
}
