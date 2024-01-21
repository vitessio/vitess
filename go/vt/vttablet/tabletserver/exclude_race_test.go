//go:build !race

package tabletserver

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// TestHandlePanicAndSendLogStatsMessageTruncation tests that when an error truncation
// length is set and a panic occurs, the code in handlePanicAndSendLogStats will
// truncate the error text in logs, but will not truncate the error text in the
// error value.
func TestHandlePanicAndSendLogStatsMessageTruncation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tl := newTestLogger()
	defer tl.Close()
	logStats := tabletenv.NewLogStats(ctx, "TestHandlePanicAndSendLogStatsMessageTruncation")
	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: config.DefaultMySQLVersion,
		TruncateErrLen:     32,
	})
	require.NoError(t, err)

	db, tsv := setupTabletServerTestCustom(t, ctx, tabletenv.NewDefaultConfig(), "", env)
	defer tsv.StopService()
	defer db.Close()

	longSql := "select * from test_table_loooooooooooooooooooooooooooooooooooong"
	longBv := map[string]*querypb.BindVariable{
		"bv1": sqltypes.Int64BindVariable(1111111111),
		"bv2": sqltypes.Int64BindVariable(2222222222),
		"bv3": sqltypes.Int64BindVariable(3333333333),
		"bv4": sqltypes.Int64BindVariable(4444444444),
	}

	defer func() {
		err := logStats.Error
		want := "Uncaught panic for Sql: \"select * from test_table_loooooooooooooooooooooooooooooooooooong\", BindVars: {bv1: \"type:INT64 value:\\\"1111111111\\\"\"bv2: \"type:INT64 value:\\\"2222222222\\\"\"bv3: \"type:INT64 value:\\\"3333333333\\\"\"bv4: \"type:INT64 value:\\\"4444444444\\\"\"}"
		require.Error(t, err)
		assert.Contains(t, err.Error(), want)
		want = "Uncaught panic for Sql: \"select * from test_t [TRUNCATED]\", BindVars: {bv1: \"typ [TRUNCATED]"
		gotWhatWeWant := false
		for _, log := range tl.getLogs() {
			if strings.HasPrefix(log, want) {
				gotWhatWeWant = true
				break
			}
		}
		assert.True(t, gotWhatWeWant)
	}()

	defer tsv.handlePanicAndSendLogStats(longSql, longBv, logStats)
	panic("panic from TestHandlePanicAndSendLogStatsMessageTruncation")
}
