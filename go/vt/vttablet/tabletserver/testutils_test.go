package tabletserver

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/dbconfigs"
)

var errRejected = errors.New("rejected")

func newDBConfigs(db *fakesqldb.DB) *dbconfigs.DBConfigs {
	params, _ := db.ConnParams().MysqlParams()
	cp := *params
	return dbconfigs.NewTestDBConfigs(cp, cp, "")
}

// requireLogs ensure that the given logs contains all the string in wants.
// the input logs string must be a semicolon separated string.
func requireLogs(t *testing.T, logs string, wants ...string) {
	logLines := strings.Split(logs, ";")
	for _, expectedLogLine := range wants {
		require.Contains(t, logLines, expectedLogLine)
	}
}
