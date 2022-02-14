package onlineddl

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
)

// CheckCancelAllMigrations cancels all pending migrations. There is no validation for affected migrations.
func CheckCancelAllMigrationsViaVtctl(t *testing.T, vtctlclient *cluster.VtctlClientProcess, keyspace string) {
	cancelQuery := "alter vitess_migration cancel all"

	_, err := vtctlclient.ApplySchemaWithOutput(keyspace, cancelQuery, cluster.VtctlClientParams{SkipPreflight: true})
	assert.NoError(t, err)
}
