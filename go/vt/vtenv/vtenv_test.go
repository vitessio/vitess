package vtenv

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestNewDefaults(t *testing.T) {
	e, err := New(Options{})
	assert.NoError(t, err)
	assert.Equal(t, config.DefaultMySQLVersion, e.MySQLVersion())
	assert.Equal(t, collations.MySQL8(), e.CollationEnv())
	assert.Equal(t, 0, e.Parser().GetTruncateErrLen())
	assert.Equal(t, "foo", e.TruncateForLog("foo"))
	assert.Equal(t, "foo", e.TruncateForUI("foo"))
}

func TestNewCustom(t *testing.T) {
	e, err := New(Options{
		MySQLServerVersion: "8.0.34",
		TruncateErrLen:     15,
		TruncateUILen:      16,
	})
	assert.NoError(t, err)
	assert.Equal(t, "8.0.34", e.MySQLVersion())
	assert.Equal(t, collations.MySQL8(), e.CollationEnv())
	assert.Equal(t, 15, e.Parser().GetTruncateErrLen())
	assert.Equal(t, "sel [TRUNCATED]", e.TruncateForLog("select 11111111111"))
	assert.Equal(t, "sele [TRUNCATED]", e.TruncateForUI("select 11111111111"))
}

func TestNewError(t *testing.T) {
	_, err := New(Options{
		MySQLServerVersion: "invalid",
	})
	assert.Error(t, err)
}

func TestNewTestEnv(t *testing.T) {
	e := NewTestEnv()
	assert.Equal(t, config.DefaultMySQLVersion, e.MySQLVersion())
	assert.Equal(t, collations.MySQL8(), e.CollationEnv())
	assert.Equal(t, sqlparser.NewTestParser(), e.Parser())
}
