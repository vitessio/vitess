package dbname

import (
	"sync/atomic"

	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	DefaultName = "_vt"
)

var (
	// This should be accessed via GetName()
	sidecarDBName atomic.Value
)

func init() {
	sidecarDBName.Store(DefaultName)
}

func SetName(name string) {
	sidecarDBName.Store(name)
}

func GetName() string {
	return sidecarDBName.Load().(string)
}

// GetIdentifier returns the sidecar database name as an SQL
// identifier string, most importantly this means that it will
// be properly escaped if/as needed.
func GetIdentifier() string {
	ident := sqlparser.NewIdentifierCS(GetName())
	return sqlparser.String(ident)
}
