package sidecardb

import (
	"sync"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

var InitVTSchemaOnTabletInit = true
var mu sync.Mutex

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	mu.Lock()
	defer mu.Unlock()
	fs.BoolVar(&InitVTSchemaOnTabletInit, "init-vt-schema-on-tablet-init", InitVTSchemaOnTabletInit,
		"EXPERIMENTAL: _vt schema is created on tablet init and not separately and/or using WithDDL")
}

func GetInitVTSchemaFlag() bool {
	mu.Lock()
	defer mu.Unlock()
	return InitVTSchemaOnTabletInit
}

func SetInitVTSchemaFlag(val bool) {
	mu.Lock()
	defer mu.Unlock()
	InitVTSchemaOnTabletInit = val
}
