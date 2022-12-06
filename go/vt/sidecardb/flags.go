package sidecardb

import (
	"sync"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

var InitVTSchemaOnTabletInit = true
var InitVTSchemaOnTabletInitMu sync.Mutex

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	InitVTSchemaOnTabletInitMu.Lock()
	defer InitVTSchemaOnTabletInitMu.Unlock()
	fs.BoolVar(&InitVTSchemaOnTabletInit, "init-vt-schema-on-tablet-init", InitVTSchemaOnTabletInit,
		"EXPERIMENTAL: _vt schema is created on tablet init and not separately and/or using WithDDL")
}
