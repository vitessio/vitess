package sidecardb

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

var InitVTSchemaOnTabletInit = true

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&InitVTSchemaOnTabletInit, "init-vt-schema-on-tablet-init", InitVTSchemaOnTabletInit,
		"EXPERIMENTAL: _vt schema is created on tablet init and not separately and/or using WithDDL")
}
