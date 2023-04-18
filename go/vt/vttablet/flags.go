package vttablet

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

const (
	VReplicationExperimentalFlagOptimizeInserts           = int64(1)
	VReplicationExperimentalFlagAllowNoBlobBinlogRowImage = int64(2)
)

var VReplicationExperimentalFlags = VReplicationExperimentalFlagOptimizeInserts | VReplicationExperimentalFlagAllowNoBlobBinlogRowImage

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	fs.Int64Var(&VReplicationExperimentalFlags, "vreplication_experimental_flags", VReplicationExperimentalFlags,
		"(Bitmask) of experimental features in vreplication to enable")
}
