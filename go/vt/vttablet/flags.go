package vttablet

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

var BinlogRowImageFullOnly = false

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&BinlogRowImageFullOnly, "binlog-row-image-full", BinlogRowImageFullOnly, "If true, do not allow noblob or minimal binlog-row-image. Set to false for also allowing noblob and minimal")
}
