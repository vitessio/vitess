package servenv

import (
	"github.com/spf13/pflag"
)

var (
	// TruncateUILen truncate queries in debug UIs to the given length. 0 means unlimited.
	TruncateUILen = 512

	// TruncateErrLen truncate queries in error logs to the given length. 0 means unlimited.
	TruncateErrLen = 0
)

func registerQueryTruncationFlags(fs *pflag.FlagSet) {
	fs.IntVar(&TruncateUILen, "sql-max-length-ui", TruncateUILen, "truncate queries in debug UIs to the given length (default 512)")
	fs.IntVar(&TruncateErrLen, "sql-max-length-errors", TruncateErrLen, "truncate queries in error logs to the given length (default unlimited)")
}

func init() {
	for _, cmd := range []string{
		"vtgate",
		"vttablet",
		"vtcombo",
		"vtctld",
		"vtctl",
		"vtexplain",
		"vtbackup",
		"vttestserver",
		"vtbench",
	} {
		OnParseFor(cmd, registerQueryTruncationFlags)
	}
}
