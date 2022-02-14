package logutil

import (
	"flag"
)

func init() {
	threshold := flag.Lookup("stderrthreshold")
	if threshold == nil {
		// the logging module doesn't specify a stderrthreshold flag
		return
	}

	const warningLevel = "1"
	if err := threshold.Value.Set(warningLevel); err != nil {
		return
	}
	threshold.DefValue = warningLevel
}
