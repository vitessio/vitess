package main

// This plugin imports opentsdb to register the opentsdb stats backend.

import (
	"vitess.io/vitess/go/stats/opentsdb"
)

func init() {
	opentsdb.Init("vtgate")
}
