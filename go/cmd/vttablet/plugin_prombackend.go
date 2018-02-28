package main

import (
	"vitess.io/vitess/go/stats/prombackend"
)

func init() {
	prombackend.Init("vttablet")
}
