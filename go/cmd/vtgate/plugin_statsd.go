package main

import "vitess.io/vitess/go/stats/statsd"

func init() {
	statsd.Init("vtgate")
}
