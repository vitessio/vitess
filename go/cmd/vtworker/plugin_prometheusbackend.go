package main

// This plugin imports Prometheus to allow for instrumentation
// with the Prometheus client library

import (
	"vitess.io/vitess/go/stats/prometheusbackend"
	"vitess.io/vitess/go/vt/servenv"
)

func init() {
	servenv.OnRun(func() {
		prometheusbackend.Init("vtworker")
	})
}
