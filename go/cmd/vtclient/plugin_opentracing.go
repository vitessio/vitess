package main

import (
	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/vt/servenv"
)

func init() {
	servenv.OnRun(func() {
		closer := trace.StartTracing("vtclient")
		servenv.OnClose(trace.LogErrorsWhenClosing(closer))
	})
}
