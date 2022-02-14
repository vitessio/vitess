package main

import (
	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/vt/servenv"
)

func init() {
	servenv.OnRun(func() {
		closer := trace.StartTracing("vtcombo")
		servenv.OnClose(trace.LogErrorsWhenClosing(closer))
	})
}
