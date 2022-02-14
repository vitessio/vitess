package main

import (
	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/vt/servenv"
)

func init() {
	servenv.OnInit(func() {
		closer := trace.StartTracing("vtctld")
		servenv.OnClose(trace.LogErrorsWhenClosing(closer))
	})
}
