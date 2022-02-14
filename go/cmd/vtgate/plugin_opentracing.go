package main

import (
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/servenv"
)

func init() {
	servenv.OnInit(func() {
		closer := trace.StartTracing("vtgate")
		servenv.OnClose(trace.LogErrorsWhenClosing(closer))
	})
}
