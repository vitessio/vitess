package main

import (
	"github.com/henryanand/vitess/go/vt/servenv"
	"github.com/henryanand/vitess/go/vt/tabletserver"
)

func init() {
	servenv.OnRun(func() {
		tabletserver.AddStatusPart()
	})
}
