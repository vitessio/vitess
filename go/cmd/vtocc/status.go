package main

import (
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
)

func init() {
	servenv.OnRun(func() {
		tabletserver.AddStatusPart()
	})
}
