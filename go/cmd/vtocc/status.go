package main

import (
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
)

// For use by plugins which wish to avoid racing when registering status page parts.
var onStatusRegistered func()

func init() {
	servenv.OnRun(func() {
		tabletserver.AddStatusPart()
		if onStatusRegistered != nil {
			onStatusRegistered()
		}
	})
}
