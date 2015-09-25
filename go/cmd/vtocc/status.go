package main

import "github.com/youtube/vitess/go/vt/tabletserver"

// For use by plugins which wish to avoid racing when registering status page parts.
var onStatusRegistered func()

func addStatusParts(qsc tabletserver.Controller) {
	qsc.AddStatusPart()
	if onStatusRegistered != nil {
		onStatusRegistered()
	}
}
