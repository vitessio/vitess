// Package services exposes all the services for the vtgateclienttest binary.
package services

import (
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

// CreateServices creates the implementation chain of all the test cases
func CreateServices() vtgateservice.VTGateService {
	var s vtgateservice.VTGateService
	s = newTerminalClient()
	s = newErrorClient(s)
	s = newCallerIDClient(s)
	s = newEchoClient(s)
	return s
}
