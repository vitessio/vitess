package engine

import (
	"testing"
)

// This package is mostly tested from VTGate. This place-holder test
// is to document this fact. In order to make sure all code paths are
// covered, you need to run the coverage tool from the vtgate directory
// using the following command:
//	go test -coverprofile=c.out -coverpkg ./engine,.
// You can then follow it with:
//	go tool cover -html=c.out -o=c.html
// to see the html output.
func TestNothing(t *testing.T) {
}
