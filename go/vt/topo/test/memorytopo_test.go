package test

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
)

// This file applies the relevant tests to memorytopo.  It is not in
// the right place, should be situated along memorytopo.  But
// memorytopo only implements the Backend parts of the topology, so
// until all the underlying topo implementations are converted to use
// only topo.Backend, this has to be here, so we can specify the
// subset of tests that work for Backend.

func TestMemoryTopo(t *testing.T) {
	factory := func() topo.Impl {
		return memorytopo.NewMemoryTopo([]string{"global", "test"})
	}

	t.Log("=== checkDirectory")
	ts := factory()
	checkDirectory(t, ts)
	ts.Close()

	t.Log("=== checkFile")
	ts = factory()
	checkFile(t, ts)
	ts.Close()
}
