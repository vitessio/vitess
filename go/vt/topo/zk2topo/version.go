package zk2topo

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/topo"
)

// ZKVersion is zookeeper's idea of a version.
// It implements topo.Version.
// We use the native zookeeper.Stat.Version type, int32.
type ZKVersion int32

// String is part of the topo.Version interface.
func (v ZKVersion) String() string {
	return fmt.Sprintf("%v", int32(v))
}

var _ topo.Version = (ZKVersion)(0) // compile-time interface check
