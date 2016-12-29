package memorytopo

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/topo"
)

// NodeVersion is the local topo.Version implementation
type NodeVersion uint64

func (v NodeVersion) String() string {
	return fmt.Sprintf("%v", uint64(v))
}

// VersionFromInt is used by old-style functions to create a proper
// Version: if version is -1, returns nil. Otherwise returns the
// NodeVersion object.
func VersionFromInt(version int64) topo.Version {
	if version == -1 {
		return nil
	}
	return NodeVersion(version)
}
