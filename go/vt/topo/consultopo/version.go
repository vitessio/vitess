package consultopo

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/topo"
)

// ConsulVersion is consul's idea of a version.
// It implements topo.Version.
// We use the native consul version type, uint64.
type ConsulVersion uint64

// String is part of the topo.Version interface.
func (v ConsulVersion) String() string {
	return fmt.Sprintf("%v", uint64(v))
}

// VersionFromInt is used by old-style functions to create a proper
// Version: if version is -1, returns nil. Otherwise returns the
// ConsulVersion object.
func VersionFromInt(version int64) topo.Version {
	if version == -1 {
		return nil
	}
	return ConsulVersion(version)
}
