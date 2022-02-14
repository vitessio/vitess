package k8stopo

import (
	"fmt"

	"vitess.io/vitess/go/vt/topo"
)

// KubernetesVersion is Kubernetes's idea of a version.
// It implements topo.Version.
type KubernetesVersion string

// String is part of the topo.Version interface.
func (v KubernetesVersion) String() string {
	return string(v)
}

// VersionFromInt is used by old-style functions to create a proper
// Version: if version is -1, returns nil. Otherwise returns the
// KubernetesVersion object.
func VersionFromInt(version int64) topo.Version {
	if version == -1 {
		return nil
	}
	return KubernetesVersion(fmt.Sprint(version))
}
