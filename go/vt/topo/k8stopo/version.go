/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
