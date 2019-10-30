/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package etcd2topo

import (
	"fmt"

	"vitess.io/vitess/go/vt/topo"
)

// EtcdVersion is etcd's idea of a version.
// It implements topo.Version.
// We use the native etcd version type, int64.
type EtcdVersion int64

// String is part of the topo.Version interface.
func (v EtcdVersion) String() string {
	return fmt.Sprintf("%v", int64(v))
}

// VersionFromInt is used by old-style functions to create a proper
// Version: if version is -1, returns nil. Otherwise returns the
// EtcdVersion object.
func VersionFromInt(version int64) topo.Version {
	if version == -1 {
		return nil
	}
	return EtcdVersion(version)
}
