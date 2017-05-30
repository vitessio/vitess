/*
Copyright 2017 Google Inc.

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
