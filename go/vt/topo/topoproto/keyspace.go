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

package topoproto

import (
	"fmt"
	"strings"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// ParseKeyspaceType parses a string into a KeyspaceType.
func ParseKeyspaceType(param string) (topodatapb.KeyspaceType, error) {
	value, ok := topodatapb.KeyspaceType_value[strings.ToUpper(param)]
	if !ok {
		// default
		return topodatapb.KeyspaceType_NORMAL, fmt.Errorf("unknown keyspace type: %v", value)
	}
	return topodatapb.KeyspaceType(value), nil
}

// KeyspaceTypeString returns the string representation of a KeyspaceType.
func KeyspaceTypeString(kt topodatapb.KeyspaceType) string {
	str, ok := topodatapb.KeyspaceType_name[int32(kt)]
	if !ok {
		return "UNKNOWN"
	}

	return str
}
