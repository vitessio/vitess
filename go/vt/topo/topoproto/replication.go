/*
Copyright 2022 The Vitess Authors.

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

import topodatapb "vitess.io/vitess/go/vt/proto/topodata"

var shardReplicationErrorTypeName map[int32]string

func init() {
	shardReplicationErrorTypeName = make(map[int32]string, len(topodatapb.ShardReplicationError_Type_name))
	for k, v := range topodatapb.ShardReplicationError_Type_name {
		shardReplicationErrorTypeName[k] = v
	}
}

// ShardReplicationErrorTypeString returns a string representation of the
// ShardReplicationError type, or "UNKNOWN" if not known.
func ShardReplicationErrorTypeString(t topodatapb.ShardReplicationError_Type) string {
	v, ok := shardReplicationErrorTypeName[int32(t)]
	if !ok {
		return "UNKNOWN"
	}

	return v
}
