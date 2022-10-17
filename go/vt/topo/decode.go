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

package topo

import (
	"fmt"
	"path"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// DecodeContent uses the filename to imply a type, and proto-decodes
// the right object, then echoes it as a string.
func DecodeContent(filename string, data []byte, json bool) (string, error) {
	name := path.Base(filename)
	dir := path.Dir(filename)
	var p proto.Message
	switch name {
	case CellInfoFile:
		p = new(topodatapb.CellInfo)
	case KeyspaceFile:
		p = new(topodatapb.Keyspace)
	case ShardFile:
		p = new(topodatapb.Shard)
	case VSchemaFile:
		p = new(vschemapb.Keyspace)
	case ShardReplicationFile:
		p = new(topodatapb.ShardReplication)
	case TabletFile:
		p = new(topodatapb.Tablet)
	case SrvVSchemaFile:
		p = new(vschemapb.SrvVSchema)
	case SrvKeyspaceFile:
		p = new(topodatapb.SrvKeyspace)
	case RoutingRulesFile:
		p = new(vschemapb.RoutingRules)
	default:
		switch dir {
		case "/" + GetExternalVitessClusterDir():
			p = new(topodatapb.ExternalVitessCluster)
		default:
		}
		if p == nil {
			if json {
				return "", fmt.Errorf("unknown topo protobuf type for %v", name)
			}
			return string(data), nil
		}
	}

	if err := proto.Unmarshal(data, p); err != nil {
		return string(data), err
	}

	var marshalled []byte
	var err error
	if json {
		marshalled, err = protojson.Marshal(p)
	} else {
		marshalled, err = prototext.Marshal(p)
	}
	return string(marshalled), err
}
