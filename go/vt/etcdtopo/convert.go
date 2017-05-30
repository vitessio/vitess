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

package etcdtopo

import (
	"encoding/json"
	"strings"

	"github.com/golang/protobuf/proto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// This file contains utility functions to maintain backward
// compatibility with old-style non-Backend etcd topologies. The old
// implementations (before 2016-08-17) used to deal with explicit data
// types. We converted them to a generic []byte and path
// interface. But the etcd implementation was not compatible with
// this.

// dataType is an enum for possible known data types, used for
// backward compatibility.
type dataType int

// Constants for type conversion
const (
	// newType is used to indicate a topology object type of
	// anything that is added after the topo.Backend refactor,
	// i.e. anything that doesn't require conversion between old
	// style topologies and the new style ones. The list of enum
	// values after this contain all types that exist at the
	// moment (2016-08-17) and doesn't need to be expanded when
	// something new is saved in the topology because it will be
	// saved in the new style, not in the old one.
	newType dataType = iota
	srvKeyspaceType
	srvVSchemaType
)

// rawDataFromNodeValue convert the data of the given type into an []byte.
// It is mindful of the backward compatibility, i.e. for newer objects
// it doesn't do anything, but for old object types that were stored in JSON
// format in converts them to proto3 binary encoding.
func rawDataFromNodeValue(valueType dataType, value string) ([]byte, error) {
	var p proto.Message
	switch valueType {
	case srvKeyspaceType:
		p = &topodatapb.SrvKeyspace{}
	case srvVSchemaType:
		p = &vschemapb.SrvVSchema{}
	default:
		return []byte(value), nil
	}

	if err := json.Unmarshal([]byte(value), p); err != nil {
		return nil, err
	}

	return proto.Marshal(p)
}

// oldTypeAndFilePath returns the data type and old file path for a given path.
func oldTypeAndFilePath(filePath string) (dataType, string) {
	parts := strings.Split(filePath, "/")

	// SrvKeyspace: local cell, keyspaces/<keyspace>/SrvKeyspace
	if len(parts) == 3 && parts[0] == "keyspaces" && parts[2] == "SrvKeyspace" {
		return srvKeyspaceType, srvKeyspaceFilePath(parts[1])
	}

	// SrvVSchema: local cell, SrvVSchema
	if len(parts) == 1 && parts[0] == "SrvVSchema" {
		return srvVSchemaType, srvVSchemaFilePath()
	}

	return newType, filePath
}
