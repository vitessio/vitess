package zktopo

import (
	"encoding/json"
	"path"
	"strings"

	"github.com/golang/protobuf/proto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// This file contains utility functions to maintain backward compatibility
// with old-style non-Backend Zookeeper topologies. The old
// implementations (before 2016-08-17) used to deal with explicit data
// types. We converted them to a generic []byte and path
// interface. But the zookeeper implementation was not compatible with
// this.

// dataType is an enum for possible data types, used for backward
// compatibility.
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
func rawDataFromNodeValue(what dataType, data []byte) ([]byte, error) {
	var p proto.Message
	switch what {
	case srvKeyspaceType:
		p = &topodatapb.SrvKeyspace{}
	case srvVSchemaType:
		p = &vschemapb.SrvVSchema{}
	default:
		return data, nil
	}

	if err := json.Unmarshal(data, p); err != nil {
		return nil, err
	}

	return proto.Marshal(p)
}

// oldTypeAndFilePath returns the data type and old file path for a given path.
func oldTypeAndFilePath(cell, filePath string) (dataType, string) {
	parts := strings.Split(filePath, "/")

	// SrvKeyspace: local cell, keyspaces/<keyspace>/SrvKeyspace
	if len(parts) == 3 && parts[0] == "keyspaces" && parts[2] == "SrvKeyspace" {
		return srvKeyspaceType, zkPathForSrvKeyspace(cell, parts[1])
	}

	// SrvVSchema: local cell, SrvVSchema
	if len(parts) == 1 && parts[0] == "SrvVSchema" {
		return srvVSchemaType, zkPathForSrvVSchema(cell)
	}

	// General case.
	return newType, path.Join("/zk", cell, "vt", filePath)
}
