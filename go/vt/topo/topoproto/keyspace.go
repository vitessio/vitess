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

// KeyspaceTypeLString returns the lowercased string representation of a
// KeyspaceType.
func KeyspaceTypeLString(kt topodatapb.KeyspaceType) string {
	return strings.ToLower(KeyspaceTypeString(kt))
}

// KeyspaceIDTypeString returns the string representation of a KeyspaceIdType.
func KeyspaceIDTypeString(kidType topodatapb.KeyspaceIdType) string {
	str, ok := topodatapb.KeyspaceIdType_name[int32(kidType)]
	if !ok {
		return "UNKNOWN"
	}

	return str
}

// KeyspaceIDTypeLString returns the lowercased string representation of a
// KeyspaceIdType.
func KeyspaceIDTypeLString(kidType topodatapb.KeyspaceIdType) string {
	return strings.ToLower(KeyspaceIDTypeString(kidType))
}
