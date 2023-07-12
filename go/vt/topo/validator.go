package topo

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// ValidateObjectName checks that the name is a valid object name.
// Object names are used for things like keyspace and shard names
// and must match specific constraints.
// They are only allowed to use ASCII letters or digits, - and _.
// No spaces or special characters are allowed.
func validateObjectName(name string) error {
	if name == "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "empty name")
	}

	if len(name) > 64 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "name %v is too long", name)
	}

	for _, c := range name {
		switch {
		case 'a' <= c && c <= 'z':
		case 'A' <= c && c <= 'Z':
		case '0' <= c && c <= '9':
		case c == '-' || c == '_':
		default:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid character %s in name %v", string(c), name)
		}
	}

	return nil
}
