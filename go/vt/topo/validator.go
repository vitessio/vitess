/*
Copyright 2026 The Vitess Authors.

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
