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

package simpleacl

import (
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/tableacl/acl"
)

// SimpleACL keeps all entries in a unique in-memory list
type SimpleACL map[string]bool

// IsMember checks the membership of a principal in this ACL
func (sacl SimpleACL) IsMember(principal *querypb.VTGateCallerID) bool {
	if sacl[principal.Username] {
		return true
	}
	for _, grp := range principal.Groups {
		if sacl[grp] {
			return true
		}
	}
	return false
}

// Factory is responsible to create new ACL instance.
type Factory struct{}

// New creates a new ACL instance.
func (factory *Factory) New(entries []string) (acl.ACL, error) {
	acl := SimpleACL(map[string]bool{})
	for _, e := range entries {
		acl[e] = true
	}
	return acl, nil
}
