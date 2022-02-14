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
