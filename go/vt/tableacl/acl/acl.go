package acl

import (
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ACL is an interface for Access Control List.
type ACL interface {
	// IsMember checks the membership of a principal in this ACL.
	IsMember(principal *querypb.VTGateCallerID) bool
}

// Factory is responsible to create new ACL instance.
type Factory interface {
	// New creates a new ACL instance.
	New(entries []string) (ACL, error)
}

// DenyAllACL implements ACL interface and always deny access request.
type DenyAllACL struct{}

// IsMember implements ACL.IsMember and always return false.
func (acl DenyAllACL) IsMember(principal *querypb.VTGateCallerID) bool {
	return false
}

// AcceptAllACL implements ACL interface and always accept access request.
type AcceptAllACL struct{}

// IsMember implements ACL.IsMember and always return true.
func (acl AcceptAllACL) IsMember(principal *querypb.VTGateCallerID) bool {
	return true
}
