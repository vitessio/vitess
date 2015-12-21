// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package acl

// ACL is an interface for Access Control List.
type ACL interface {
	// IsMember checks the membership of a principal in this ACL.
	IsMember(principal string) bool
}

// Factory is responsible to create new ACL instance.
type Factory interface {
	// New creates a new ACL instance.
	New(entries []string) (ACL, error)
}

// DenyAllACL implements ACL interface and alway deny access request.
type DenyAllACL struct{}

// IsMember implements ACL.IsMember and always return fasle.
func (acl DenyAllACL) IsMember(principal string) bool {
	return false
}

// AcceptAllACL implements ACL interface and alway accept access request.
type AcceptAllACL struct{}

// IsMember implements ACL.IsMember and always return true.
func (acl AcceptAllACL) IsMember(principal string) bool {
	return true
}
