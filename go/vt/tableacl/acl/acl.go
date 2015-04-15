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
	// All returns an ACL instance that contains all users.
	All() ACL
	// AllString returns a string representation of all users.
	AllString() string
}
