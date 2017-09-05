/*
Copyright 2017 Google Inc.

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

package acl

import (
	querypb "github.com/youtube/vitess/go/vt/proto/query"
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

// DenyAllACL implements ACL interface and alway deny access request.
type DenyAllACL struct{}

// IsMember implements ACL.IsMember and always return false.
func (acl DenyAllACL) IsMember(principal *querypb.VTGateCallerID) bool {
	return false
}

// AcceptAllACL implements ACL interface and alway accept access request.
type AcceptAllACL struct{}

// IsMember implements ACL.IsMember and always return true.
func (acl AcceptAllACL) IsMember(principal *querypb.VTGateCallerID) bool {
	return true
}
