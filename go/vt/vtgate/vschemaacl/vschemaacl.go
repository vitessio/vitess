/*
Copyright 2018 The Vitess Authors.

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

package vschemaacl

import (
	"flag"
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	// AuthorizedDDLUsers specifies the users that can perform ddl operations
	AuthorizedDDLUsers = flag.String("vschema_ddl_authorized_users", "", "List of users authorized to execute vschema ddl operations, or '%' to allow all users.")

	// ddlAllowAll is true if the special value of "*" was specified
	allowAll bool

	// ddlACL contains a set of allowed usernames
	acl map[string]struct{}
)

// Init parses the users option and sets allowAll / acl accordingly
func Init() {
	acl = make(map[string]struct{})
	allowAll = false

	if *AuthorizedDDLUsers == "%" {
		allowAll = true
		return
	} else if *AuthorizedDDLUsers == "" {
		return
	}

	for _, user := range strings.Split(*AuthorizedDDLUsers, ",") {
		user = strings.TrimSpace(user)
		acl[user] = struct{}{}
	}
}

// Authorized returns true if the given caller is allowed to execute vschema operations
func Authorized(caller *querypb.VTGateCallerID) bool {
	if allowAll {
		return true
	}

	user := caller.GetUsername()
	_, ok := acl[user]
	return ok
}
