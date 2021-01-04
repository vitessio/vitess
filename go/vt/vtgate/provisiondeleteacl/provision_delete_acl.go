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

package provisiondeleteacl

import (
	"flag"
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	provisionAuthorizedUsers = flag.String(
		"provision_delete_keyspace_authorized_users",
		"",
		"List of users authorized to delete keyspaces via `DROP DATABASE <keyspace>`, or '%' to allow all users.",
	)

	allowAll bool
	acl map[string]struct{}
)

func Init() {
	acl = make(map[string]struct{})
	allowAll = false

	if *provisionAuthorizedUsers == "%" {
		allowAll = true
		return
	} else if *provisionAuthorizedUsers == "" {
		return
	}

	for _, user := range strings.Split(*provisionAuthorizedUsers, ",") {
		user = strings.TrimSpace(user)
		acl[user] = struct{}{}
	}
}

func Authorized(caller *querypb.VTGateCallerID) bool {
	if allowAll {
		return true
	}

	user := caller.GetUsername()
	_, ok := acl[user]
	return ok
}
